import threading
import time
import logging
import json
import os
import sys
from typing import Optional, List

from ring_buffer import RingBuffer
from config import LogAgentConfig, LogEntry
from reporter import ReporterWorker, HttpLogReporter, ConsoleLogReporter, LogReporter
from crash_protector import CrashProtector
from sample_controller import SampleController
from management_server import ManagementServer

logger = logging.getLogger(__name__)


def _safe_print(msg: str = ""):
    """兼容 Windows GBK 终端的打印函数。"""
    try:
        print(msg)
    except UnicodeEncodeError:
        try:
            print(msg.encode("utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8", errors="replace"))
        except Exception:
            pass


class LogAgent:
    """
    高吞吐日志收集 Agent。

    核心特性：
    1. 异步写入：业务线程写入日志仅需将日志放入环形缓冲区，几乎零阻塞
    2. 环形缓冲区：多生产者单消费者设计，支持高并发写入
    3. 满溢策略可配置：丢弃最老 / 丢弃最新 / 阻塞
    4. 崩溃保护：进程异常退出时缓冲区日志自动落盘
    5. 批量上报：减少网络 IO 次数，提高吞吐

    典型用法：
        agent = LogAgent(config)
        agent.start()

        # 业务代码中写日志
        agent.info("hello world")

        # 程序结束时优雅关闭
        agent.stop()
    """

    def __init__(self, config: Optional[LogAgentConfig] = None):
        self._config = config or LogAgentConfig()

        self._buffer = RingBuffer(
            capacity=self._config.buffer_capacity,
            overflow_strategy=self._config.overflow_strategy
        )

        self._reporter = self._create_reporter()
        self._reporter_worker = ReporterWorker(self._buffer, self._reporter, self._config)

        self._crash_protector = CrashProtector(dump_dir=self._config.crash_dump_dir)

        self._sample_controller = SampleController()

        self._management = ManagementServer(self)

        self._started = False
        self._start_lock = threading.Lock()

        self._write_count = 0
        self._sample_hit_count = 0
        self._stats_lock = threading.Lock()

    def _create_reporter(self) -> LogReporter:
        reporter_type = self._config.reporter_type.lower()
        if reporter_type == "console":
            return ConsoleLogReporter()
        elif reporter_type == "http":
            return HttpLogReporter(self._config)
        else:
            raise ValueError(f"unknown reporter type: {reporter_type}")

    def start(self):
        """启动 Agent，开始后台上报。"""
        with self._start_lock:
            if self._started:
                return

            if self._config.crash_dump_enabled:
                recovered = self._crash_protector.recover_latest_dump()
                if recovered:
                    for entry in recovered:
                        self._buffer.put(entry, block=False)
                    logger.info(f"recovered {len(recovered)} logs from crash dump")

                self._crash_protector.register(self._buffer, self._reporter_worker)

            self._reporter_worker.start()
            self._management.start()
            self._started = True
            logger.info("log agent started")

    def stop(self, timeout: float = 5.0):
        """优雅停止 Agent，等待所有日志上报完毕。"""
        with self._start_lock:
            if not self._started:
                return
            self._started = False

        self._management.stop()

        if self._config.crash_dump_enabled:
            self._crash_protector.unregister()

        self._reporter_worker.stop(timeout=timeout)
        self._reporter.close()
        logger.info("log agent stopped")

    @property
    def management_url(self) -> Optional[str]:
        """管理服务器地址（未启用或未启动成功返回 None。"""
        port = self._management.actual_port
        if port:
            return self._management.base_url
        return None

    @property
    def management_port(self) -> int:
        """管理服务器实际监听端口，0 表示未启用。"""
        return self._management.actual_port

    def log(self, level: str, message: str, trace_id: Optional[str] = None,
            extra: Optional[dict] = None, service: Optional[str] = None,
            block: bool = False, timeout: Optional[float] = None) -> str:
        """
        写一条日志。

        这是业务线程调用的主要接口。
        默认非阻塞模式：仅将日志放入环形缓冲区，耗时极短，几乎不阻塞业务线程。
        阻塞模式：缓冲区满时会等待空位，直到超时（仅 BLOCK 策略有效）。

        Args:
            level: 日志级别，如 DEBUG/INFO/WARN/ERROR
            message: 日志内容
            trace_id: 追踪 ID
            extra: 额外字段
            service: 服务名
            block: 是否阻塞等待空位（仅 BLOCK 策略有效）
                   DROP_OLDEST / DROP_NEWEST 策略下，此参数无效，总是立即返回
            timeout: 阻塞超时时间（秒），None 表示一直等

        Returns:
            "success" - 写入成功
            "dropped" - 被丢弃（缓冲区满 或 采样过滤）
            "timeout" - 阻塞等待超时（仅 BLOCK + block=True 时可能返回）
            "sampled_out" - 被采样规则过滤（级别太低 / 采样未命中）
        """
        entry = LogEntry(
            level=level,
            message=message,
            trace_id=trace_id,
            extra=extra or {},
            service=service or self._config.service if hasattr(self._config, 'service') else "default",
        )

        keep, _reason = self._sample_controller.should_keep(entry)
        if not keep:
            with self._stats_lock:
                self._sample_hit_count += 1
            return "sampled_out"

        result = self._buffer.put(entry, block=block, timeout=timeout)

        if result == RingBuffer.PUT_SUCCESS:
            with self._stats_lock:
                self._write_count += 1

        return result

    @property
    def sampler(self) -> SampleController:
        """访问采样控制器，用于动态调整日志级别和采样率。"""
        return self._sample_controller

    def debug(self, message: str, **kwargs):
        return self.log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs):
        return self.log("INFO", message, **kwargs)

    def warn(self, message: str, **kwargs):
        return self.log("WARN", message, **kwargs)

    def warning(self, message: str, **kwargs):
        return self.log("WARN", message, **kwargs)

    def error(self, message: str, **kwargs):
        return self.log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs):
        return self.log("CRITICAL", message, **kwargs)

    def set_overflow_strategy(self, strategy: str):
        """动态设置满溢策略。"""
        self._buffer.overflow_strategy = strategy

    def flush(self):
        """触发立即上报。"""
        self._reporter_worker.flush()

    def crash_dump(self) -> int:
        """手动触发崩溃转储（用于测试）。"""
        return self._crash_protector.dump_to_disk()

    def get_stats(self) -> dict:
        """获取运行统计。"""
        with self._stats_lock:
            write_count = self._write_count

        buffer_stats = self._buffer.get_stats()
        reporter_stats = self._reporter_worker.get_stats()

        return {
            "write_count": write_count,
            "buffer": buffer_stats,
            "reporter": reporter_stats,
        }

    def get_status(self) -> dict:
        """
        获取完整的运行状态，包括上报速率、积压量、重试次数等。

        适合压测时监控、判断是否需要扩容。
        """
        rate_stats = self._reporter_worker.get_rate_stats()
        buffer_stats = self._buffer.get_stats()

        with self._stats_lock:
            write_count = self._write_count

        dropped_total = (buffer_stats.get("dropped_oldest_count", 0)
                        + buffer_stats.get("dropped_newest_count", 0))

        backlog = rate_stats.get("total_pending", 0)
        qps_recent = rate_stats.get("qps_recent", 0)
        buffer_capacity = buffer_stats.get("capacity", 1)
        buffer_usage_pct = (buffer_stats.get("size", 0) / buffer_capacity * 100)

        target_info = None
        if hasattr(self._reporter, "get_target_info"):
            target_info = self._reporter.get_target_info()

        status = {
            "running": self._started,
            "uptime_sec": rate_stats.get("uptime_sec", 0),
            "overflow_strategy": buffer_stats.get("overflow_strategy"),
            "buffer": {
                "capacity": buffer_capacity,
                "size": buffer_stats.get("size", 0),
                "usage_pct": round(buffer_usage_pct, 1),
            },
            "throughput": {
                "total_written": write_count,
                "total_reported": rate_stats.get("total_reported", 0),
                "total_failed": rate_stats.get("total_failed", 0),
                "qps_recent": qps_recent,
                "qps_avg": rate_stats.get("qps_avg", 0),
            },
            "sampling": self._sample_controller.get_stats(),
            "backlog": {
                "total": backlog,
                "in_buffer": rate_stats.get("pending_in_buffer", 0),
                "in_retry": rate_stats.get("pending_in_retry", 0),
            },
            "errors": {
                "total_dropped": dropped_total,
                "dropped_oldest": buffer_stats.get("dropped_oldest_count", 0),
                "dropped_newest": buffer_stats.get("dropped_newest_count", 0),
                "overflow_count": buffer_stats.get("overflow_count", 0),
                "total_retries": rate_stats.get("total_retries", 0),
            },
            "target": target_info,
        }

        if qps_recent > 0 and backlog > 1000:
            estimate_sec = round(backlog / qps_recent, 1)
            status["backlog"]["drain_estimate_sec"] = estimate_sec

        return status

    def print_status(self):
        """
        以直观的表格形式打印运行状态。

        包含：积压量、上报速度、重试次数、丢弃量、缓冲区使用率。
        压测时可以直接调用此方法判断是否需要扩容。
        """
        s = self.get_status()

        running = "[OK] RUNNING" if s["running"] else "[--] STOPPED"
        uptime = s["uptime_sec"]
        if uptime >= 60:
            uptime_str = f"{uptime/60:.1f} 分钟"
        else:
            uptime_str = f"{uptime:.1f} 秒"

        target = s.get("target", {})
        target_lines = []
        if target:
            role = target.get("current_endpoint_role", "")
            ep = target.get("current_endpoint", target.get("endpoint", "?"))
            env = target.get("env", "?")
            role_label = {"primary": "主地址", "backup#1": "备地址#1", "backup#2": "备地址#2"}.get(role, role)
            header1 = f"[{env}] {ep}"
            if role_label:
                header1 += f"  ({role_label})"
            if target.get("consecutive_failures", 0) > 0:
                header1 += f"  [!] 连续失败 {target['consecutive_failures']} 次"
            target_lines.append(header1)

            # 主备切换信息
            failover = target.get("failover", {})
            if failover:
                switch_count = failover.get("switch_count", 0)
                if switch_count > 0:
                    last_switch = failover.get("last_switch_time")
                    last_reason = failover.get("last_switch_reason", "")
                    switch_str = f"  累计切换 {switch_count} 次"
                    if last_switch:
                        ts = time.strftime("%H:%M:%S", time.localtime(last_switch))
                        switch_str += f"，上次: {ts}"
                    if last_reason:
                        switch_str += f" ({last_reason})"
                    target_lines.append(switch_str)

                    # 恢复进度
                    recover_needed = failover.get("recover_success_needed", 0)
                    if role != "primary" and recover_needed > 0:
                        current_streak = 0
                        streak_map = failover.get("per_endpoint_success_streak", {})
                        if ep in streak_map:
                            current_streak = streak_map[ep]
                        pct = min(100, int(current_streak / max(1, recover_needed) * 100))
                        target_lines.append(f"  恢复主地址进度: {current_streak}/{recover_needed} ({pct}%)")

                # 所有地址状态一览
                all_eps = target.get("all_endpoints", [])
                failures = failover.get("per_endpoint_failures", {})
                if len(all_eps) > 1:
                    eps_str_parts = []
                    for i, e in enumerate(all_eps):
                        tag = "*" if e == ep else " "
                        f_cnt = failures.get(e, 0)
                        label = "主" if i == 0 else f"备{i}"
                        eps_str_parts.append(f"{tag}[{label}] {e} (失败{f_cnt})")
                    target_lines.append("  全部目标: " + " | ".join(eps_str_parts))

        if not target_lines:
            target_lines = ["N/A"]

        buf_size = s["buffer"]["size"]
        buf_cap = s["buffer"]["capacity"]
        buf_pct = s["buffer"]["usage_pct"]
        buf_bar = "█" * int(buf_pct / 10) + "░" * (10 - int(buf_pct / 10))

        qps = s["throughput"]["qps_recent"]
        backlog = s["backlog"]["total"]
        drain_est = s["backlog"].get("drain_estimate_sec", None)

        drain_str = ""
        if drain_est is not None:
            if drain_est > 60:
                drain_str = f" (预计 {drain_est/60:.1f} 分钟排空)"
            else:
                drain_str = f" (预计 {drain_est} 秒排空)"

        # 管理服务器信息
        mgmt_port = self._management.actual_port
        mgmt_line = ""
        if mgmt_port:
            mgmt_line = f"  管理端口: http://{self._config.management_host}:{mgmt_port}"

        _safe_print()
        _safe_print("┌" + "─" * 82 + "┐")
        _safe_print(f"│  LogAgent 运行状态   {running:<30}   运行时长: {uptime_str:<15}│")
        for i, tl in enumerate(target_lines):
            prefix = "│  上报目标: " if i == 0 else "│            "
            _safe_print(f"{prefix}{tl:<67}│")
        if mgmt_line:
            _safe_print(f"{mgmt_line:<50}│")
        _safe_print("├" + "─" * 82 + "┤")
        _safe_print(f"│  [buffer] 缓冲区                     │  [thrpt] 吞吐")
        _safe_print(f"│     容量: {buf_cap:<10,}              │     累计写入: {s['throughput']['total_written']:>12,}")
        _safe_print(f"│     已用: {buf_size:<10,}              │     累计上报: {s['throughput']['total_reported']:>12,}")
        _safe_print(f"│     使用率: {buf_bar} {buf_pct:>5.1f}%     │     近期 QPS: {qps:>12,.1f}")
        _safe_print(f"│                                     │     平均 QPS: {s['throughput']['qps_avg']:>12,.1f}")
        _safe_print("├" + "─" * 82 + "┤")
        _safe_print(f"│  [queued] 积压日志                    │  [error] 异常")
        _safe_print(f"│     总数: {backlog:<10,}  {drain_str:<20}│     溢出次数: {s['errors']['overflow_count']:>10,}")
        _safe_print(f"│     缓冲区内: {s['backlog']['in_buffer']:<8,}            │     丢弃总数: {s['errors']['total_dropped']:>10,}")
        _safe_print(f"│     重试中:   {s['backlog']['in_retry']:<8,}            │       丢最老: {s['errors']['dropped_oldest']:>10,}")
        _safe_print(f"│                                     │       丢最新: {s['errors']['dropped_newest']:>10,}")
        _safe_print(f"│                                     │     重试次数: {s['errors']['total_retries']:>10,}")
        _safe_print(f"│                                     │     上报失败: {s['throughput']['total_failed']:>10,}")
        _safe_print("└" + "─" * 82 + "┘")

        if buf_pct > 80:
            _safe_print("  [!] 缓冲区使用率超过 80%，建议：")
            _safe_print("     - 扩容缓冲区容量 (buffer_capacity)")
            _safe_print("     - 加快上报速度 (reporter_flush_interval_ms)")
            _safe_print("     - 增加上报批量大小 (reporter_batch_size)")
        elif backlog > 1000 and qps < 100:
            _safe_print("  [!] 积压严重但上报速度慢，建议检查网络或日志服务")

        _safe_print()

    def query_logs(self, level: Optional[str] = None,
                   keyword: Optional[str] = None,
                   trace_id: Optional[str] = None,
                   service: Optional[str] = None,
                   limit: int = 100,
                   order: str = "desc") -> List[dict]:
        """
        本地查询尚未上报的日志，用于快速排查问题。

        只读操作，不会影响日志上报流程。

        注意：无论 order 选择升序还是降序，当 limit 小于积压量时，
        永远优先取"最近写入的那一批日志"，然后再按 order 指定的方向排序。
        这样 asc 模式下看的是"最近 5 条按时间线展开"，而不是"最早 5 条"。

        Args:
            level: 按级别过滤，如 "ERROR"、"WARN"；None 表示所有级别
            keyword: 按消息内容关键词过滤（大小写不敏感）
            trace_id: 按 trace_id 精确匹配
            service: 按服务名精确匹配
            limit: 最多返回多少条，默认 100 条（优先取最近的 limit 条）
            order: 排序方式
                   "desc" - 从新到旧（默认）
                   "asc"  - 从旧到新（最近一批按时间线展开）

        Returns:
            符合条件的日志字典列表，按指定顺序排序
        """
        if order not in ("asc", "desc"):
            raise ValueError("order must be 'asc' or 'desc'")

        entries = self._reporter_worker.peek_all_pending()

        level_upper = level.upper() if level else None
        keyword_lower = keyword.lower() if keyword else None

        all_matched = []
        for entry in entries:
            if level_upper and entry.level.upper() != level_upper:
                continue

            if keyword_lower and keyword_lower not in entry.message.lower():
                continue

            if trace_id and entry.trace_id != trace_id:
                continue

            if service and entry.service != service:
                continue

            all_matched.append(entry.to_dict())

        # 第一步：取最近 limit 条（切片尾部）
        if len(all_matched) > limit:
            all_matched = all_matched[-limit:]

        # 第二步：按 order 排序
        # entries 原始顺序是"从旧到新"（先写入的在前）
        # all_matched 继承了这个顺序
        if order == "desc":
            all_matched.reverse()

        return all_matched

    def print_logs(self, level: Optional[str] = None,
                   keyword: Optional[str] = None,
                   trace_id: Optional[str] = None,
                   service: Optional[str] = None,
                   limit: int = 50,
                   order: str = "desc"):
        """
        打印查询到的日志到控制台，方便快速查看。
        参数同 query_logs。
        """
        logs = self.query_logs(level=level, keyword=keyword,
                               trace_id=trace_id, service=service,
                               limit=limit, order=order)

        if not logs:
            _safe_print("(没有符合条件的未上报日志)")
            return

        order_label = "从新到旧" if order == "desc" else "从旧到新"
        _safe_print(f"┌── 未上报日志查询结果（共 {len(logs)} 条，排序: {order_label}）")
        _safe_print(f"│   过滤条件: level={level}, keyword={keyword}, trace_id={trace_id}, service={service}")
        _safe_print(f"├{'─' * 100}")

        for i, log in enumerate(logs):
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(log["timestamp"]))
            trace = log.get("trace_id", "-") or "-"
            svc = log.get("service", "-") or "-"
            msg = log["message"]
            if len(msg) > 60:
                msg = msg[:57] + "..."
            _safe_print(f"│ [{log['level']:<8}] {ts}  svc={svc:<15} trace={trace:<12}  {msg}")

        _safe_print(f"└{'─' * 100}")

    def export_diagnostic_data(self, output_dir: Optional[str] = None,
                               include_crash_dumps: bool = True,
                               service: Optional[str] = None) -> str:
        """
        一键导出诊断数据：缓冲区积压日志 + 崩溃转储，合并到一个文件。

        导出内容包括：
        1. 诊断头部信息（导出时间、Agent 状态、配置摘要）
        2. 当前积压在缓冲区中尚未上报的日志
        3. 正在上报重试中的日志
        4. 未归档的崩溃转储文件中的日志（可选）

        Args:
            output_dir: 输出目录，None 则使用崩溃转储目录
            include_crash_dumps: 是否包含未归档的崩溃转储
            service: 按服务名过滤导出，None 表示全部

        Returns:
            导出文件的绝对路径
        """
        if output_dir is None:
            output_dir = self._config.crash_dump_dir

        os.makedirs(output_dir, exist_ok=True)

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"diagnostic_export_{timestamp}_{os.getpid()}.jsonl"
        filepath = os.path.abspath(os.path.join(output_dir, filename))

        current_stats = self.get_status()

        all_entries = []

        pending_entries = self._reporter_worker.peek_all_pending()
        for entry in pending_entries:
            d = entry.to_dict()
            d["_source"] = "pending"
            all_entries.append(d)

        if include_crash_dumps:
            dump_files = self._crash_protector.list_dump_files()
            for dump_file in dump_files:
                full_path = os.path.join(self._config.crash_dump_dir, dump_file)
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                d = json.loads(line)
                                d["_source"] = f"crash_dump:{dump_file}"
                                all_entries.append(d)
                            except Exception:
                                continue
                except Exception:
                    continue

        header = {
            "_type": "diagnostic_header",
            "export_time": time.time(),
            "export_time_str": time.strftime("%Y-%m-%d %H:%M:%S"),
            "pid": os.getpid(),
            "status": current_stats,
            "config": {
                "buffer_capacity": self._config.buffer_capacity,
                "overflow_strategy": self._config.overflow_strategy,
                "reporter_type": self._config.reporter_type,
                "reporter_endpoint": self._config.reporter_endpoint,
                "reporter_env": self._config.reporter_env,
            },
        }

        target_info = None
        if hasattr(self._reporter, "get_target_info"):
            target_info = self._reporter.get_target_info()
            header["target_info"] = target_info

        if service:
            all_entries = [e for e in all_entries if e.get("service") == service]
            header["filter_service"] = service

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(json.dumps(header, ensure_ascii=False) + "\n")

            for entry in all_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

            f.flush()
            os.fsync(f.fileno())

        logger.info(f"diagnostic data exported to {filepath}, "
                    f"total {len(all_entries)} entries")
        return filepath

    @property
    def buffer(self) -> RingBuffer:
        return self._buffer

    @property
    def reporter(self):
        return self._reporter

    @property
    def crash_protector(self) -> CrashProtector:
        return self._crash_protector
