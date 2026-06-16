import threading
import time
import json
import logging
import urllib.request
import urllib.error
from typing import List, Optional
from abc import ABC, abstractmethod

from config import LogAgentConfig, LogEntry

logger = logging.getLogger(__name__)


class LogReporter(ABC):
    """
    日志上报器抽象基类。
    """

    @abstractmethod
    def report(self, entries: List[LogEntry]) -> bool:
        """
        上报一批日志。

        Returns:
            True 表示上报成功，False 表示上报失败
        """
        pass

    @abstractmethod
    def close(self):
        """关闭上报器，释放资源。"""
        pass


class HttpLogReporter(LogReporter):
    """
    HTTP 日志上报器，支持主备多目标地址自动切换。

    支持：
    - 自定义请求头、Bearer Token、Basic Auth 鉴权
    - 多目标地址：主地址 + 若干备用地址
    - 主地址连续失败超过阈值时，自动切到备用地址
    - 当前地址连续成功 N 次后，尝试切回主地址
    - 状态可查看：当前地址、切换次数、上次切换时间、恢复进度

    上报失败时，错误信息会包含目标地址和环境标识。
    """

    def __init__(self, config: LogAgentConfig):
        self._env = config.reporter_env
        self._timeout_sec = config.reporter_connect_timeout_sec
        self._max_retries = config.reporter_max_retries
        self._retry_backoff_ms = config.reporter_retry_backoff_ms

        self._primary_endpoint = config.reporter_endpoint
        self._backup_endpoints = list(config.reporter_backup_endpoints or [])
        self._all_endpoints = [self._primary_endpoint] + self._backup_endpoints

        self._failover_threshold = config.reporter_failover_threshold
        self._recover_after_success = config.reporter_recover_after_success

        self._current_index = 0
        self._endpoint_failures = {ep: 0 for ep in self._all_endpoints}
        self._endpoint_success_streak = {ep: 0 for ep in self._all_endpoints}

        self._switch_count = 0
        self._last_switch_time = None
        self._last_switch_reason = ""

        self._headers = {
            "Content-Type": "application/json",
            "User-Agent": f"log-agent/1.0 (env={self._env})",
        }

        if config.reporter_headers:
            self._headers.update(config.reporter_headers)

        if config.reporter_auth_token:
            self._headers["Authorization"] = f"Bearer {config.reporter_auth_token}"

        if config.reporter_basic_auth:
            import base64
            user, pwd = config.reporter_basic_auth
            basic_token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            self._headers["Authorization"] = f"Basic {basic_token}"

        self._simulate_failure_endpoints: set = set()
        self._simulate_success_endpoints: set = set()
        self._simulate_delay_ms = 0

        self._consecutive_failures = 0
        self._last_failure_reason = ""

    @property
    def _endpoint(self) -> str:
        return self._all_endpoints[self._current_index]

    def set_simulate_failure(self, fail: bool, endpoint: Optional[str] = None):
        """模拟网络故障。endpoint 指定某地址，None 表示全部。"""
        if endpoint:
            if fail:
                self._simulate_failure_endpoints.add(endpoint)
                self._simulate_success_endpoints.discard(endpoint)
            else:
                self._simulate_failure_endpoints.discard(endpoint)
        else:
            if fail:
                for ep in self._all_endpoints:
                    self._simulate_failure_endpoints.add(ep)
                    self._simulate_success_endpoints.discard(ep)
            else:
                self._simulate_failure_endpoints.clear()

    def set_simulate_success(self, succeed: bool, endpoint: Optional[str] = None):
        """模拟上报成功（跳过真实HTTP）。endpoint 指定某地址，None 表示全部。"""
        if endpoint:
            if succeed:
                self._simulate_success_endpoints.add(endpoint)
                self._simulate_failure_endpoints.discard(endpoint)
            else:
                self._simulate_success_endpoints.discard(endpoint)
        else:
            if succeed:
                for ep in self._all_endpoints:
                    self._simulate_success_endpoints.add(ep)
                    self._simulate_failure_endpoints.discard(ep)
            else:
                self._simulate_success_endpoints.clear()

    def set_simulate_delay(self, delay_ms: int):
        """模拟网络延迟。"""
        self._simulate_delay_ms = delay_ms

    def get_target_info(self) -> dict:
        """获取上报目标信息，出问题时快速识别是哪个目标。"""
        headers_safe = {
            k: (v if k.lower() != "authorization" else "***")
            for k, v in self._headers.items()
        }
        return {
            "current_endpoint": self._endpoint,
            "current_endpoint_role": "primary" if self._current_index == 0 else f"backup#{self._current_index}",
            "primary_endpoint": self._primary_endpoint,
            "all_endpoints": self._all_endpoints,
            "env": self._env,
            "timeout_sec": self._timeout_sec,
            "headers": headers_safe,
            "consecutive_failures": self._consecutive_failures,
            "last_failure_reason": self._last_failure_reason,
            "failover": {
                "switch_count": self._switch_count,
                "last_switch_time": self._last_switch_time,
                "last_switch_reason": self._last_switch_reason,
                "threshold": self._failover_threshold,
                "recover_success_needed": self._recover_after_success,
                "per_endpoint_failures": dict(self._endpoint_failures),
                "per_endpoint_success_streak": dict(self._endpoint_success_streak),
            },
        }

    def report(self, entries: List[LogEntry]) -> bool:
        """
        上报一批日志。

        流程：
        1. 尝试当前地址
        2. 如果失败次数超过阈值，且有备用地址，则切换
        3. 当前地址连续成功足够次数后，尝试切回主地址
        """
        if self._simulate_delay_ms > 0:
            time.sleep(self._simulate_delay_ms / 1000.0)

        endpoint = self._endpoint
        success = self._try_report(endpoint, entries)

        if success:
            self._consecutive_failures = 0
            self._last_failure_reason = ""
            self._endpoint_failures[endpoint] = 0
            self._endpoint_success_streak[endpoint] = self._endpoint_success_streak.get(endpoint, 0) + 1

            self._try_recover_to_primary()

            return True

        self._record_failure(endpoint, self._last_failure_reason or "unknown")

        self._maybe_failover()

        return False

    def _try_report(self, endpoint: str, entries: List[LogEntry]) -> bool:
        """尝试向指定地址上报一次。"""
        if endpoint in self._simulate_success_endpoints:
            return True
        if endpoint in self._simulate_failure_endpoints:
            self._last_failure_reason = "simulated failure"
            return False

        target_info = f"[env={self._env}] {endpoint}"

        try:
            payload = json.dumps([e.to_dict() for e in entries]).encode("utf-8")

            req = urllib.request.Request(
                endpoint,
                data=payload,
                method="POST",
                headers=self._headers,
            )

            with urllib.request.urlopen(req, timeout=self._timeout_sec) as resp:
                status = resp.getcode()
                if 200 <= status < 300:
                    return True
                else:
                    self._last_failure_reason = f"HTTP {status}"
                    logger.warning(f"report failed {target_info}: HTTP {status}")
                    return False

        except urllib.error.HTTPError as e:
            reason = f"HTTP {e.code}: {e.reason}"
            self._last_failure_reason = reason
            logger.warning(f"report failed {target_info}: {reason}")
            return False
        except urllib.error.URLError as e:
            reason = f"URL Error: {e.reason}"
            self._last_failure_reason = reason
            logger.warning(f"report failed {target_info}: {reason}")
            return False
        except Exception as e:
            reason = f"Unexpected: {type(e).__name__}: {e}"
            self._last_failure_reason = reason
            logger.error(f"report failed {target_info}: {reason}")
            return False

    def _record_failure(self, endpoint: str, reason: str):
        self._consecutive_failures += 1
        self._endpoint_failures[endpoint] = self._endpoint_failures.get(endpoint, 0) + 1
        self._endpoint_success_streak[endpoint] = 0

    def _maybe_failover(self):
        """当前地址失败太多时，切换到下一个可用地址。"""
        if self._consecutive_failures < self._failover_threshold:
            return

        if len(self._all_endpoints) <= 1:
            return

        old_index = self._current_index
        new_index = (self._current_index + 1) % len(self._all_endpoints)

        attempts = 0
        while new_index != old_index and attempts < len(self._all_endpoints):
            if self._endpoint_failures.get(self._all_endpoints[new_index], 0) < self._failover_threshold:
                break
            new_index = (new_index + 1) % len(self._all_endpoints)
            attempts += 1

        if new_index != old_index:
            self._current_index = new_index
            self._switch_count += 1
            self._last_switch_time = time.time()
            self._last_switch_reason = f"consecutive failures reached {self._failover_threshold}"
            self._consecutive_failures = 0
            logger.warning(
                f"failover: switched endpoint from "
                f"{self._all_endpoints[old_index]} to {self._all_endpoints[new_index]} "
                f"(reason: {self._last_switch_reason})"
            )

    def _try_recover_to_primary(self):
        """当前地址（非主）连续成功足够次数后，尝试切回主地址。"""
        if self._current_index == 0:
            return

        current_ep = self._endpoint
        streak = self._endpoint_success_streak.get(current_ep, 0)

        if streak >= self._recover_after_success:
            old_ep = current_ep
            self._current_index = 0
            self._switch_count += 1
            self._last_switch_time = time.time()
            self._last_switch_reason = f"backup had {streak} consecutive successes, recovering to primary"
            self._consecutive_failures = 0
            logger.info(
                f"failover: recovered to primary {self._primary_endpoint} "
                f"from {old_ep} (after {streak} successes)"
            )

    def close(self):
        pass


class ConsoleLogReporter(LogReporter):
    """
    控制台上报器，用于调试。
    """

    def __init__(self):
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def get_target_info(self) -> dict:
        return {
            "type": "console",
            "current_endpoint": "stdout",
            "reported_count": self._count,
        }

    def report(self, entries: List[LogEntry]) -> bool:
        for entry in entries:
            print(f"[{entry.level}] {entry.message}")
            self._count += 1
        return True

    def close(self):
        pass


class ReporterWorker:
    """
    上报工作线程。

    负责从环形缓冲区批量消费日志，然后通过 reporter 上报。
    支持批量上报、失败重试、优雅关闭。

    崩溃保护设计：
    - 维护 _pending_entries 列表，跟踪正在上报/重试中的日志
    - 提供 drain_all_pending() 方法，崩溃时可获取所有待处理日志
    """

    def __init__(self, ring_buffer, reporter: LogReporter, config: LogAgentConfig):
        self._buffer = ring_buffer
        self._reporter = reporter
        self._config = config
        self._batch_size = config.reporter_batch_size
        self._flush_interval = config.reporter_flush_interval_ms / 1000.0

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._flush_event = threading.Event()

        self._reported_count = 0
        self._failed_count = 0
        self._retry_count = 0

        self._pending_entries: List[LogEntry] = []
        self._pending_lock = threading.Lock()

        self._stats_lock = threading.Lock()

        self._start_time = time.time()
        self._recent_reports = []
        self._recent_window_sec = 60.0

    def start(self):
        """启动上报线程。"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="log-reporter")
        self._thread.start()

    def stop(self, timeout: float = 5.0):
        """停止上报线程，等待剩余日志处理完毕。"""
        self._stop_event.set()
        self._flush_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def flush(self):
        """触发立即上报。"""
        self._flush_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                entries = self._buffer.get_batch(
                    max_count=self._batch_size,
                    block=True,
                    timeout=self._flush_interval
                )

                if entries:
                    self._report_with_retry(entries)

                if self._flush_event.is_set():
                    self._flush_event.clear()

            except Exception as e:
                logger.error(f"reporter worker error: {e}")

        self._drain_and_report()

    def _report_with_retry(self, entries: List[LogEntry]):
        with self._pending_lock:
            self._pending_entries.extend(entries)

        try:
            retries = 0
            while True:
                success = self._reporter.report(entries)
                if success:
                    with self._stats_lock:
                        self._reported_count += len(entries)
                        now = time.time()
                        self._recent_reports.append((now, len(entries)))
                        cutoff = now - self._recent_window_sec
                        while self._recent_reports and self._recent_reports[0][0] < cutoff:
                            self._recent_reports.pop(0)
                    return

                retries += 1
                with self._stats_lock:
                    self._retry_count += 1

                if retries >= self._config.reporter_max_retries:
                    with self._stats_lock:
                        self._failed_count += len(entries)
                    logger.warning(
                        f"failed to report {len(entries)} logs after {retries} retries, "
                        f"logs will remain in buffer if space allows"
                    )

                    for entry in entries:
                        self._buffer.put(entry, block=False)
                    return

                backoff = self._config.reporter_retry_backoff_ms / 1000.0 * retries
                time.sleep(backoff)
        finally:
            with self._pending_lock:
                    for entry in entries:
                        try:
                            self._pending_entries.remove(entry)
                        except ValueError:
                            pass

    def _drain_and_report(self):
        """排空缓冲区并尝试上报，用于关闭时。"""
        entries = self._buffer.drain_all()
        if entries:
            try:
                success = self._reporter.report(entries)
                if success:
                    with self._stats_lock:
                        self._reported_count += len(entries)
                else:
                    with self._stats_lock:
                        self._failed_count += len(entries)
            except Exception as e:
                logger.error(f"drain report error: {e}")
                with self._stats_lock:
                    self._failed_count += len(entries)

    def drain_all_pending(self) -> List[LogEntry]:
        """
        获取所有待处理的日志（缓冲区 + 正在上报中的）。

        崩溃保护调用，用于异常退出时落盘。
        注意：调用后这些日志就从系统中移除了。
        """
        all_entries = []

        with self._pending_lock:
            all_entries.extend(self._pending_entries)
            self._pending_entries = []

        buffer_entries = self._buffer.drain_all()
        all_entries.extend(buffer_entries)

        return all_entries

    def peek_all_pending(self) -> List[LogEntry]:
        """
        只读查看所有待处理的日志，不移除。

        用于本地查询、问题排查。
        """
        all_entries = []

        with self._pending_lock:
            all_entries.extend(list(self._pending_entries))

        buffer_entries = self._buffer.peek()
        all_entries.extend(buffer_entries)

        return all_entries

    def get_stats(self) -> dict:
        with self._stats_lock:
            return {
                "reported_count": self._reported_count,
                "failed_count": self._failed_count,
                "retry_count": self._retry_count,
            }

    def get_rate_stats(self) -> dict:
        """获取上报速率统计。"""
        now = time.time()
        with self._stats_lock:
            cutoff = now - self._recent_window_sec
            while self._recent_reports and self._recent_reports[0][0] < cutoff:
                self._recent_reports.pop(0)

            total_recent = sum(count for _, count in self._recent_reports)

            if len(self._recent_reports) >= 2:
                time_span = self._recent_reports[-1][0] - self._recent_reports[0][0]
                if time_span > 0:
                    qps = total_recent / time_span
                else:
                    qps = 0.0
            else:
                qps = 0.0

            avg_qps = self._reported_count / (now - self._start_time) if now > self._start_time else 0.0

            pending_in_buffer = len(self._buffer)
            with self._pending_lock:
                pending_in_retry = len(self._pending_entries)

            total_pending = pending_in_buffer + pending_in_retry

            return {
                "qps_recent": round(qps, 2),
                "qps_avg": round(avg_qps, 2),
                "total_reported": self._reported_count,
                "total_failed": self._failed_count,
                "total_retries": self._retry_count,
                "pending_in_buffer": pending_in_buffer,
                "pending_in_retry": pending_in_retry,
                "total_pending": total_pending,
                "window_sec": self._recent_window_sec,
                "uptime_sec": round(now - self._start_time, 1),
            }
