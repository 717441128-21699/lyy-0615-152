"""
LogAgent 命令行管理入口。

优先连接到"正在运行"的 Agent（通过本地 HTTP 管理端口自动发现），
如果没有检测到正在运行的实例，则在本地临时启动一个空实例（兼容老行为）。

用法：
    python cli.py status                          # 查看运行状态
    python cli.py logs [--level ERROR] [--keyword xxx] [--trace xxx] [--service svc] [--limit 50] [--order desc|asc]
    python cli.py export [--output ./output_dir] [--service svc]
    python cli.py level <LEVEL>                    # 设置最小日志级别 DEBUG/INFO/WARN/ERROR
    python cli.py sample <RATE>                    # 设置全局采样率 0.0~1.0
    python cli.py service <SERVICE> <RATE>              # 设置服务采样率
    python cli.py add-trace <TRACE_ID>                 # 添加 trace 白名单
    python cli.py rm-trace <TRACE_ID>               # 移除 trace 白名单
    python cli.py add-keyword <KEYWORD> <RATE>       # 添加关键词采样
    python cli.py target                           # 查看上报目标和主备状态
    python cli.py ping                             # 检查是否有 Agent 正在运行

通过环境变量配置 Agent：
    LOG_AGENT_CONFIG=./config.json   指定额外配置
    LOG_AGENT_MGMT_URL=http://127.0.0.1:PORT  直接指定管理地址，跳过自动发现
    LOG_AGENT_MGMT_TOKEN=xxx        指定鉴权 token
"""

import argparse
import sys
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
from typing import Optional, Dict, Any

from log_agent import LogAgent
from config import LogAgentConfig
from management_server import discover_agent


def _http_get(url: str, token: Optional[str] = None, timeout: float = 5.0) -> Any:
    """发送 GET 请求并解析 JSON。"""
    sep = "&" if "?" in url else "?"
    if token:
        url = f"{url}{sep}token={urllib.parse.quote(token)}"
    req = urllib.request.Request(url, method="GET")
    if token:
        req.add_header("X-Management-Token", token)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def _http_post(url: str, data: Optional[dict] = None,
               token: Optional[str] = None, timeout: float = 5.0) -> Any:
    """发送 POST 请求并解析 JSON。"""
    sep = "&" if "?" in url else "?"
    if token:
        url = f"{url}{sep}token={urllib.parse.quote(token)}"
    body = json.dumps(data or {}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Management-Token", token)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp_body = resp.read().decode("utf-8")
    return json.loads(resp_body) if resp_body else {}


class RemoteAgentClient:
    """连接到正在运行的 Agent 的 HTTP 客户端。"""

    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.is_remote = True

    def get_status(self) -> dict:
        return _http_get(f"{self.base_url}/status", self.token)

    def print_status(self):
        # 通过 /status 拿到 JSON 后自己打印，模拟本地 print_status
        s = self.get_status()
        _print_status_from_json(s)
        samp = s.get("sampling", {})
        print("  ── 采样配置")
        print(f"     最小级别: {samp.get('min_level', 'DEBUG')}")
        print(f"     全局采样率: {samp.get('global_sample_rate', 1.0)}")
        svc = samp.get("service_rules", {})
        if svc:
            print(f"     服务采样: {svc}")
        kw = samp.get("keyword_rules", [])
        if kw:
            print(f"     关键词规则: {kw}")
        print()

    def query_logs(self, **kwargs) -> list:
        params = {}
        for k, v in kwargs.items():
            if v is not None:
                params[k] = str(v)
        qs = urllib.parse.urlencode(params)
        data = _http_get(f"{self.base_url}/logs?{qs}", self.token)
        return data.get("logs", [])

    def print_logs(self, **kwargs):
        logs = self.query_logs(**kwargs)
        order = kwargs.get("order", "desc")
        order_label = "从新到旧" if order == "desc" else "从旧到新"
        if not logs:
            print("(没有符合条件的未上报日志)")
            return
        print(f"┌── 未上报日志查询结果（共 {len(logs)} 条，排序: {order_label}，远程）")
        print(f"│   过滤条件: {kwargs}")
        print(f"├{'─' * 100}")
        for log in logs:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(log["timestamp"]))
            trace = log.get("trace_id", "-") or "-"
            svc = log.get("service", "-") or "-"
            msg = log["message"]
            if len(msg) > 60:
                msg = msg[:57] + "..."
            print(f"│ [{log['level']:<8}] {ts}  svc={svc:<15} trace={trace:<12}  {msg}")
        print(f"└{'─' * 100}")

    def export_diagnostic_data(self, **kwargs) -> str:
        params = {}
        if kwargs.get("output_dir"):
            params["output_dir"] = kwargs["output_dir"]
        params["include_crash"] = "0" if kwargs.get("no_crash_dumps") else "1"
        if kwargs.get("service"):
            params["service"] = kwargs["service"]
        qs = urllib.parse.urlencode(params)
        data = _http_get(f"{self.base_url}/export?{qs}", self.token, timeout=30)
        if not data.get("ok"):
            raise RuntimeError(data.get("error", "export failed"))
        return data["path"]

    def get_target_info(self) -> dict:
        return _http_get(f"{self.base_url}/target", self.token)

    class _SamplerProxy:
        def __init__(self, client: "RemoteAgentClient"):
            self.client = client

        def set_min_level(self, level: str):
            return _http_post(f"{self.client.base_url}/sampling/level",
                              {"level": level}, self.client.token)

        def set_global_sample_rate(self, rate: float):
            return _http_post(f"{self.client.base_url}/sampling/global_rate",
                              {"rate": rate}, self.client.token)

        def set_service_sample_rate(self, service: str, rate: float):
            return _http_post(f"{self.client.base_url}/sampling/service_rate",
                              {"service": service, "rate": rate}, self.client.token)

        def add_trace_whitelist(self, trace_id: str):
            return _http_post(f"{self.client.base_url}/sampling/trace_whitelist",
                              {"action": "add", "trace_id": trace_id}, self.client.token)

        def remove_trace_whitelist(self, trace_id: str):
            return _http_post(f"{self.client.base_url}/sampling/trace_whitelist",
                              {"action": "remove", "trace_id": trace_id}, self.client.token)

        def add_keyword_rule(self, keyword: str, rate: float):
            return _http_post(f"{self.client.base_url}/sampling/keyword_rule",
                              {"keyword": keyword, "rate": rate}, self.client.token)

    @property
    def sampler(self):
        return self._SamplerProxy(self)


def _print_status_from_json(s: dict):
    """与本地 LogAgent.print_status 保持视觉一致的远程 JSON 版。"""
    running = "[OK] RUNNING" if s.get("running") else "[--] STOPPED"
    uptime = s.get("uptime_sec", 0)
    uptime_str = f"{uptime/60:.1f} 分钟" if uptime >= 60 else f"{uptime:.1f} 秒"

    target = s.get("target", {})
    target_lines = []
    if target:
        role = target.get("current_endpoint_role", "")
        ep = target.get("current_endpoint", target.get("endpoint", "?"))
        env = target.get("env", "?")
        role_label = {"primary": "主地址", "backup#1": "备地址#1", "backup#2": "备地址#2"}.get(role, role)
        h = f"[{env}] {ep}"
        if role_label:
            h += f"  ({role_label})"
        if target.get("consecutive_failures", 0) > 0:
            h += f"  [!] 连续失败 {target['consecutive_failures']} 次"
        target_lines.append(h)

        failover = target.get("failover", {})
        if failover:
            sc = failover.get("switch_count", 0)
            if sc > 0:
                ls = failover.get("last_switch_time")
                lr = failover.get("last_switch_reason", "")
                ss = f"  累计切换 {sc} 次"
                if ls:
                    ss += f"，上次: {time.strftime('%H:%M:%S', time.localtime(ls))}"
                if lr:
                    ss += f" ({lr})"
                target_lines.append(ss)
                recover_needed = failover.get("recover_success_needed", 0)
                if role != "primary" and recover_needed > 0:
                    streak_map = failover.get("per_endpoint_success_streak", {})
                    cur = streak_map.get(ep, 0)
                    pct = min(100, int(cur / max(1, recover_needed) * 100))
                    target_lines.append(f"  恢复主地址进度: {cur}/{recover_needed} ({pct}%)")
            all_eps = target.get("all_endpoints", [])
            failures = failover.get("per_endpoint_failures", {})
            if len(all_eps) > 1:
                parts = []
                for i, e in enumerate(all_eps):
                    tag = "*" if e == ep else " "
                    f_cnt = failures.get(e, 0)
                    label = "主" if i == 0 else f"备{i}"
                    parts.append(f"{tag}[{label}] {e} (失败{f_cnt})")
                target_lines.append("  全部目标: " + " | ".join(parts))
    if not target_lines:
        target_lines = ["N/A"]

    buf = s.get("buffer", {})
    thr = s.get("throughput", {})
    que = s.get("backlog", {})
    err = s.get("errors", {})
    buf_size = buf.get("size", 0)
    buf_cap = buf.get("capacity", 0)
    buf_pct = buf.get("usage_pct", 0)
    buf_bar = "█" * int(buf_pct / 10) + "░" * (10 - int(buf_pct / 10))
    qps = thr.get("qps_recent", 0)
    backlog = que.get("total", 0)
    drain_est = que.get("drain_estimate_sec")
    drain_str = ""
    if drain_est is not None:
        drain_str = f" (预计 {drain_est/60:.1f} 分钟排空)" if drain_est > 60 else f" (预计 {drain_est} 秒排空)"

    print()
    print("┌" + "─" * 82 + "┐")
    print(f"│  LogAgent 运行状态   {running:<30}   运行时长: {uptime_str:<15}│")
    for i, tl in enumerate(target_lines):
        prefix = "│  上报目标: " if i == 0 else "│            "
        print(f"{prefix}{tl:<67}│")
    print("├" + "─" * 82 + "┤")
    print(f"│  [buffer] 缓冲区                     │  [thrpt] 吞吐")
    print(f"│     容量: {buf_cap:<10,}              │     累计写入: {thr.get('total_written', 0):>12,}")
    print(f"│     已用: {buf_size:<10,}              │     累计上报: {thr.get('total_reported', 0):>12,}")
    print(f"│     使用率: {buf_bar} {buf_pct:>5.1f}%     │     近期 QPS: {qps:>12,.1f}")
    print(f"│                                     │     平均 QPS: {thr.get('qps_avg', 0):>12,.1f}")
    print("├" + "─" * 82 + "┤")
    print(f"│  [queued] 积压日志                    │  [error] 异常")
    print(f"│     总数: {backlog:<10,}  {drain_str:<20}│     溢出次数: {err.get('overflow_count', 0):>10,}")
    print(f"│     缓冲区内: {que.get('in_buffer', 0):<8,}            │     丢弃总数: {err.get('total_dropped', 0):>10,}")
    print(f"│     重试中:   {que.get('in_retry', 0):<8,}            │       丢最老: {err.get('dropped_oldest', 0):>10,}")
    print(f"│                                     │       丢最新: {err.get('dropped_newest', 0):>10,}")
    print(f"│                                     │     重试次数: {err.get('total_retries', 0):>10,}")
    print(f"│                                     │     上报失败: {thr.get('total_failed', 0):>10,}")
    print("└" + "─" * 82 + "┘")


def _load_config_from_env() -> LogAgentConfig:
    config = LogAgentConfig()

    if os.environ.get("LOG_AGENT_ENDPOINT"):
        config.reporter_endpoint = os.environ["LOG_AGENT_ENDPOINT"]
    if os.environ.get("LOG_AGENT_BACKUPS"):
        config.reporter_backup_endpoints = os.environ["LOG_AGENT_BACKUPS"].split(",")
    if os.environ.get("LOG_AGENT_ENV"):
        config.reporter_env = os.environ["LOG_AGENT_ENV"]
    if os.environ.get("LOG_AGENT_TOKEN"):
        config.reporter_auth_token = os.environ["LOG_AGENT_TOKEN"]
    if os.environ.get("LOG_AGENT_CRASH_DIR"):
        config.crash_dump_dir = os.environ["LOG_AGENT_CRASH_DIR"]
    if os.environ.get("LOG_AGENT_BUFFER"):
        config.buffer_capacity = int(os.environ["LOG_AGENT_BUFFER"])

    config_file = os.environ.get("LOG_AGENT_CONFIG")
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in data.items():
                if hasattr(config, k):
                    setattr(config, k, v)
        except Exception as e:
            print(f"[warn] 加载配置文件失败: {e}", file=sys.stderr)

    return config


def _connect_or_create() -> Any:
    """优先连接运行中的 Agent，否则本地启动一个临时实例。"""
    # 显式指定管理地址
    mgmt_url = os.environ.get("LOG_AGENT_MGMT_URL")
    mgmt_token = os.environ.get("LOG_AGENT_MGMT_TOKEN")

    if mgmt_url:
        return RemoteAgentClient(mgmt_url, mgmt_token)

    # 自动发现
    config = _load_config_from_env()
    info = discover_agent(config.crash_dump_dir)
    if info:
        base = f"http://{info['host']}:{info['port']}"
        print(f"[i] 已连接到正在运行的 Agent: {base} (pid={info['pid']})", file=sys.stderr)
        return RemoteAgentClient(base, info.get("token") or mgmt_token)

    # 没找到，本地起一个（兼容）
    print("[i] 未检测到正在运行的 Agent，已启动本地临时实例（仅对本次命令生效）", file=sys.stderr)
    agent = LogAgent(config)
    agent.start()
    agent._ephemeral = True
    return agent


# ===== 命令处理（兼容远程和本地两种 agent 对象） =====

def cmd_status(args, agent):
    print()
    agent.print_status()


def cmd_logs(args, agent):
    agent.print_logs(
        level=args.level,
        keyword=args.keyword,
        trace_id=args.trace,
        service=args.service,
        limit=args.limit,
        order=args.order,
    )


def cmd_export(args, agent):
    kwargs = {}
    if hasattr(args, "output") and args.output:
        kwargs["output_dir"] = args.output
    if hasattr(args, "no_crash_dumps"):
        kwargs["include_crash_dumps"] = not args.no_crash_dumps
    if hasattr(args, "service") and args.service:
        kwargs["service"] = args.service

    if hasattr(agent, "export_diagnostic_data"):
        path = agent.export_diagnostic_data(**kwargs)
    else:
        # remote
        path = agent.export_diagnostic_data(
            output_dir=kwargs.get("output_dir"),
            no_crash_dumps=not kwargs.get("include_crash_dumps", True),
            service=kwargs.get("service"),
        )
    print()
    print(f"[OK] 诊断数据已导出:")
    print(f"   {path}")
    try:
        size = os.path.getsize(path)
        print(f"   文件大小: {size:,} bytes")
    except Exception:
        pass
    print()


def cmd_level(args, agent):
    level = args.level.upper()
    try:
        agent.sampler.set_min_level(level)
        print(f"[OK] 最小日志级别已设置为: {level}")
        print(f"   低于 {level} 的日志将被过滤")
    except (ValueError, urllib.error.HTTPError) as e:
        body = ""
        if isinstance(e, urllib.error.HTTPError):
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
        print(f"[FAIL] 设置失败: {e} {body}", file=sys.stderr)
        sys.exit(1)


def cmd_sample(args, agent):
    try:
        rate = float(args.rate)
        agent.sampler.set_global_sample_rate(rate)
        pct = rate * 100
        print(f"[OK] 全局采样率已设置为: {rate} ({pct:.0f}%)")
        print(f"   新写入的日志中每 100 条保留 {pct:.0f} 条")
    except (ValueError, TypeError) as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_service(args, agent):
    try:
        rate = float(args.rate)
        agent.sampler.set_service_sample_rate(args.service, rate)
        print(f"[OK] 服务 '{args.service}' 采样率已设置为: {rate}")
    except (ValueError, TypeError) as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_add_trace(args, agent):
    agent.sampler.add_trace_whitelist(args.trace_id)
    print(f"[OK] trace_id '{args.trace_id}' 已加入白名单，该 trace 的日志将全量保留")


def cmd_rm_trace(args, agent):
    agent.sampler.remove_trace_whitelist(args.trace_id)
    print(f"[OK] trace_id '{args.trace_id}' 已从白名单移除")


def cmd_add_keyword(args, agent):
    try:
        rate = float(args.rate)
        agent.sampler.add_keyword_rule(args.keyword, rate)
        print(f"[OK] 关键词 '{args.keyword}' 采样率已设置为: {rate}")
    except (ValueError, TypeError) as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_target(args, agent):
    info = {}
    if hasattr(agent, "get_target_info"):
        info = agent.get_target_info()
    elif hasattr(agent, "_reporter") and hasattr(agent._reporter, "get_target_info"):
        info = agent._reporter.get_target_info()
    print()
    print(json.dumps(info, ensure_ascii=False, indent=2, default=str))
    print()


def cmd_ping(args, agent):
    if getattr(agent, "is_remote", False):
        print(f"[OK] Agent 正在运行: {agent.base_url}")
    else:
        print("[i] 当前使用的是本地临时实例，不是运行中的 Agent")
        print("    要连接到正在运行的 Agent，请先启动它并确保 management_enabled=True")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="logagent",
        description="LogAgent 本地管理工具（自动连接正在运行的 Agent）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    subparsers.add_parser("ping", help="检测是否有 Agent 正在运行")
    subparsers.add_parser("status", help="查看运行状态")
    subparsers.add_parser("target", help="查看上报目标和主备切换信息")

    p_logs = subparsers.add_parser("logs", help="查询未上报的日志")
    p_logs.add_argument("--level", default=None, help="按级别过滤: DEBUG/INFO/WARN/ERROR")
    p_logs.add_argument("--keyword", default=None, help="按关键词过滤")
    p_logs.add_argument("--trace", default=None, help="按 trace_id 过滤")
    p_logs.add_argument("--service", default=None, help="按服务名过滤")
    p_logs.add_argument("--limit", type=int, default=50, help="最多显示条数")
    p_logs.add_argument("--order", choices=["desc", "asc"], default="desc",
                        help="排序: desc=从新到旧, asc=从旧到新")

    p_export = subparsers.add_parser("export", help="导出诊断数据包")
    p_export.add_argument("--output", default=None, help="输出目录")
    p_export.add_argument("--service", default=None, help="按服务名过滤")
    p_export.add_argument("--no-crash-dumps", action="store_true", help="不包含崩溃转储")

    p_level = subparsers.add_parser("level", help="设置最小日志级别")
    p_level.add_argument("level", help="DEBUG / INFO / WARN / ERROR")

    p_sample = subparsers.add_parser("sample", help="设置全局采样率")
    p_sample.add_argument("rate", help="0.0 ~ 1.0，例如 0.1 表示 10%%")

    p_service = subparsers.add_parser("service", help="设置特定服务采样率")
    p_service.add_argument("service", help="服务名")
    p_service.add_argument("rate", help="0.0 ~ 1.0")

    p_add_trace = subparsers.add_parser("add-trace", help="添加 trace_id 到白名单（全量采样）")
    p_add_trace.add_argument("trace_id", help="要保留的 trace_id")

    p_rm_trace = subparsers.add_parser("rm-trace", help="从白名单移除 trace_id")
    p_rm_trace.add_argument("trace_id", help="要移除的 trace_id")

    p_kw = subparsers.add_parser("add-keyword", help="添加关键词采样规则")
    p_kw.add_argument("keyword", help="关键词")
    p_kw.add_argument("rate", help="0.0 ~ 1.0")

    return parser


COMMAND_MAP = {
    "ping": cmd_ping,
    "status": cmd_status,
    "target": cmd_target,
    "logs": cmd_logs,
    "export": cmd_export,
    "level": cmd_level,
    "sample": cmd_sample,
    "service": cmd_service,
    "add-trace": cmd_add_trace,
    "rm-trace": cmd_rm_trace,
    "add-keyword": cmd_add_keyword,
}


def main():
    parser = build_parser()

    if len(sys.argv) < 2:
        parser.print_help()
        print()
        print("示例:")
        print("  python cli.py ping                       # 检测是否有 Agent 在运行")
        print("  python cli.py status                     # 查看状态（远程优先）")
        print("  python cli.py target                     # 查看上报目标和主备信息")
        print("  python cli.py logs --level ERROR --limit 100")
        print("  python cli.py logs --service order-service --order asc")
        print("  python cli.py export --service order-service")
        print("  python cli.py level WARN")
        print("  python cli.py sample 0.5")
        print("  python cli.py service order-service 1.0")
        print("  python cli.py add-trace trace-abc123")
        print()
        print("环境变量:")
        print("  LOG_AGENT_MGMT_URL=http://127.0.0.1:PORT   指定管理地址")
        print("  LOG_AGENT_MGMT_TOKEN=xxx                  指定鉴权 token")
        sys.exit(0)

    args = parser.parse_args()

    if args.command not in COMMAND_MAP:
        parser.print_help()
        sys.exit(1)

    agent = _connect_or_create()
    try:
        handler = COMMAND_MAP[args.command]
        handler(args, agent)
    finally:
        if hasattr(agent, "_ephemeral") and agent._ephemeral:
            agent.stop(timeout=0.5)


if __name__ == "__main__":
    main()
