"""
LogAgent 命令行管理入口。

用法：
    python cli.py status                          # 查看运行状态
    python cli.py logs [--level ERROR] [--keyword xxx] [--trace xxx] [--limit 50] [--order desc|asc]
    python cli.py export [--output ./output_dir]
    python cli.py level <LEVEL>                    # 设置最小日志级别 DEBUG/INFO/WARN/ERROR
    python cli.py sample <RATE>                    # 设置全局采样率 0.0~1.0
    python cli.py service <SERVICE> <RATE>             # 设置服务采样率
    python cli.py trace add-trace <TRACE_ID>           # 添加 trace 白名单
    python cli.py rm-trace <TRACE_ID>              # 移除 trace 白名单
    python cli.py add-keyword <KEYWORD> <RATE>      # 添加关键词采样

通过环境变量配置 Agent：
    LOG_AGENT_CONFIG=./config.json   指定额外配置

支持 Python 脚本：
    python cli.py --config path/to/agent_instance.pickle
"""

import argparse
import sys
import json
import os
import time

from log_agent import LogAgent
from config import LogAgentConfig


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


def cmd_status(args, agent: LogAgent):
    print()
    agent.print_status()

    s = agent.get_status()
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


def cmd_logs(args, agent: LogAgent):
    agent.print_logs(
        level=args.level,
        keyword=args.keyword,
        trace_id=args.trace,
        limit=args.limit,
        order=args.order,
    )


def cmd_export(args, agent: LogAgent):
    output_dir = args.output or agent._config.crash_dump_dir
    path = agent.export_diagnostic_data(
        output_dir=output_dir,
        include_crash_dumps=not args.no_crash_dumps,
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


def cmd_level(args, agent: LogAgent):
    level = args.level.upper()
    try:
        agent.sampler.set_min_level(level)
        print(f"[OK] 最小日志级别已设置为: {level}")
        print(f"   低于 {level} 的日志将被过滤")
    except ValueError as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sample(args, agent: LogAgent):
    try:
        rate = float(args.rate)
        agent.sampler.set_global_sample_rate(rate)
        pct = rate * 100
        print(f"[OK] 全局采样率已设置为: {rate} ({pct:.0f}%)")
        print(f"   新写入的日志中每 100 条保留 {pct:.0f} 条")
    except ValueError as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_service(args, agent: LogAgent):
    try:
        rate = float(args.rate)
        agent.sampler.set_service_sample_rate(args.service, rate)
        print(f"[OK] 服务 '{args.service}' 采样率已设置为: {rate}")
    except ValueError as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_add_trace(args, agent: LogAgent):
    agent.sampler.add_trace_whitelist(args.trace_id)
    print(f"[OK] trace_id '{args.trace_id}' 已加入白名单，该 trace 的日志将全量保留")


def cmd_rm_trace(args, agent: LogAgent):
    agent.sampler.remove_trace_whitelist(args.trace_id)
    print(f"[OK] trace_id '{args.trace_id}' 已从白名单移除")


def cmd_add_keyword(args, agent: LogAgent):
    try:
        rate = float(args.rate)
        agent.sampler.add_keyword_rule(args.keyword, rate)
        print(f"[OK] 关键词 '{args.keyword}' 采样率已设置为: {rate}")
    except ValueError as e:
        print(f"[FAIL] 设置失败: {e}", file=sys.stderr)
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="logagent",
        description="LogAgent 本地管理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    subparsers.add_parser("status", help="查看运行状态")

    p_logs = subparsers.add_parser("logs", help="查询未上报的日志")
    p_logs.add_argument("--level", default=None, help="按级别过滤: DEBUG/INFO/WARN/ERROR")
    p_logs.add_argument("--keyword", default=None, help="按关键词过滤")
    p_logs.add_argument("--trace", default=None, help="按 trace_id 过滤")
    p_logs.add_argument("--limit", type=int, default=50, help="最多显示条数")
    p_logs.add_argument("--order", choices=["desc", "asc"], default="desc", help="排序: desc=从新到旧, asc=从旧到新")

    p_export = subparsers.add_parser("export", help="导出诊断数据包")
    p_export.add_argument("--output", default=None, help="输出目录")
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
        "status": cmd_status,
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
        print("  python cli.py status")
        print("  python cli.py logs --level ERROR --limit 100")
        print("  python cli.py export")
        print("  python cli.py level WARN")
        print("  python cli.py sample 0.5")
        print("  python cli.py service order-service 1.0")
        print("  python cli.py add-trace trace-abc123")
        sys.exit(0)

    args = parser.parse_args()

    if args.command in COMMAND_MAP:
        config = _load_config_from_env()
        agent = LogAgent(config)
        agent.start()

        try:
            handler = COMMAND_MAP[args.command]
            handler(args, agent)
        finally:
            agent.stop()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
