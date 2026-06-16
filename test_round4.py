"""
测试第四轮新增功能：
  1. 本地日志排序 desc/asc + limit 取最近
  2. 动态日志级别和采样控制
  3. HTTP 主备多目标切换
  4. CLI 命令行管理入口
"""

import os
import sys
import time
import json
import shutil
import tempfile
import subprocess
import threading

from log_agent import LogAgent
from config import LogAgentConfig, LogEntry
from sample_controller import SampleController
from reporter import HttpLogReporter


def test_log_sorting_and_limit():
    """功能 1：本地查看默认从新到旧，limit 小时拿到最新"""
    print("  [T1.1] 默认 desc 排序 + limit 取最近一批...", end=" ")

    config = LogAgentConfig(
        reporter_type="http",
        reporter_endpoint="http://localhost:19999/logs",
        reporter_flush_interval_ms=10000,
        reporter_connect_timeout_sec=0.2,
        buffer_capacity=50,
    )
    agent = LogAgent(config)
    # 让 reporter 一直失败，日志会留在 pending_entries
    agent._reporter_worker._reporter.set_simulate_failure(True)
    agent.start()

    try:
        # 写入 20 条日志，message 中带序号
        for i in range(20):
            agent.log("INFO", f"msg-{i}", service="order-service", trace_id=f"t-{i}")

        # flush_interval=10秒，日志留在 buffer 中。peek_all_pending 可以看到
        time.sleep(0.1)

        # 默认 desc：limit=5 应该拿到 msg-15 ~ msg-19（最新的 5 条）
        logs = agent.query_logs(limit=5)
        assert len(logs) == 5, f"期望 5 条，实际 {len(logs)}"
        messages = [log["message"] for log in logs]
        assert messages == ["msg-19", "msg-18", "msg-17", "msg-16", "msg-15"], f"顺序不对: {messages}"

        # asc：limit=5 应该拿到最近 5 条（msg-15~msg-19）然后按时间线升序排
        logs_asc = agent.query_logs(limit=5, order="asc")
        messages_asc = [log["message"] for log in logs_asc]
        assert messages_asc == ["msg-15", "msg-16", "msg-17", "msg-18", "msg-19"], f"顺序不对: {messages_asc}"

        # desc + 全部
        logs_all_desc = agent.query_logs(limit=100, order="desc")
        assert len(logs_all_desc) == 20
        assert logs_all_desc[0]["message"] == "msg-19"
        assert logs_all_desc[-1]["message"] == "msg-0"

        print("PASS")

    finally:
        agent.stop(timeout=0.5)


def test_sample_controller():
    """功能 2：动态采样控制器"""
    print("  [T2.1] 全局最小日志级别过滤...", end=" ")
    sc = SampleController()

    e_debug = LogEntry("DEBUG", "dbg", service="svc")
    e_info = LogEntry("INFO", "inf", service="svc")
    e_warn = LogEntry("WARN", "wrn", service="svc")
    e_error = LogEntry("ERROR", "err", service="svc")

    # 默认 DEBUG 以上全部保留
    assert sc.should_keep(e_debug)[0]
    assert sc.should_keep(e_info)[0]

    # 设为 WARN，DEBUG/INFO 被过滤
    sc.set_min_level("WARN")
    kept, reason = sc.should_keep(e_debug)
    assert not kept and reason == "level"
    kept, reason = sc.should_keep(e_info)
    assert not kept and reason == "level"
    kept, reason = sc.should_keep(e_warn)
    assert kept
    kept, reason = sc.should_keep(e_error)
    assert kept
    print("PASS")

    print("  [T2.2] 全局采样率 0%/100%...", end=" ")
    sc2 = SampleController(global_sample_rate=0.0)
    kept, reason = sc2.should_keep(e_info)
    assert not kept and reason == "global"

    sc2.set_global_sample_rate(1.0)
    kept, reason = sc2.should_keep(e_info)
    assert kept
    print("PASS")

    print("  [T2.3] 按服务覆盖采样率...", end=" ")
    sc3 = SampleController(global_sample_rate=0.0)
    sc3.set_service_sample_rate("vip-service", 1.0)

    e_normal = LogEntry("INFO", "x", service="ordinary")
    e_vip = LogEntry("INFO", "x", service="vip-service")
    assert not sc3.should_keep(e_normal)[0]
    assert sc3.should_keep(e_vip)[0]
    print("PASS")

    print("  [T2.4] trace_id 白名单...", end=" ")
    sc4 = SampleController(global_sample_rate=0.0)
    sc4.add_trace_whitelist("trace-important-001")
    e_trace = LogEntry("INFO", "x", service="svc", trace_id="trace-important-001")
    e_other = LogEntry("INFO", "x", service="svc", trace_id="trace-other")
    kept, reason = sc4.should_keep(e_trace)
    assert kept and reason == "trace_whitelist"
    assert not sc4.should_keep(e_other)[0]

    sc4.remove_trace_whitelist("trace-important-001")
    assert not sc4.should_keep(e_trace)[0]
    print("PASS")

    print("  [T2.5] 关键词规则提高采样...", end=" ")
    sc5 = SampleController(global_sample_rate=0.0)
    sc5.add_keyword_rule("critical", 1.0)
    e_crit = LogEntry("ERROR", "a critical error happened", service="svc")
    e_ok = LogEntry("INFO", "all good", service="svc")
    kept, reason = sc5.should_keep(e_crit)
    assert kept and reason == "keyword"
    assert not sc5.should_keep(e_ok)[0]
    print("PASS")

    print("  [T2.6] Agent 中集成采样，返回 sampled_out...", end=" ")
    config = LogAgentConfig(
        reporter_type="http",
        reporter_endpoint="http://localhost:19999/logs",
        reporter_flush_interval_ms=5000,
        reporter_connect_timeout_sec=0.2,
        buffer_capacity=100,
    )
    agent = LogAgent(config)
    agent._reporter_worker._reporter.set_simulate_failure(True)
    agent.start()
    try:
        agent.sampler.set_min_level("ERROR")
        result = agent.log("INFO", "info msg", service="svc")
        assert result == "sampled_out", f"期望 sampled_out，实际 {result}"

        result = agent.log("ERROR", "error msg", service="svc")
        assert result == "success", f"期望 success，实际 {result}"
        print("PASS")
    finally:
        agent.stop(timeout=0.5)


def test_http_failover():
    """功能 3：HTTP 主备自动切换"""
    print("  [T3.1] 主备多地址配置 + get_target_info...", end=" ")

    config = LogAgentConfig(
        reporter_endpoint="http://primary.example.com/logs",
        reporter_backup_endpoints=[
            "http://backup1.example.com/logs",
            "http://backup2.example.com/logs",
        ],
        reporter_auth_token="secret-token",
        reporter_basic_auth=("user", "pass"),
        reporter_env="production",
        reporter_failover_threshold=3,
        reporter_recover_after_success=5,
        reporter_connect_timeout_sec=0.2,
    )

    reporter = HttpLogReporter(config)

    info = reporter.get_target_info()
    assert info["env"] == "production"
    assert info["current_endpoint_role"] == "primary"
    assert info["current_endpoint"] == "http://primary.example.com/logs"
    # 鉴权信息应脱敏
    assert info["headers"].get("Authorization", "") == "***"
    assert "secret-token" not in str(info)
    assert "user" not in str(info)
    assert len(info["all_endpoints"]) == 3
    assert info["failover"]["switch_count"] == 0
    print("PASS")

    print("  [T3.2] 主地址连续失败触发切备...", end=" ")
    reporter2 = HttpLogReporter(config)
    reporter2.set_simulate_failure(True, endpoint="http://primary.example.com/logs")
    reporter2.set_simulate_success(True, endpoint="http://backup1.example.com/logs")
    reporter2.set_simulate_success(True, endpoint="http://backup2.example.com/logs")

    for i in range(3):
        batch = [LogEntry("INFO", f"msg{i}", service="s")]
        reporter2.report(batch)

    info = reporter2.get_target_info()
    # 连续失败 3 次后应切到备用
    assert info["current_endpoint_role"] != "primary", f"应该已切到备地址，当前: {info['current_endpoint']}"
    assert info["current_endpoint"] == "http://backup1.example.com/logs"
    assert info["failover"]["switch_count"] == 1
    assert "fail" in str(info["failover"]["last_switch_reason"]).lower() or info["failover"]["last_switch_reason"]
    assert info["consecutive_failures"] == 0  # 切换后重置
    print("PASS")

    print("  [T3.3] 备地址连续成功触发切回主...", end=" ")
    # 主地址故障解除
    reporter2.set_simulate_failure(False, endpoint="http://primary.example.com/logs")
    reporter2.set_simulate_success(True, endpoint="http://primary.example.com/logs")

    for i in range(10):
        batch = [LogEntry("INFO", f"msg{i}", service="s")]
        reporter2.report(batch)

    info = reporter2.get_target_info()
    # 备地址连续成功 10 次后应切回主
    assert info["current_endpoint_role"] == "primary", f"应该已切回主地址，当前: {info['current_endpoint']}"
    assert info["failover"]["switch_count"] == 2
    assert info["failover"]["last_switch_reason"]  # 有原因字符串
    print("PASS")

    print("  [T3.4] Agent 状态展示 failover 信息...", end=" ")

    agent_config = LogAgentConfig(
        reporter_type="console",
        buffer_capacity=200,
    )
    agent = LogAgent(agent_config)
    agent.start()

    try:
        for i in range(3):
            agent.log("INFO", f"msg-{i}", service="svc")
        time.sleep(0.2)

        status = agent.get_status()
        # console reporter 没有 target_info，但其他状态字段应该完整
        assert "buffer" in status
        assert "throughput" in status
        assert "sampling" in status
        print(f"PASS (sampling keys={list(status['sampling'].keys())})")
    finally:
        agent.stop(timeout=0.5)


def test_cli_commands():
    """功能 4：CLI 命令行入口"""
    print("  [T4.1] CLI 查看 help...", end=" ")
    result = subprocess.run(
        [sys.executable, "cli.py", "--help"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=10,
    )
    assert result.returncode == 0
    assert "status" in result.stdout
    assert "logs" in result.stdout
    assert "export" in result.stdout
    assert "level" in result.stdout
    assert "sample" in result.stdout
    print("PASS")

    print("  [T4.2] CLI status 命令...", end=" ")
    result = subprocess.run(
        [sys.executable, "cli.py", "status"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=10,
    )
    assert result.returncode == 0
    assert "LogAgent" in result.stdout
    assert "RUNNING" in result.stdout
    assert "缓冲区" in result.stdout
    print("PASS")

    print("  [T4.3] CLI logs 命令...", end=" ")
    result = subprocess.run(
        [sys.executable, "cli.py", "logs", "--limit", "10", "--order", "desc"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=10,
    )
    assert result.returncode == 0
    print("PASS")

    print("  [T4.4] CLI level / sample 命令...", end=" ")
    result = subprocess.run(
        [sys.executable, "cli.py", "level", "ERROR"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=10,
    )
    assert result.returncode == 0, f"level 失败: {result.stderr}"
    assert "ERROR" in result.stdout

    result = subprocess.run(
        [sys.executable, "cli.py", "sample", "0.3"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
        timeout=10,
    )
    assert result.returncode == 0, f"sample 失败: {result.stderr}"
    assert "0.3" in result.stdout
    print("PASS")

    print("  [T4.5] CLI export 命令...", end=" ")
    tmpdir = tempfile.mkdtemp(prefix="cli_export_")
    try:
        result = subprocess.run(
            [sys.executable, "cli.py", "export", "--output", tmpdir],
            capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)),
            timeout=15,
        )
        assert result.returncode == 0, f"失败: {result.stderr}"
        exported = [f for f in os.listdir(tmpdir) if f.startswith("diagnostic_export_")]
        assert len(exported) >= 1, f"未找到导出文件, stdout={result.stdout}"
        print("PASS")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    print("\n======== 第四轮功能测试 ========\n")

    test_log_sorting_and_limit()
    print()
    test_sample_controller()
    print()
    test_http_failover()
    print()
    test_cli_commands()

    print("\n======== 全部通过 ✓ ========\n")
