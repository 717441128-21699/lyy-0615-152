"""
测试第五轮新增功能：
  1. 日志查询：无论 order=desc/asc，都先取最近 limit 条，再排序
  2. 服务名贯穿：query_logs / export 支持 service 过滤；服务采样率优先级高于全局
  3. 跨进程管理：Agent 启动 HTTP 管理端口，CLI 通过 HTTP 控制正在运行的 Agent
  4. status 展示主备信息：当前目标、角色、切换次数、上次切换时间、恢复进度
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


def _make_agent(reporter_type="http", simulate_failure=False, simulate_success=False,
                disable_reporter=False, **kwargs):
    config_kwargs = dict(
        reporter_type=reporter_type,
        reporter_endpoint="http://localhost:19999/logs",
        reporter_flush_interval_ms=60000,
        reporter_connect_timeout_sec=0.2,
        reporter_max_retries=0,
        buffer_capacity=200,
    )
    config_kwargs.update(kwargs)
    config = LogAgentConfig(**config_kwargs)
    agent = LogAgent(config)
    if reporter_type == "http":
        if simulate_failure:
            agent._reporter_worker._reporter.set_simulate_failure(True)
        if simulate_success:
            agent._reporter_worker._reporter.set_simulate_success(True)
    agent._disable_reporter = disable_reporter
    return agent


def test_log_query_always_takes_latest():
    """功能 1：无论 desc/asc，都先拿最近 limit 条，再按 order 排序"""
    print("  [T1.1] order=desc 取最近 5 条并从新到旧排...", end=" ")
    agent = _make_agent(reporter_type="console")
    try:
        for i in range(20):
            agent._buffer.put(LogEntry("INFO", f"msg-{i}", service="svc"), block=False)
        time.sleep(0.05)

        logs = agent.query_logs(limit=5, order="desc")
        assert len(logs) == 5
        assert [l["message"] for l in logs] == ["msg-19", "msg-18", "msg-17", "msg-16", "msg-15"]
        print("PASS")
    finally:
        pass

    print("  [T1.2] order=asc 取最近 5 条并从旧到新排（关键改动）...", end=" ")
    agent = _make_agent(reporter_type="console")
    try:
        for i in range(20):
            agent._buffer.put(LogEntry("INFO", f"msg-{i}", service="svc"), block=False)
        time.sleep(0.05)

        logs = agent.query_logs(limit=5, order="asc")
        assert len(logs) == 5
        assert [l["message"] for l in logs] == ["msg-15", "msg-16", "msg-17", "msg-18", "msg-19"], \
            f"期望最近5条按升序，实际: {[l['message'] for l in logs]}"
        print("PASS")
    finally:
        pass


def test_service_name_end_to_end():
    """功能 2：服务名贯穿 query_logs、export、采样优先级"""
    print("  [T2.1] query_logs 支持按 service 过滤...", end=" ")
    agent = _make_agent(reporter_type="console")
    try:
        for i in range(5):
            agent._buffer.put(LogEntry("INFO", f"order-{i}", service="order-service"), block=False)
        for i in range(3):
            agent._buffer.put(LogEntry("INFO", f"pay-{i}", service="pay-service"), block=False)
        time.sleep(0.05)

        logs_order = agent.query_logs(service="order-service", limit=100)
        assert len(logs_order) == 5, f"期望 5 条 order，实际 {len(logs_order)}"
        for l in logs_order:
            assert l["service"] == "order-service"

        logs_pay = agent.query_logs(service="pay-service", limit=100)
        assert len(logs_pay) == 3
        print("PASS")
    finally:
        pass

    print("  [T2.2] 服务采样率优先级高于全局采样率...", end=" ")
    sc = SampleController(global_sample_rate=0.0)
    sc.set_service_sample_rate("vip-service", 1.0)

    # 跑 100 次，vip-service 应全部通过，普通服务应全部不通过
    vip_kept = 0
    normal_kept = 0
    for _ in range(100):
        if sc.should_keep(LogEntry("INFO", "x", service="vip-service"))[0]:
            vip_kept += 1
        if sc.should_keep(LogEntry("INFO", "x", service="ordinary"))[0]:
            normal_kept += 1
    assert vip_kept == 100, f"vip 应 100% 保留，实际 {vip_kept}"
    assert normal_kept == 0, f"普通服务应 0% 保留，实际 {normal_kept}"
    print("PASS")

    print("  [T2.3] export_diagnostic_data 支持 service 过滤...", end=" ")
    tmpdir = tempfile.mkdtemp(prefix="svc_export_")
    agent = _make_agent(reporter_type="console", crash_dump_dir=tmpdir)
    try:
        for i in range(3):
            agent._buffer.put(LogEntry("INFO", f"a-{i}", service="svc-a"), block=False)
        for i in range(2):
            agent._buffer.put(LogEntry("INFO", f"b-{i}", service="svc-b"), block=False)
        time.sleep(0.05)

        path = agent.export_diagnostic_data(output_dir=tmpdir, service="svc-a",
                                            include_crash_dumps=False)
        assert os.path.exists(path)

        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        header = json.loads(lines[0])
        assert header.get("filter_service") == "svc-a"
        entries = [json.loads(l) for l in lines[1:]]
        assert len(entries) == 3, f"期望 3 条 svc-a，实际 {len(entries)}"
        for e in entries:
            assert e["service"] == "svc-a"
        print("PASS")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_management_http_server():
    """功能 3：本地 HTTP 管理端口 + CLI 通过 HTTP 控制远程 Agent"""
    print("  [T3.1] Agent 启动后 management_port 可用...", end=" ")
    agent = _make_agent(simulate_failure=True)
    agent.start()
    try:
        assert agent.management_port > 0, f"management_port 应 > 0，实际 {agent.management_port}"
        assert agent.management_url is not None
        assert agent.management_url.startswith("http://")
        print(f"PASS (port={agent.management_port})")
    finally:
        agent.stop(timeout=0.5)

    print("  [T3.2] HTTP /health 和 /status 接口...", end=" ")
    import urllib.request
    import urllib.parse

    agent = _make_agent(simulate_failure=True)
    agent.start()
    try:
        base = agent.management_url
        # 从文件里拿 token
        token_path = os.path.join(agent._config.crash_dump_dir, ".agent_mgmt.json")
        with open(token_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        token = info["token"]

        url = f"{base}/health?token={urllib.parse.quote(token)}"
        with urllib.request.urlopen(url, timeout=2.0) as r:
            health = json.loads(r.read().decode())
        assert health.get("ok") is True
        assert health.get("pid") == os.getpid()

        url = f"{base}/status?token={urllib.parse.quote(token)}"
        with urllib.request.urlopen(url, timeout=2.0) as r:
            status = json.loads(r.read().decode())
        assert "buffer" in status
        assert "throughput" in status
        assert "sampling" in status
        print("PASS")
    finally:
        agent.stop(timeout=0.5)

    print("  [T3.3] HTTP 动态调整采样率，新日志立即生效...", end=" ")
    agent = _make_agent(simulate_failure=True)
    agent.start()
    try:
        base = agent.management_url
        token_path = os.path.join(agent._config.crash_dump_dir, ".agent_mgmt.json")
        with open(token_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        token = info["token"]

        # 先通过 HTTP 把最小级别设为 ERROR
        req = urllib.request.Request(
            f"{base}/sampling/level?token={urllib.parse.quote(token)}",
            data=json.dumps({"level": "ERROR"}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=2.0) as r:
            resp = json.loads(r.read().decode())
        assert resp.get("ok") is True

        # 现在写 INFO 应被过滤
        for _ in range(10):
            r = agent.log("INFO", "info-msg", service="svc")
            assert r == "sampled_out", f"期望 sampled_out，实际 {r}"
        # ERROR 应通过
        r = agent.log("ERROR", "error-msg", service="svc")
        assert r == "success"
        print("PASS")
    finally:
        agent.stop(timeout=0.5)

    print("  [T3.4] CLI 通过 HTTP 连接到正在运行的 Agent...", end=" ")
    agent = _make_agent(simulate_failure=True)
    agent.start()
    try:
        base = agent.management_url
        env = os.environ.copy()
        env["LOG_AGENT_MGMT_URL"] = base

        token_path = os.path.join(agent._config.crash_dump_dir, ".agent_mgmt.json")
        with open(token_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        env["LOG_AGENT_MGMT_TOKEN"] = info["token"]

        # 先 ping 看看是否识别为远程
        result = subprocess.run(
            [sys.executable, "cli.py", "ping"],
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env, timeout=10,
        )
        assert result.returncode == 0, f"ping 失败: {result.stderr}"
        assert "正在运行" in result.stdout or base in result.stdout, f"stdout={result.stdout}"

        # 通过 CLI 设置级别，再验证 Agent 内生效
        result = subprocess.run(
            [sys.executable, "cli.py", "level", "WARN"],
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env, timeout=10,
        )
        assert result.returncode == 0, f"level 失败: {result.stderr}"
        assert "WARN" in result.stdout

        # 验证 Agent 那边规则已生效
        r = agent.log("INFO", "should-be-filtered", service="svc")
        assert r == "sampled_out", f"通过 CLI 设置 WARN 后 INFO 应被过滤，实际 {r}"
        r = agent.log("WARN", "should-pass", service="svc")
        assert r == "success"
        print("PASS")
    finally:
        agent.stop(timeout=0.5)


def test_status_failover_display():
    """功能 4：status 中清晰展示主备信息"""
    print("  [T4.1] get_target_info 包含角色和切换信息...", end=" ")
    config = LogAgentConfig(
        reporter_endpoint="http://primary.example.com/logs",
        reporter_backup_endpoints=[
            "http://backup1.example.com/logs",
            "http://backup2.example.com/logs",
        ],
        reporter_failover_threshold=2,
        reporter_recover_after_success=3,
        reporter_connect_timeout_sec=0.2,
    )
    r = HttpLogReporter(config)
    r.set_simulate_failure(True, endpoint="http://primary.example.com/logs")
    r.set_simulate_success(True, endpoint="http://backup1.example.com/logs")
    r.set_simulate_success(True, endpoint="http://backup2.example.com/logs")

    for _ in range(2):
        r.report([LogEntry("INFO", "x", service="s")])

    info = r.get_target_info()
    assert info["current_endpoint_role"] != "primary", f"应切到备，实际 role={info['current_endpoint_role']}"
    assert info["failover"]["switch_count"] == 1
    assert info["failover"]["last_switch_reason"]
    assert "last_switch_time" in info["failover"]
    print("PASS")

    print("  [T4.2] get_target_info 包含所有地址一览及各地址失败数...", end=" ")
    all_eps = info.get("all_endpoints", [])
    assert len(all_eps) == 3
    failures = info["failover"].get("per_endpoint_failures", {})
    assert failures.get("http://primary.example.com/logs", 0) >= 2
    print(f"PASS (failures={failures})")

    print("  [T4.3] 切回主地址后 switch_count 更新...", end=" ")
    r.set_simulate_failure(False, endpoint="http://primary.example.com/logs")
    r.set_simulate_success(True, endpoint="http://primary.example.com/logs")

    for _ in range(10):
        r.report([LogEntry("INFO", "x", service="s")])

    info2 = r.get_target_info()
    assert info2["current_endpoint_role"] == "primary"
    assert info2["failover"]["switch_count"] == 2
    print(f"PASS (switch_count={info2['failover']['switch_count']})")


if __name__ == "__main__":
    print("\n======== 第五轮功能测试 ========\n")

    test_log_query_always_takes_latest()
    print()
    test_service_name_end_to_end()
    print()
    test_management_http_server()
    print()
    test_status_failover_display()

    print("\n======== 全部通过 ✓ ========\n")
