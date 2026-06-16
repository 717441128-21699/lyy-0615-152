"""
新功能测试：
1. 本地日志查询（按级别、关键词、trace_id 过滤）
2. HTTP 自定义请求头和鉴权
3. 一键导出诊断数据
4. 运行状态监控
"""

import os
import sys
import time
import tempfile
import shutil
import json

sys.path.insert(0, os.path.dirname(__file__))

from log_agent import LogAgent
from config import LogAgentConfig


def test_local_query():
    """测试本地日志查询功能。"""
    print("=" * 70)
    print("新功能 1：本地日志查询（按级别、关键词、trace_id 过滤）")
    print("=" * 70)

    config = LogAgentConfig(
        buffer_capacity=1000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_flush_interval_ms=1000,
    )

    agent = LogAgent(config)
    agent.reporter.set_simulate_failure(True)
    agent.start()

    print("\n写入一些测试日志...")
    agent.info("user login successful", trace_id="trace-001")
    agent.debug("cache miss for key=user:123", trace_id="trace-001")
    agent.warn("slow query detected (1.2s)", trace_id="trace-002")
    agent.error("database connection failed", trace_id="trace-003")
    agent.info("order #12345 created", trace_id="trace-004")
    agent.error("payment timeout for order #12345", trace_id="trace-004")
    agent.info("heartbeat ok")

    time.sleep(0.05)
    print(f"缓冲区当前有 {len(agent.buffer)} 条未上报日志")

    print("\n--- 查询：只看 ERROR 级别 ---")
    results = agent.query_logs(level="ERROR", limit=10)
    for r in results:
        print(f"  [{r['level']}] {r['message']}  trace={r.get('trace_id')}")
    assert len(results) == 2, "应该有 2 条 ERROR"

    print("\n--- 查询：包含 'order' 关键词 ---")
    results = agent.query_logs(keyword="order", limit=10)
    for r in results:
        print(f"  [{r['level']}] {r['message']}")
    assert len(results) == 2, "应该有 2 条包含 order"

    print("\n--- 查询：trace_id = trace-001 ---")
    results = agent.query_logs(trace_id="trace-001", limit=10)
    for r in results:
        print(f"  [{r['level']}] {r['message']}")
    assert len(results) == 2, "应该有 2 条 trace-001"

    print("\n--- 查询：WARN 级别 + 关键词 'slow' ---")
    results = agent.query_logs(level="WARN", keyword="slow", limit=10)
    for r in results:
        print(f"  [{r['level']}] {r['message']}")
    assert len(results) == 1, "应该有 1 条符合"

    print("\n--- print_logs 直接打印（ERROR 级别）---")
    agent.print_logs(level="ERROR", limit=10)

    agent.stop()
    print("\n✓ 本地日志查询测试通过")
    print()


def test_http_headers_and_auth():
    """测试 HTTP 自定义请求头和鉴权。"""
    print("=" * 70)
    print("新功能 2：HTTP 自定义请求头、鉴权、目标信息")
    print("=" * 70)

    config = LogAgentConfig(
        buffer_capacity=1000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_endpoint="http://log-server.prod.example.com/api/v1/logs",
        reporter_env="production",
        reporter_headers={
            "X-App-Name": "order-service",
            "X-Cluster": "ap-southeast-1",
        },
        reporter_auth_token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        reporter_connect_timeout_sec=3.0,
    )

    agent = LogAgent(config)
    agent.start()

    print("\nHTTP 上报目标信息：")
    target_info = agent.reporter.get_target_info()

    print(f"  环境:       {target_info['env']}")
    print(f"  端点:       {target_info['endpoint']}")
    print(f"  超时:       {target_info['timeout_sec']}s")
    print(f"  请求头:")
    for k, v in target_info["headers"].items():
        print(f"    {k}: {v}")

    assert target_info["env"] == "production"
    assert "X-App-Name" in target_info["headers"]
    assert target_info["headers"]["X-App-Name"] == "order-service"
    assert target_info["headers"]["Authorization"] == "***", "鉴权信息应该脱敏"
    assert target_info["headers"]["User-Agent"] == "log-agent/1.0 (env=production)"

    print("\n模拟网络故障，观察错误信息是否包含目标...")
    agent.reporter.set_simulate_failure(True)

    for i in range(5):
        agent.info(f"test log {i}")

    time.sleep(0.3)

    target_info2 = agent.reporter.get_target_info()
    print(f"  连续失败次数: {target_info2['consecutive_failures']}")
    print(f"  最后失败原因: {target_info2['last_failure_reason']}")
    assert target_info2["consecutive_failures"] >= 1
    assert target_info2["last_failure_reason"] == "simulated failure"

    print("\n--- 不同环境配置示例 ---")
    config_dev = LogAgentConfig(
        reporter_endpoint="http://log-server.dev.local:8080/logs",
        reporter_env="development",
        reporter_basic_auth=("dev-user", "dev-pass123"),
    )
    agent_dev = LogAgent(config_dev)
    target_dev = agent_dev.reporter.get_target_info()
    print(f"  开发环境: {target_dev['env']} -> {target_dev['endpoint']}")
    print(f"    鉴权: {target_dev['headers'].get('Authorization', 'none')}")

    config_prod = LogAgentConfig(
        reporter_endpoint="https://log.example.com/v2/batch",
        reporter_env="production",
        reporter_auth_token="prod-token-xxx",
    )
    agent_prod = LogAgent(config_prod)
    target_prod = agent_prod.reporter.get_target_info()
    print(f"  生产环境: {target_prod['env']} -> {target_prod['endpoint']}")
    print(f"    鉴权: {target_prod['headers'].get('Authorization', 'none')}")

    agent.stop()
    print("\n✓ HTTP 请求头与鉴权测试通过")
    print()


def test_export_diagnostic_data():
    """测试一键导出诊断数据。"""
    print("=" * 70)
    print("新功能 3：一键导出诊断数据（积压 + 崩溃转储）")
    print("=" * 70)

    tmp_dir = tempfile.mkdtemp(prefix="diag_test_")
    try:
        config = LogAgentConfig(
            buffer_capacity=1000,
            overflow_strategy="drop_oldest",
            reporter_type="http",
            reporter_endpoint="http://log-server.example.com/logs",
            reporter_env="test",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
            reporter_flush_interval_ms=1000,
        )

        agent = LogAgent(config)
        agent.reporter.set_simulate_failure(True)
        agent.start()

        print("写入一些日志到缓冲区...")
        for i in range(30):
            agent.info(f"pending log #{i}", extra={"seq": i})

        time.sleep(0.05)
        print(f"缓冲区当前积压: {len(agent.buffer)} 条")

        print("\n先模拟一次崩溃，生成转储文件...")
        dumped = agent.crash_dump()
        print(f"崩溃转储: {dumped} 条")

        agent.stop()

        print("\n重新启动 Agent，再写入一些新日志...")
        config2 = LogAgentConfig(
            buffer_capacity=1000,
            overflow_strategy="drop_oldest",
            reporter_type="http",
            reporter_endpoint="http://log-server.example.com/logs",
            reporter_env="test",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=False,
            reporter_flush_interval_ms=1000,
        )
        agent2 = LogAgent(config2)
        agent2.reporter.set_simulate_failure(True)
        agent2.start()

        for i in range(20):
            agent2.info(f"new pending log #{i}", extra={"seq": i})

        time.sleep(0.05)

        dump_files = agent2.crash_protector.list_dump_files()
        print(f"未归档崩溃转储文件: {len(dump_files)} 个")
        print(f"当前缓冲区积压: {len(agent2.buffer)} 条")

        print("\n调用 export_diagnostic_data() 一键导出...")
        export_path = agent2.export_diagnostic_data(output_dir=tmp_dir)

        print(f"导出文件: {export_path}")
        assert os.path.exists(export_path), "导出文件应该存在"

        with open(export_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]

        print(f"文件总行数: {len(lines)}")

        header = json.loads(lines[0])
        print(f"\n诊断头部信息:")
        print(f"  类型: {header.get('_type')}")
        print(f"  导出时间: {header.get('export_time_str')}")
        print(f"  PID: {header.get('pid')}")
        print(f"  配置: {header.get('config')}")
        print(f"  目标: {header.get('target_info', {}).get('endpoint')}")

        assert header["_type"] == "diagnostic_header"

        print(f"\n日志条目来源分布:")
        sources = {}
        for line in lines[1:]:
            entry = json.loads(line)
            src = entry.get("_source", "unknown")
            sources[src] = sources.get(src, 0) + 1

        for src, count in sources.items():
            print(f"  {src}: {count} 条")

        assert "pending" in sources, "应该有 pending 来源的日志"
        assert any(k.startswith("crash_dump:") for k in sources), "应该有 crash_dump 来源的日志"

        total_entries = len(lines) - 1
        print(f"\n总计导出 {total_entries} 条日志条目")

        agent2.stop()
        print("\n✓ 诊断数据导出测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_status_monitoring():
    """测试运行状态监控。"""
    print("=" * 70)
    print("新功能 4：运行状态监控（积压、QPS、重试、丢弃）")
    print("=" * 70)

    config = LogAgentConfig(
        buffer_capacity=2000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_endpoint="http://log-server.example.com/logs",
        reporter_env="staging",
        reporter_batch_size=50,
        reporter_flush_interval_ms=20,
    )

    agent = LogAgent(config)
    agent.reporter.set_simulate_delay(5)
    agent.start()

    print("写入 500 条日志...")
    for i in range(500):
        agent.info(f"test log {i}")

    time.sleep(0.2)

    print("\nget_status() 返回结构化状态:")
    status = agent.get_status()
    print(f"  运行中: {status['running']}")
    print(f"  运行时长: {status['uptime_sec']:.1f}s")
    print(f"  溢出策略: {status['overflow_strategy']}")
    print(f"  缓冲区: {status['buffer']['size']:,}/{status['buffer']['capacity']:,} "
          f"({status['buffer']['usage_pct']}%)")
    print(f"  近期 QPS: {status['throughput']['qps_recent']:,.1f}")
    print(f"  平均 QPS: {status['throughput']['qps_avg']:,.1f}")
    print(f"  总写入: {status['throughput']['total_written']:,}")
    print(f"  总上报: {status['throughput']['total_reported']:,}")
    print(f"  积压总数: {status['backlog']['total']:,}")
    print(f"    缓冲区内: {status['backlog']['in_buffer']:,}")
    print(f"    重试中: {status['backlog']['in_retry']:,}")
    print(f"  丢弃总数: {status['errors']['total_dropped']:,}")
    print(f"  重试次数: {status['errors']['total_retries']:,}")

    assert status["running"] is True
    assert status["throughput"]["total_written"] == 500
    assert status["throughput"]["qps_recent"] >= 0

    print("\nprint_status() 可视化状态表:")
    agent.print_status()

    print("\n--- 模拟网络故障，看状态变化 ---")
    agent.reporter.set_simulate_failure(True)

    for i in range(3000):
        agent.info(f"more log {i}")

    time.sleep(0.2)

    print("网络故障后的状态:")
    agent.print_status()

    status2 = agent.get_status()
    assert status2["errors"]["total_dropped"] > 0, "应该有丢弃"
    assert status2["target"]["consecutive_failures"] >= 1, "应该有连续失败记录"

    agent.stop()
    print("\n✓ 运行状态监控测试通过")
    print()


if __name__ == "__main__":
    test_local_query()
    test_http_headers_and_auth()
    test_export_diagnostic_data()
    test_status_monitoring()

    print("=" * 70)
    print("所有新功能测试通过！")
    print("=" * 70)
