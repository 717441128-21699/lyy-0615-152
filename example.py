"""
高吞吐日志收集 Agent 使用示例。
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from log_agent import LogAgent
from config import LogAgentConfig


def example_basic():
    """基本使用示例。"""
    print("=" * 60)
    print("示例1：基本使用")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=10000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=50,
        reporter_flush_interval_ms=50,
    )

    agent = LogAgent(config)
    agent.start()

    agent.debug("这是一条调试日志")
    agent.info("这是一条信息日志")
    agent.warn("这是一条警告日志")
    agent.error("这是一条错误日志", extra={"error_code": 500})

    agent.info("带 trace_id 的日志", trace_id="abc-123")

    time.sleep(0.1)

    stats = agent.get_stats()
    print(f"写入: {stats['write_count']} 条")
    print(f"上报: {stats['reporter']['reported_count']} 条")

    agent.stop()
    print()


def example_overflow_strategy():
    """满溢策略配置示例。"""
    print("=" * 60)
    print("示例2：满溢策略配置")
    print("=" * 60)

    strategies = ["drop_oldest", "drop_newest", "block"]

    for strategy in strategies:
        print(f"\n--- 策略: {strategy} ---")

        config = LogAgentConfig(
            buffer_capacity=100,
            overflow_strategy=strategy,
            reporter_type="http",
        )

        agent = LogAgent(config)
        agent.reporter.set_simulate_failure(True)
        agent.start()

        for i in range(150):
            success = agent.info(f"log-{i}")
            if not success and strategy == "block":
                print(f"  第 {i} 条写入失败（非阻塞模式）")

        time.sleep(0.05)
        stats = agent.get_stats()
        print(f"  缓冲区大小: {stats['buffer']['size']}")
        print(f"  溢出次数: {stats['buffer']['overflow_count']}")
        print(f"  丢弃最老: {stats['buffer']['dropped_oldest_count']}")
        print(f"  丢弃最新: {stats['buffer']['dropped_newest_count']}")

        agent.stop()

    print()


def example_dynamic_strategy():
    """动态切换策略示例。"""
    print("=" * 60)
    print("示例3：动态切换满溢策略")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=50,
        overflow_strategy="drop_oldest",
        reporter_type="http",
    )

    agent = LogAgent(config)
    agent.reporter.set_simulate_failure(True)
    agent.start()

    print("阶段1：drop_oldest 模式，写入 80 条...")
    for i in range(80):
        agent.info(f"phase1-{i}")
    stats = agent.get_stats()
    print(f"  缓冲区: {stats['buffer']['size']}, "
          f"丢弃最老: {stats['buffer']['dropped_oldest_count']}")

    print("\n阶段2：切换到 drop_newest，再写入 50 条...")
    agent.set_overflow_strategy("drop_newest")
    for i in range(50):
        agent.info(f"phase2-{i}")
    stats = agent.get_stats()
    print(f"  缓冲区: {stats['buffer']['size']}, "
          f"丢弃最新: {stats['buffer']['dropped_newest_count']}")

    print("\n阶段3：恢复网络，日志开始上报...")
    agent.reporter.set_simulate_failure(False)
    time.sleep(0.2)
    stats = agent.get_stats()
    print(f"  已上报: {stats['reporter']['reported_count']}")

    agent.stop()
    print()


def example_crash_protection():
    """崩溃保护示例。"""
    print("=" * 60)
    print("示例4：崩溃保护")
    print("=" * 60)

    import tempfile
    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="example_crash_")
    try:
        config = LogAgentConfig(
            buffer_capacity=1000,
            overflow_strategy="drop_oldest",
            reporter_type="http",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
        )

        agent = LogAgent(config)
        agent.reporter.set_simulate_failure(True)
        agent.start()

        print("写入关键日志...")
        for i in range(50):
            agent.info(f"critical-log-{i}", extra={"important": True})

        print(f"缓冲区中有 {len(agent.buffer)} 条日志等待上报")

        print("\n模拟进程崩溃前的手动转储...")
        dumped = agent.crash_dump()
        print(f"转储了 {dumped} 条日志到磁盘")

        dump_files = agent.crash_protector.list_dump_files()
        if dump_files:
            print(f"转储文件: {dump_files[0]}")

        agent.stop()
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def example_high_throughput():
    """高吞吐场景示例。"""
    print("=" * 60)
    print("示例5：高吞吐场景（模拟业务线程）")
    print("=" * 60)

    import threading

    config = LogAgentConfig(
        buffer_capacity=50000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=200,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.start()

    def business_thread(thread_id):
        for i in range(1000):
            agent.info(
                f"thread-{thread_id} processing request",
                trace_id=f"trace-{thread_id}-{i}",
                extra={"request_id": i, "thread": thread_id}
            )

    print("启动 10 个业务线程，每个写 1000 条日志...")
    start = time.perf_counter()

    threads = []
    for i in range(10):
        t = threading.Thread(target=business_thread, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start
    total = 10 * 1000

    print(f"\n写入 {total} 条日志，耗时 {elapsed:.3f} 秒")
    print(f"吞吐: {total / elapsed:,.0f} 条/秒")

    time.sleep(0.2)
    stats = agent.get_stats()
    print(f"已上报: {stats['reporter']['reported_count']} 条")
    print(f"缓冲区剩余: {stats['buffer']['size']} 条")

    agent.stop()
    print()


if __name__ == "__main__":
    example_basic()
    example_overflow_strategy()
    example_dynamic_strategy()
    example_crash_protection()
    example_high_throughput()

    print("=" * 60)
    print("所有示例运行完成！")
    print("=" * 60)
