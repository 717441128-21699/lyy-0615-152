"""
性能测试：验证业务线程写日志几乎零阻塞。
"""

import time
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from log_agent import LogAgent
from config import LogAgentConfig


def benchmark_single_thread():
    """单线程写入性能测试。"""
    print("=" * 60)
    print("单线程写入性能测试")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=100000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=100,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.start()

    n = 100000

    start = time.perf_counter()
    for i in range(n):
        agent.info(f"test log message #{i}", extra={"index": i})
    end = time.perf_counter()

    elapsed = end - start
    qps = n / elapsed

    print(f"写入 {n} 条日志，耗时 {elapsed:.3f} 秒")
    print(f"吞吐率: {qps:,.0f} 条/秒")
    print(f"单条平均耗时: {elapsed / n * 1_000_000:.2f} 微秒")

    time.sleep(0.2)

    stats = agent.get_stats()
    print(f"\n缓冲区状态: size={stats['buffer']['size']}, "
          f"溢出次数={stats['buffer']['overflow_count']}")
    print(f"已上报: {stats['reporter']['reported_count']} 条")

    agent.stop()
    print()


def benchmark_multi_thread(num_threads=8):
    """多线程并发写入性能测试。"""
    print("=" * 60)
    print(f"多线程写入性能测试 ({num_threads} 个生产者线程)")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=200000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=200,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.start()

    n_per_thread = 25000
    total = num_threads * n_per_thread

    latencies = []

    def worker(thread_id):
        local_latencies = []
        for i in range(n_per_thread):
            t0 = time.perf_counter()
            agent.info(f"thread-{thread_id} log #{i}")
            t1 = time.perf_counter()
            local_latencies.append(t1 - t0)
        latencies.extend(local_latencies)

    start = time.perf_counter()

    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()
    elapsed = end - start
    qps = total / elapsed

    latencies.sort()
    p50 = latencies[len(latencies) // 2] * 1_000_000
    p99 = latencies[int(len(latencies) * 0.99)] * 1_000_000
    p999 = latencies[int(len(latencies) * 0.999)] * 1_000_000

    print(f"共写入 {total} 条日志，耗时 {elapsed:.3f} 秒")
    print(f"吞吐率: {qps:,.0f} 条/秒")
    print(f"延迟分布:")
    print(f"  P50:  {p50:.2f} 微秒")
    print(f"  P99:  {p99:.2f} 微秒")
    print(f"  P999: {p999:.2f} 微秒")

    time.sleep(0.2)

    stats = agent.get_stats()
    print(f"\n缓冲区状态: size={stats['buffer']['size']}, "
          f"溢出次数={stats['buffer']['overflow_count']}")

    agent.stop()
    print()


def compare_with_sync_logging():
    """
    对比同步日志和异步日志对业务线程的影响。

    模拟场景：业务逻辑处理 + 写日志
    """
    print("=" * 60)
    print("同步 vs 异步 对业务线程的影响对比")
    print("=" * 60)

    n = 10000
    mock_work_time = 0.001  # 每次业务处理 1ms

    print(f"场景：业务处理 {mock_work_time*1000}ms + 写日志，循环 {n} 次")
    print()

    start = time.perf_counter()
    for i in range(n):
        time.sleep(mock_work_time)
        print(f"[INFO] sync log message #{i}")
    sync_elapsed = time.perf_counter() - start

    print(f"同步日志总耗时: {sync_elapsed:.3f} 秒")
    print()

    config = LogAgentConfig(
        buffer_capacity=50000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
    )
    agent = LogAgent(config)
    agent.start()

    start = time.perf_counter()
    for i in range(n):
        time.sleep(mock_work_time)
        agent.info(f"async log message #{i}")
    async_elapsed = time.perf_counter() - start

    print(f"异步日志总耗时: {async_elapsed:.3f} 秒")
    print(f"加速比: {sync_elapsed / async_elapsed:.2f}x")
    print(f"节省时间: {(sync_elapsed - async_elapsed) / sync_elapsed * 100:.1f}%")

    agent.stop()
    print()


if __name__ == "__main__":
    benchmark_single_thread()
    benchmark_multi_thread(num_threads=8)

    try:
        compare_with_sync_logging()
    except Exception as e:
        print(f"对比测试跳过: {e}")
