"""
核心性能测试：验证业务线程写日志几乎零阻塞。
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
        buffer_capacity=200000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=500,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.start()

    n = 200000
    latencies = []

    start = time.perf_counter()
    t0 = start
    for i in range(n):
        t_before = time.perf_counter()
        agent.info(f"test log message #{i}", extra={"index": i})
        t_after = time.perf_counter()
        latencies.append(t_after - t_before)
    end = time.perf_counter()

    elapsed = end - start
    qps = n / elapsed

    latencies.sort()
    p50 = latencies[len(latencies) // 2] * 1_000_000
    p99 = latencies[int(len(latencies) * 0.99)] * 1_000_000
    p999 = latencies[int(len(latencies) * 0.999)] * 1_000_000
    p9999 = latencies[int(len(latencies) * 0.9999)] * 1_000_000 if n >= 10000 else 0

    print(f"写入 {n:,} 条日志，耗时 {elapsed:.3f} 秒")
    print(f"吞吐率: {qps:,.0f} 条/秒")
    print(f"平均延迟: {elapsed / n * 1_000_000:.2f} 微秒")
    print(f"延迟分布:")
    print(f"  P50:   {p50:.2f} 微秒")
    print(f"  P99:   {p99:.2f} 微秒")
    print(f"  P999:  {p999:.2f} 微秒")
    if p9999:
        print(f"  P9999: {p9999:.2f} 微秒")

    time.sleep(0.3)

    stats = agent.get_stats()
    print(f"\n缓冲区状态: size={stats['buffer']['size']:,}, "
          f"溢出={stats['buffer']['overflow_count']:,}")
    print(f"已上报: {stats['reporter']['reported_count']:,} 条")

    agent.stop()
    print()


def benchmark_multi_thread(num_threads=8):
    """多线程并发写入性能测试。"""
    print("=" * 60)
    print(f"多线程写入性能测试 ({num_threads} 个生产者线程)")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=500000,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=500,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.start()

    n_per_thread = 50000
    total = num_threads * n_per_thread

    all_latencies = []
    latencies_lock = threading.Lock()

    def worker(thread_id):
        local_latencies = []
        for i in range(n_per_thread):
            t0 = time.perf_counter()
            agent.info(f"thread-{thread_id} log #{i}")
            t1 = time.perf_counter()
            local_latencies.append(t1 - t0)
        with latencies_lock:
            all_latencies.extend(local_latencies)

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

    all_latencies.sort()
    p50 = all_latencies[len(all_latencies) // 2] * 1_000_000
    p99 = all_latencies[int(len(all_latencies) * 0.99)] * 1_000_000
    p999 = all_latencies[int(len(all_latencies) * 0.999)] * 1_000_000

    print(f"共写入 {total:,} 条日志，耗时 {elapsed:.3f} 秒")
    print(f"吞吐率: {qps:,.0f} 条/秒")
    print(f"延迟分布:")
    print(f"  P50:  {p50:.2f} 微秒")
    print(f"  P99:  {p99:.2f} 微秒")
    print(f"  P999: {p999:.2f} 微秒")

    time.sleep(0.3)

    stats = agent.get_stats()
    print(f"\n缓冲区状态: size={stats['buffer']['size']:,}, "
          f"溢出={stats['buffer']['overflow_count']:,}")
    print(f"已上报: {stats['reporter']['reported_count']:,} 条")

    agent.stop()
    print()


def benchmark_ring_buffer_only():
    """纯环形缓冲区写入性能（无上报线程）。"""
    print("=" * 60)
    print("纯环形缓冲区写入性能（无上报开销）")
    print("=" * 60)

    from ring_buffer import RingBuffer

    buf = RingBuffer(capacity=500000, overflow_strategy="drop_oldest")

    n = 500000
    latencies = []

    start = time.perf_counter()
    for i in range(n):
        t0 = time.perf_counter()
        buf.put(f"log-{i}")
        t1 = time.perf_counter()
        latencies.append(t1 - t0)
    end = time.perf_counter()

    elapsed = end - start
    qps = n / elapsed

    latencies.sort()
    p50 = latencies[len(latencies) // 2] * 1_000_000
    p99 = latencies[int(len(latencies) * 0.99)] * 1_000_000
    p999 = latencies[int(len(latencies) * 0.999)] * 1_000_000

    print(f"写入 {n:,} 条，耗时 {elapsed:.3f} 秒")
    print(f"吞吐率: {qps:,.0f} 条/秒")
    print(f"平均: {elapsed / n * 1_000_000:.2f} 微秒/条")
    print(f"延迟分布:")
    print(f"  P50:  {p50:.2f} 微秒")
    print(f"  P99:  {p99:.2f} 微秒")
    print(f"  P999: {p999:.2f} 微秒")
    print(f"缓冲区大小: {len(buf):,}")
    print()


if __name__ == "__main__":
    benchmark_ring_buffer_only()
    benchmark_single_thread()
    benchmark_multi_thread(num_threads=8)

    print("=" * 60)
    print("性能测试完成！")
    print("=" * 60)
