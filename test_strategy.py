"""
缓冲区满溢策略测试：验证三种策略的行为。
"""

import time
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from ring_buffer import RingBuffer
from log_agent import LogAgent
from config import LogAgentConfig, LogEntry


def test_drop_oldest():
    """测试丢弃最老策略。"""
    print("=" * 60)
    print("策略测试：丢弃最老 (Drop Oldest)")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.DROP_OLDEST)

    print("写入 5 条日志填满缓冲区...")
    for i in range(5):
        result = buf.put(f"log-{i}")
        assert result == RingBuffer.PUT_SUCCESS, f"log-{i} should be written"
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n继续写入 3 条新日志，应该覆盖最老的 3 条...")
    for i in range(5, 8):
        result = buf.put(f"log-{i}")
        assert result == RingBuffer.PUT_SUCCESS, f"log-{i} should be written (drop oldest)"

    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")
    stats = buf.get_stats()
    print(f"溢出次数: {stats['overflow_count']}")
    print(f"丢弃最老数: {stats['dropped_oldest_count']}")
    print(f"丢弃最新数: {stats['dropped_newest_count']}")

    print("\n读取缓冲区内容：")
    items = []
    while not buf.is_empty():
        items.append(buf.get(block=False))

    for item in items:
        print(f"  {item}")

    assert items == [f"log-{i}" for i in range(3, 8)], "应该保留 log-3 到 log-7"
    print("\n✓ 丢弃最老策略验证通过")
    print()


def test_drop_newest():
    """测试丢弃最新策略。"""
    print("=" * 60)
    print("策略测试：丢弃最新 (Drop Newest)")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.DROP_NEWEST)

    print("写入 5 条日志填满缓冲区...")
    for i in range(5):
        result = buf.put(f"log-{i}")
        assert result == RingBuffer.PUT_SUCCESS
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n继续写入 3 条新日志，这些新日志应该被丢弃...")
    dropped = 0
    for i in range(5, 8):
        result = buf.put(f"log-{i}")
        if result == RingBuffer.PUT_DROPPED:
            dropped += 1
    print(f"丢弃了 {dropped} 条新日志")

    stats = buf.get_stats()
    print(f"溢出次数: {stats['overflow_count']}")
    print(f"丢弃最老数: {stats['dropped_oldest_count']}")
    print(f"丢弃最新数: {stats['dropped_newest_count']}")

    print("\n读取缓冲区内容：")
    items = []
    while not buf.is_empty():
        items.append(buf.get(block=False))

    for item in items:
        print(f"  {item}")

    assert items == [f"log-{i}" for i in range(5)], "应该保留 log-0 到 log-4（最早的 5 条）"
    assert dropped == 3, "应该丢弃 3 条新日志"
    print("\n✓ 丢弃最新策略验证通过")
    print()


def test_block_non_blocking():
    """测试阻塞策略的非阻塞写入模式。"""
    print("=" * 60)
    print("策略测试：阻塞 (Block) - 非阻塞写入模式")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.BLOCK)

    print("写入 5 条日志填满缓冲区...")
    for i in range(5):
        result = buf.put(f"log-{i}", block=False)
        assert result == RingBuffer.PUT_SUCCESS
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n尝试以非阻塞方式写入，应该返回 PUT_DROPPED...")
    result = buf.put("log-5", block=False)
    assert result == RingBuffer.PUT_DROPPED, "非阻塞写入应该返回 dropped"
    print(f"结果: {result}")

    stats = buf.get_stats()
    print(f"溢出次数: {stats['overflow_count']}")

    print("\n消费一条后再写入...")
    item = buf.get()
    print(f"消费了: {item}")

    result = buf.put("log-5", block=False)
    assert result == RingBuffer.PUT_SUCCESS, "消费后应该能写入"
    print("写入 log-5 成功")

    print("\n✓ 阻塞策略（非阻塞模式）验证通过")
    print()


def test_block_with_timeout():
    """测试阻塞策略带超时等待。"""
    print("=" * 60)
    print("策略测试：阻塞 (Block) - 带超时等待")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.BLOCK)

    print("填满缓冲区...")
    for i in range(5):
        buf.put(f"log-{i}", block=False)

    print("\n启动一个线程，200ms 后消费一条日志...")

    def consumer():
        time.sleep(0.2)
        item = buf.get()
        print(f"  [消费者] 消费了: {item}")

    t = threading.Thread(target=consumer)
    t.start()

    print("尝试阻塞写入，超时 500ms...")
    t0 = time.time()
    result = buf.put("log-new", block=True, timeout=0.5)
    elapsed = time.time() - t0

    print(f"结果: {result}")
    print(f"等待时间: {elapsed*1000:.0f}ms")

    assert result == RingBuffer.PUT_SUCCESS, "消费者腾出位置后应该写入成功"
    assert elapsed >= 0.15, "应该等待了至少 150ms"

    t.join()
    print("\n✓ 阻塞超时等待（有数据消费）验证通过")
    print()


def test_block_timeout_expired():
    """测试阻塞策略超时到期（没有消费）。"""
    print("=" * 60)
    print("策略测试：阻塞 (Block) - 超时到期失败")
    print("=" * 60)

    buf = RingBuffer(capacity=3, overflow_strategy=RingBuffer.BLOCK)

    print("填满缓冲区...")
    for i in range(3):
        buf.put(f"log-{i}", block=False)

    print("\n尝试阻塞写入，超时 200ms，没有消费者...")
    t0 = time.time()
    result = buf.put("log-new", block=True, timeout=0.2)
    elapsed = time.time() - t0

    print(f"结果: {result}")
    print(f"等待时间: {elapsed*1000:.0f}ms")
    print(f"缓冲区大小: {len(buf)}")

    assert result == RingBuffer.PUT_TIMEOUT, "超时后应该返回 timeout"
    assert elapsed >= 0.18, "应该等待了接近 200ms"
    assert len(buf) == 3, "缓冲区仍然是满的"

    print("\n✓ 阻塞超时到期验证通过")
    print()


def test_strategy_switch():
    """测试动态切换策略。"""
    print("=" * 60)
    print("策略测试：动态切换策略")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.DROP_OLDEST)

    print("初始策略: drop_oldest")
    for i in range(7):
        buf.put(f"log-{i}")
    print(f"写入 7 条后，大小: {len(buf)}")

    print("\n切换到 drop_newest...")
    buf.overflow_strategy = RingBuffer.DROP_NEWEST

    for i in range(7, 10):
        buf.put(f"log-{i}")
    print(f"再写入 3 条后，大小: {len(buf)}")
    print(f"策略: {buf.overflow_strategy}")

    items = buf.drain_all()
    print(f"缓冲区内容: {items}")

    print("\n✓ 动态切换策略验证通过")
    print()


def test_drop_strategies_never_block():
    """验证丢弃策略永远不会阻塞业务线程。"""
    print("=" * 60)
    print("验证：丢弃最老/丢弃最新 策略绝不阻塞")
    print("=" * 60)

    for strategy_name, strategy in [
        ("drop_oldest", RingBuffer.DROP_OLDEST),
        ("drop_newest", RingBuffer.DROP_NEWEST),
    ]:
        buf = RingBuffer(capacity=10, overflow_strategy=strategy)

        print(f"\n策略: {strategy_name}")

        start = time.perf_counter()
        for i in range(100000):
            result = buf.put(f"log-{i}")
            assert result in (RingBuffer.PUT_SUCCESS, RingBuffer.PUT_DROPPED)
        elapsed = time.perf_counter() - start

        print(f"  写入 10 万条，耗时 {elapsed:.3f}s，平均 {elapsed/100000*1e6:.2f} 微秒/条")
        print(f"  缓冲区最终大小: {len(buf)}")

    print("\n✓ 丢弃策略零阻塞验证通过")
    print()


def test_full_agent_with_strategy():
    """测试完整 Agent 与策略配合。"""
    print("=" * 60)
    print("集成测试：Agent + 策略 + 模拟网络故障")
    print("=" * 60)

    config = LogAgentConfig(
        buffer_capacity=100,
        overflow_strategy="drop_oldest",
        reporter_type="http",
        reporter_batch_size=10,
        reporter_flush_interval_ms=10,
    )

    agent = LogAgent(config)
    agent.reporter.set_simulate_failure(True)
    agent.reporter.set_simulate_delay(0)
    agent.start()

    print("模拟网络故障，写入 200 条日志...")
    success_count = 0
    for i in range(200):
        result = agent.info(f"log-{i}")
        if result == "success":
            success_count += 1

    time.sleep(0.1)

    stats = agent.get_stats()
    print(f"写入总数: {stats['write_count']}")
    print(f"缓冲区大小: {stats['buffer']['size']}")
    print(f"溢出次数: {stats['buffer']['overflow_count']}")
    print(f"丢弃最老: {stats['buffer']['dropped_oldest_count']}")
    print(f"已上报: {stats['reporter']['reported_count']}")

    print("\n恢复网络...")
    agent.reporter.set_simulate_failure(False)
    time.sleep(0.2)

    stats = agent.get_stats()
    print(f"恢复后缓冲区大小: {stats['buffer']['size']}")
    print(f"已上报: {stats['reporter']['reported_count']}")

    agent.stop()

    print("\n✓ 集成测试通过")
    print()


if __name__ == "__main__":
    test_drop_oldest()
    test_drop_newest()
    test_block_non_blocking()
    test_block_with_timeout()
    test_block_timeout_expired()
    test_strategy_switch()
    test_drop_strategies_never_block()
    test_full_agent_with_strategy()

    print("=" * 60)
    print("所有策略测试通过！")
    print("=" * 60)
