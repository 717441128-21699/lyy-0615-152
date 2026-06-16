"""
缓冲区满溢策略测试：验证三种策略的行为。
"""

import time
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
        success = buf.put(f"log-{i}")
        assert success, f"log-{i} should be written"
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n继续写入 3 条新日志，应该覆盖最老的 3 条...")
    for i in range(5, 8):
        success = buf.put(f"log-{i}")
        assert success, f"log-{i} should be written"

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
        success = buf.put(f"log-{i}")
        assert success
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n继续写入 3 条新日志，这些新日志应该被丢弃...")
    dropped = 0
    for i in range(5, 8):
        success = buf.put(f"log-{i}")
        if not success:
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


def test_block_strategy():
    """测试阻塞策略（非阻塞模式）。"""
    print("=" * 60)
    print("策略测试：阻塞 (Block) - 非阻塞写入模式")
    print("=" * 60)

    buf = RingBuffer(capacity=5, overflow_strategy=RingBuffer.BLOCK)

    print("写入 5 条日志填满缓冲区...")
    for i in range(5):
        success = buf.put(f"log-{i}", block=False)
        assert success
    print(f"缓冲区大小: {len(buf)} / {buf.capacity}")

    print("\n尝试以非阻塞方式写入，应该失败...")
    success = buf.put("log-5", block=False)
    assert not success, "非阻塞写入应该失败"

    stats = buf.get_stats()
    print(f"溢出次数: {stats['overflow_count']}")
    print(f"写入成功: {not success}")

    print("\n消费一条后再写入...")
    item = buf.get()
    print(f"消费了: {item}")

    success = buf.put("log-5", block=False)
    assert success, "消费后应该能写入"
    print("写入 log-5 成功")

    print("\n✓ 阻塞策略验证通过")
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
    for i in range(200):
        agent.info(f"log-{i}")

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
    test_block_strategy()
    test_strategy_switch()
    test_full_agent_with_strategy()

    print("=" * 60)
    print("所有策略测试通过！")
    print("=" * 60)
