"""
崩溃保护测试：验证进程异常退出时缓冲区日志落盘。
"""

import os
import sys
import time
import json
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(__file__))

from log_agent import LogAgent
from config import LogAgentConfig
from crash_protector import CrashProtector
from ring_buffer import RingBuffer


def test_crash_dump_basic():
    """测试手动触发崩溃转储。"""
    print("=" * 60)
    print("崩溃保护测试：手动触发落盘")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_test_")
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

        print("写入 100 条日志（网络故障，全部停留在缓冲区）...")
        for i in range(100):
            agent.info(f"important log #{i}", extra={"seq": i})

        time.sleep(0.05)
        print(f"缓冲区大小: {len(agent.buffer)}")

        print("\n触发崩溃转储...")
        dumped = agent.crash_dump()
        print(f"落盘日志数: {dumped}")

        assert dumped > 0, "应该有日志落盘"

        dump_files = agent.crash_protector.list_dump_files()
        print(f"转储文件数: {len(dump_files)}")
        assert len(dump_files) >= 1

        latest_file = os.path.join(tmp_dir, dump_files[0])
        print(f"转储文件: {latest_file}")

        with open(latest_file, "r", encoding="utf-8") as f:
            lines = [line for line in f if line.strip()]
        print(f"文件中的日志行数: {len(lines)}")

        first = json.loads(lines[0])
        last = json.loads(lines[-1])
        print(f"第一条日志: {first['message']}")
        print(f"最后一条日志: {last['message']}")

        assert len(lines) == dumped, "文件行数应等于落盘数"

        agent.stop()
        print("\n✓ 手动崩溃转储测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_recover_from_crash():
    """测试从崩溃转储中恢复日志。"""
    print("=" * 60)
    print("崩溃保护测试：崩溃后恢复")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_recover_")
    try:
        config1 = LogAgentConfig(
            buffer_capacity=1000,
            reporter_type="http",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
        )

        agent1 = LogAgent(config1)
        agent1.reporter.set_simulate_failure(True)
        agent1.start()

        print("【进程 A】写入 50 条日志...")
        for i in range(50):
            agent1.info(f"crash-scene-log-{i}", extra={"seq": i})

        time.sleep(0.05)
        print(f"【进程 A】缓冲区大小: {len(agent1.buffer)}")

        print("【进程 A】模拟崩溃，日志落盘...")
        dumped = agent1.crash_dump()
        print(f"【进程 A】落盘: {dumped} 条")

        agent1.stop()

        print("\n【进程 B】启动，从崩溃转储恢复...")
        config2 = LogAgentConfig(
            buffer_capacity=1000,
            reporter_type="http",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
        )
        agent2 = LogAgent(config2)
        agent2.reporter.set_simulate_failure(False)
        agent2.start()

        time.sleep(0.2)

        stats = agent2.get_stats()
        print(f"【进程 B】已上报: {stats['reporter']['reported_count']} 条")

        assert stats['reporter']['reported_count'] >= dumped - 10, \
            "恢复的日志应该被上报"

        agent2.stop()
        print("\n✓ 崩溃恢复测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_empty_buffer_dump():
    """测试空缓冲区的崩溃转储。"""
    print("=" * 60)
    print("崩溃保护测试：空缓冲区转储")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_empty_")
    try:
        protector = CrashProtector(dump_dir=tmp_dir)

        buf = RingBuffer(capacity=100)
        protector.register(buf)

        print("缓冲区为空时触发转储...")
        dumped = protector.dump_to_disk()
        print(f"落盘数量: {dumped}")

        assert dumped == 0, "空缓冲区不应该有落盘"

        files = protector.list_dump_files()
        print(f"转储文件数: {len(files)}")

        print("\n✓ 空缓冲区转储测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_concurrent_dump():
    """测试并发触发崩溃转储（应只执行一次）。"""
    print("=" * 60)
    print("崩溃保护测试：并发转储去重")
    print("=" * 60)

    import threading

    tmp_dir = tempfile.mkdtemp(prefix="crash_concurrent_")
    try:
        protector = CrashProtector(dump_dir=tmp_dir)
        buf = RingBuffer(capacity=1000)
        protector.register(buf)

        for i in range(100):
            buf.put(f"log-{i}")

        print("并发触发 10 次转储...")
        results = []

        def do_dump():
            results.append(protector.dump_to_disk())

        threads = []
        for _ in range(10):
            t = threading.Thread(target=do_dump)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print(f"各线程返回: {results}")
        non_zero = [r for r in results if r > 0]
        print(f"非零结果数: {len(non_zero)}")

        assert len(non_zero) == 1, "只有一次转储应该真正执行"

        files = protector.list_dump_files()
        print(f"转储文件数: {len(files)}")
        assert len(files) == 1, "应该只生成一个转储文件"

        print("\n✓ 并发转储去重测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    test_crash_dump_basic()
    test_recover_from_crash()
    test_empty_buffer_dump()
    test_concurrent_dump()

    print("=" * 60)
    print("所有崩溃保护测试通过！")
    print("=" * 60)
