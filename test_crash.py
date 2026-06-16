"""
崩溃保护测试：验证进程异常退出时缓冲区日志落盘，以及恢复后归档。
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
        agent.reporter.set_simulate_delay(0)
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
        agent1.reporter.set_simulate_delay(0)
        agent1.start()

        print("【进程 A】写入 50 条日志（模拟网络故障，日志留在缓冲区）...")
        for i in range(50):
            agent1.info(f"crash-scene-log-{i}", extra={"seq": i})

        time.sleep(0.1)
        print(f"【进程 A】缓冲区大小: {len(agent1.buffer)}")

        print("【进程 A】模拟崩溃，日志落盘...")
        dumped = agent1.crash_dump()
        print(f"【进程 A】落盘: {dumped} 条")

        agent1.stop()

        dump_files = [f for f in os.listdir(tmp_dir) if f.endswith('.jsonl')]
        print(f"转储文件数: {len(dump_files)}")

        assert dumped > 0, "应该有日志落盘"

        print("\n【进程 B】启动，从崩溃转储恢复，并用 console 上报...")
        config2 = LogAgentConfig(
            buffer_capacity=1000,
            reporter_type="console",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
            reporter_flush_interval_ms=10,
        )
        agent2 = LogAgent(config2)
        agent2.start()

        time.sleep(0.3)

        stats = agent2.get_stats()
        print(f"【进程 B】已上报: {stats['reporter']['reported_count']} 条")

        assert stats['reporter']['reported_count'] >= dumped - 10, \
            "恢复的日志应该被上报"

        remaining = agent2.crash_protector.list_dump_files()
        archived = agent2.crash_protector.list_archived_files()
        print(f"【进程 B】未归档文件: {len(remaining)}, 已归档: {len(archived)}")
        assert len(archived) >= 1, "恢复后应该有归档文件"
        assert len(remaining) == 0, "未归档目录应该空了"

        print("\n【进程 B】再启动一次（连续重启），应该不会重复恢复...")
        agent2.stop()

        config3 = LogAgentConfig(
            buffer_capacity=1000,
            reporter_type="console",
            crash_dump_dir=tmp_dir,
            crash_dump_enabled=True,
        )
        agent3 = LogAgent(config3)
        agent3.start()
        time.sleep(0.2)

        stats3 = agent3.get_stats()
        print(f"【进程 C】已上报: {stats3['reporter']['reported_count']} 条")
        assert stats3['reporter']['reported_count'] == 0, "第二次启动不应该再恢复到日志"

        agent3.stop()
        print("\n✓ 崩溃恢复测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_recover_then_archive():
    """测试恢复后自动归档，同一份文件只恢复一次。"""
    print("=" * 60)
    print("崩溃保护测试：恢复后自动归档")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_archive_")
    try:
        protector = CrashProtector(dump_dir=tmp_dir)
        buf = RingBuffer(capacity=1000)

        for i in range(30):
            buf.put(f"crash-log-{i}")

        protector.register(buf)

        print("生成崩溃转储文件...")
        dumped = protector.dump_to_disk()
        print(f"落盘: {dumped} 条")

        dump_files_before = protector.list_dump_files()
        archived_before = protector.list_archived_files()
        print(f"恢复前：未归档文件 {len(dump_files_before)} 个，归档文件 {len(archived_before)} 个")

        print("\n第一次恢复（应该成功，并自动归档）...")
        recovered1 = protector.recover_latest_dump(auto_archive=True)
        print(f"恢复: {len(recovered1)} 条")

        dump_files_after1 = protector.list_dump_files()
        archived_after1 = protector.list_archived_files()
        print(f"恢复后：未归档文件 {len(dump_files_after1)} 个，归档文件 {len(archived_after1)} 个")

        assert len(dump_files_after1) == 0, "未归档目录应该空了"
        assert len(archived_after1) == 1, "归档目录应该有 1 个文件"
        assert len(recovered1) == dumped, "恢复数量应等于落盘数量"

        print("\n第二次恢复（应该恢复 0 条，因为已归档）...")
        recovered2 = protector.recover_latest_dump(auto_archive=True)
        print(f"恢复: {len(recovered2)} 条")

        assert len(recovered2) == 0, "第二次恢复应该为 0 条"
        print("✓ 同一份转储文件只恢复了一次")

        archived_after2 = protector.list_archived_files()
        assert len(archived_after2) == 1, "归档文件数不应变化"

        print("\n✓ 恢复后自动归档测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_recover_without_archive():
    """测试不自动归档的模式。"""
    print("=" * 60)
    print("崩溃保护测试：不自动归档模式")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_noarch_")
    try:
        protector = CrashProtector(dump_dir=tmp_dir)
        buf = RingBuffer(capacity=1000)

        for i in range(20):
            buf.put(f"log-{i}")

        protector.register(buf)
        protector.dump_to_disk()

        print("第一次恢复（不归档）...")
        r1 = protector.recover_latest_dump(auto_archive=False)
        print(f"恢复: {len(r1)} 条")

        dump_files = protector.list_dump_files()
        print(f"未归档文件数: {len(dump_files)}")
        assert len(dump_files) == 1, "文件应该还在原位置"

        print("\n第二次恢复（仍能恢复到）...")
        r2 = protector.recover_latest_dump(auto_archive=False)
        print(f"恢复: {len(r2)} 条")
        assert len(r2) == len(r1), "不归档模式下每次都能恢复"

        print("\n✓ 不自动归档模式测试通过")
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

        from config import LogEntry
        for i in range(100):
            buf.put(LogEntry(level="INFO", message=f"log-{i}"))

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


def test_multiple_crashes_sequential_restart():
    """测试多次崩溃、多次重启，每次只恢复最新的未归档文件。"""
    print("=" * 60)
    print("崩溃保护测试：连续崩溃重启场景")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="crash_multi_")
    try:
        protector = CrashProtector(dump_dir=tmp_dir)

        print("手动生成 2 个转储文件（模拟 2 次崩溃）...")
        from config import LogEntry

        entries_a = [LogEntry(level="INFO", message=f"crash-A-{i}") for i in range(10)]
        entries_b = [LogEntry(level="INFO", message=f"crash-B-{i}") for i in range(20)]

        file_a = os.path.join(tmp_dir, "crash_logs_20250101_000001_11111.jsonl")
        file_b = os.path.join(tmp_dir, "crash_logs_20250101_000002_22222.jsonl")

        with open(file_a, "w", encoding="utf-8") as f:
            for e in entries_a:
                f.write(json.dumps(e.to_dict()) + "\n")

        with open(file_b, "w", encoding="utf-8") as f:
            for e in entries_b:
                f.write(json.dumps(e.to_dict()) + "\n")

        files = protector.list_dump_files()
        print(f"未归档文件数: {len(files)}")
        assert len(files) == 2, "应该有 2 个转储文件"
        print(f"文件列表: {files}")

        print("\n第 1 次恢复（应该只恢复最新的 B）...")
        recovered = protector.recover_latest_dump(auto_archive=True)
        print(f"恢复了 {len(recovered)} 条日志")

        first_msg = recovered[0].message if recovered else ""
        print(f"第一条内容: {first_msg}")

        assert len(recovered) == 20, "应该恢复最新的 20 条（B 批次）"
        assert "crash-B" in first_msg, "应该是 B 批次的日志"

        remaining = protector.list_dump_files()
        archived = protector.list_archived_files()
        print(f"剩余未归档: {len(remaining)} 个, 已归档: {len(archived)} 个")
        assert len(remaining) == 1, "还剩 1 个较早的转储"
        assert len(archived) == 1, "1 个已归档"

        print("\n第 2 次恢复（恢复剩下的 A）...")
        recovered2 = protector.recover_latest_dump(auto_archive=True)
        print(f"恢复了 {len(recovered2)} 条日志")

        if recovered2:
            print(f"第一条内容: {recovered2[0].message}")
            assert "crash-A" in recovered2[0].message

        remaining2 = protector.list_dump_files()
        archived2 = protector.list_archived_files()
        print(f"剩余未归档: {len(remaining2)} 个, 已归档: {len(archived2)} 个")
        assert len(remaining2) == 0, "应该全部处理完了"
        assert len(archived2) == 2, "2 个都已归档"

        print("\n第 3 次恢复（没有更多文件了）...")
        recovered3 = protector.recover_latest_dump(auto_archive=True)
        print(f"恢复了 {len(recovered3)} 条日志")
        assert len(recovered3) == 0, "没有未归档文件了"

        print("\n✓ 连续崩溃重启测试通过")
        print()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    test_crash_dump_basic()
    test_recover_from_crash()
    test_recover_then_archive()
    test_recover_without_archive()
    test_empty_buffer_dump()
    test_concurrent_dump()
    test_multiple_crashes_sequential_restart()

    print("=" * 60)
    print("所有崩溃保护测试通过！")
    print("=" * 60)
