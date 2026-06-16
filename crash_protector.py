import os
import sys
import json
import atexit
import signal
import time
import logging
import threading
from typing import List, Optional

from config import LogEntry

logger = logging.getLogger(__name__)


class CrashProtector:
    """
    崩溃保护器。

    职责：
    1. 注册信号处理函数（SIGINT、SIGTERM、SIGSEGV 等）
    2. 注册 atexit 钩子
    3. 进程异常退出时，将缓冲区中的日志落盘
    4. 下次启动时可从落盘文件恢复未上报的日志

    实现思路：
    - 使用多道防线：信号处理 + atexit + try/finally
    - 落盘文件使用 JSON Lines 格式，每行一条日志
    - 文件名包含时间戳和 PID，便于区分不同崩溃实例
    - 使用单独的写文件逻辑，尽量减少依赖，确保崩溃时也能执行
    """

    def __init__(self, dump_dir: str = "./crash_logs"):
        self._dump_dir = dump_dir
        self._dump_file: Optional[str] = None
        self._buffer_ref = None
        self._reporter_worker_ref = None
        self._registered = False
        self._dumping = False
        self._dump_lock = threading.Lock()

        self._ensure_dump_dir()

    def _ensure_dump_dir(self):
        try:
            os.makedirs(self._dump_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"failed to create crash dump dir: {e}")

    def register(self, ring_buffer, reporter_worker=None):
        """
        注册崩溃保护。

        Args:
            ring_buffer: 环形缓冲区引用
            reporter_worker: 上报工作线程引用（可选，用于优雅关闭）
        """
        if self._registered:
            return

        self._buffer_ref = ring_buffer
        self._reporter_worker_ref = reporter_worker

        self._register_signals()
        atexit.register(self._on_exit)

        self._registered = True
        logger.info("crash protector registered")

    def _register_signals(self):
        signals = [
            signal.SIGINT,
            signal.SIGTERM,
        ]

        if hasattr(signal, "SIGSEGV"):
            signals.append(signal.SIGSEGV)
        if hasattr(signal, "SIGABRT"):
            signals.append(signal.SIGABRT)
        if hasattr(signal, "SIGFPE"):
            signals.append(signal.SIGFPE)
        if hasattr(signal, "SIGILL"):
            signals.append(signal.SIGILL)

        for sig in signals:
            try:
                signal.signal(sig, self._signal_handler)
            except (ValueError, OSError):
                pass

    def _signal_handler(self, signum, frame):
        logger.warning(f"received signal {signum}, triggering crash dump")
        self.dump_to_disk()

        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    def _on_exit(self):
        logger.info("process exiting, dumping remaining logs")
        self.dump_to_disk()

    def dump_to_disk(self) -> int:
        """
        将缓冲区中的日志落盘。

        线程安全，可重入（确保只执行一次真正的落盘）。

        Returns:
            落盘的日志条数
        """
        with self._dump_lock:
            if self._dumping:
                return 0
            self._dumping = True

        try:
            entries = []

            if self._reporter_worker_ref:
                try:
                    entries = self._reporter_worker_ref.drain_all_pending()
                except Exception as e:
                    logger.error(f"drain from reporter worker failed: {e}")

            if not entries and self._buffer_ref:
                try:
                    entries = self._buffer_ref.drain_all()
                except Exception as e:
                    logger.error(f"drain buffer failed: {e}")

            if not entries:
                return 0

            dump_file = self._get_dump_file_path()
            count = self._write_entries(dump_file, entries)
            logger.warning(f"crash dump: {count} logs written to {dump_file}")
            return count

        except Exception as e:
            try:
                sys.stderr.write(f"crash dump error: {e}\n")
            except Exception:
                pass
            return 0
        finally:
            self._dumping = False

    def _get_dump_file_path(self) -> str:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        pid = os.getpid()
        filename = f"crash_logs_{timestamp}_{pid}.jsonl"
        return os.path.join(self._dump_dir, filename)

    def _write_entries(self, filepath: str, entries: list) -> int:
        count = 0
        try:
            with open(filepath, "a", encoding="utf-8") as f:
                for entry in entries:
                    try:
                        if hasattr(entry, "to_dict"):
                            data = entry.to_dict()
                        elif isinstance(entry, dict):
                            data = entry
                        else:
                            data = {"message": str(entry)}
                        line = json.dumps(data, ensure_ascii=False)
                        f.write(line + "\n")
                        count += 1
                    except Exception:
                        continue
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            try:
                sys.stderr.write(f"write crash dump error: {e}\n")
            except Exception:
                pass
        return count

    def recover_latest_dump(self) -> List[LogEntry]:
        """
        从最近的崩溃转储文件中恢复日志。

        Returns:
            恢复的日志条目列表
        """
        try:
            if not os.path.isdir(self._dump_dir):
                return []

            dump_files = [
                f for f in os.listdir(self._dump_dir)
                if f.startswith("crash_logs_") and f.endswith(".jsonl")
            ]

            if not dump_files:
                return []

            dump_files.sort(reverse=True)
            latest_file = os.path.join(self._dump_dir, dump_files[0])

            entries = []
            with open(latest_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        entry = LogEntry(
                            level=data.get("level", "INFO"),
                            message=data.get("message", ""),
                            timestamp=data.get("timestamp", time.time()),
                            service=data.get("service", "default"),
                            trace_id=data.get("trace_id"),
                            extra=data.get("extra", {}),
                        )
                        entries.append(entry)
                    except Exception:
                        continue

            if entries:
                logger.info(f"recovered {len(entries)} logs from {latest_file}")

            return entries

        except Exception as e:
            logger.error(f"recover crash dump failed: {e}")
            return []

    def list_dump_files(self) -> List[str]:
        """列出所有崩溃转储文件。"""
        try:
            if not os.path.isdir(self._dump_dir):
                return []
            files = [
                f for f in os.listdir(self._dump_dir)
                if f.startswith("crash_logs_") and f.endswith(".jsonl")
            ]
            return sorted(files, reverse=True)
        except Exception:
            return []

    def cleanup_old_dumps(self, keep_days: int = 7):
        """清理旧的崩溃转储文件。"""
        try:
            if not os.path.isdir(self._dump_dir):
                return
            cutoff = time.time() - keep_days * 86400
            for filename in os.listdir(self._dump_dir):
                if not (filename.startswith("crash_logs_") and filename.endswith(".jsonl")):
                    continue
                filepath = os.path.join(self._dump_dir, filename)
                try:
                    mtime = os.path.getmtime(filepath)
                    if mtime < cutoff:
                        os.remove(filepath)
                except Exception:
                    continue
        except Exception:
            pass
