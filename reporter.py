import threading
import time
import json
import logging
import urllib.request
import urllib.error
from typing import List, Optional
from abc import ABC, abstractmethod

from config import LogAgentConfig, LogEntry

logger = logging.getLogger(__name__)


class LogReporter(ABC):
    """
    日志上报器抽象基类。
    """

    @abstractmethod
    def report(self, entries: List[LogEntry]) -> bool:
        """
        上报一批日志。

        Returns:
            True 表示上报成功，False 表示上报失败
        """
        pass

    @abstractmethod
    def close(self):
        """关闭上报器，释放资源。"""
        pass


class HttpLogReporter(LogReporter):
    """
    HTTP 日志上报器。

    将批量日志以 JSON 数组形式 POST 到配置的 HTTP 端点。
    请求体格式：
    [
      {"level": "INFO", "message": "...", "timestamp": 1234567890, ...},
      ...
    ]

    支持真实网络故障模拟，便于测试。
    """

    def __init__(self, config: LogAgentConfig):
        self._endpoint = config.reporter_endpoint
        self._timeout_sec = 5.0
        self._max_retries = config.reporter_max_retries
        self._retry_backoff_ms = config.reporter_retry_backoff_ms

        self._simulate_failure = False
        self._simulate_delay_ms = 0

    def set_simulate_failure(self, fail: bool):
        """模拟网络故障，用于测试。"""
        self._simulate_failure = fail

    def set_simulate_delay(self, delay_ms: int):
        """模拟网络延迟。"""
        self._simulate_delay_ms = delay_ms

    def report(self, entries: List[LogEntry]) -> bool:
        """
        上报一批日志。

        真实发送 HTTP POST 请求，Content-Type: application/json。
        返回 True 表示上报成功，False 表示失败。
        """
        if self._simulate_delay_ms > 0:
            time.sleep(self._simulate_delay_ms / 1000.0)

        if self._simulate_failure:
            return False

        try:
            payload = json.dumps([e.to_dict() for e in entries]).encode("utf-8")

            req = urllib.request.Request(
                self._endpoint,
                data=payload,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "log-agent/1.0",
                },
            )

            with urllib.request.urlopen(req, timeout=self._timeout_sec) as resp:
                    status = resp.getcode()
                    if 200 <= status < 300:
                        return True
                    else:
                        logger.warning(f"http report got status {status}")
                        return False

        except urllib.error.HTTPError as e:
            logger.warning(f"http report HTTP error: {e.code} {e.reason}")
            return False
        except urllib.error.URLError as e:
            logger.warning(f"http report URL error: {e.reason}")
            return False
        except Exception as e:
            logger.error(f"http report unexpected error: {e}")
            return False

    def close(self):
        pass


class ConsoleLogReporter(LogReporter):
    """
    控制台上报器，用于调试。
    """

    def __init__(self):
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def report(self, entries: List[LogEntry]) -> bool:
        for entry in entries:
            print(f"[{entry.level}] {entry.message}")
            self._count += 1
        return True

    def close(self):
        pass


class ReporterWorker:
    """
    上报工作线程。

    负责从环形缓冲区批量消费日志，然后通过 reporter 上报。
    支持批量上报、失败重试、优雅关闭。

    崩溃保护设计：
    - 维护 _pending_entries 列表，跟踪正在上报/重试中的日志
    - 提供 drain_all_pending() 方法，崩溃时可获取所有待处理日志
    """

    def __init__(self, ring_buffer, reporter: LogReporter, config: LogAgentConfig):
        self._buffer = ring_buffer
        self._reporter = reporter
        self._config = config
        self._batch_size = config.reporter_batch_size
        self._flush_interval = config.reporter_flush_interval_ms / 1000.0

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._flush_event = threading.Event()

        self._reported_count = 0
        self._failed_count = 0
        self._retry_count = 0

        self._pending_entries: List[LogEntry] = []
        self._pending_lock = threading.Lock()

        self._stats_lock = threading.Lock()

    def start(self):
        """启动上报线程。"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="log-reporter")
        self._thread.start()

    def stop(self, timeout: float = 5.0):
        """停止上报线程，等待剩余日志处理完毕。"""
        self._stop_event.set()
        self._flush_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def flush(self):
        """触发立即上报。"""
        self._flush_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                entries = self._buffer.get_batch(
                    max_count=self._batch_size,
                    block=True,
                    timeout=self._flush_interval
                )

                if entries:
                    self._report_with_retry(entries)

                if self._flush_event.is_set():
                    self._flush_event.clear()

            except Exception as e:
                logger.error(f"reporter worker error: {e}")

        self._drain_and_report()

    def _report_with_retry(self, entries: List[LogEntry]):
        with self._pending_lock:
            self._pending_entries.extend(entries)

        try:
            retries = 0
            while True:
                success = self._reporter.report(entries)
                if success:
                    with self._stats_lock:
                        self._reported_count += len(entries)
                    return

                retries += 1
                with self._stats_lock:
                    self._retry_count += 1

                if retries >= self._config.reporter_max_retries:
                    with self._stats_lock:
                        self._failed_count += len(entries)
                    logger.warning(
                        f"failed to report {len(entries)} logs after {retries} retries, "
                        f"logs will remain in buffer if space allows"
                    )

                    for entry in entries:
                        self._buffer.put(entry, block=False)
                    return

                backoff = self._config.reporter_retry_backoff_ms / 1000.0 * retries
                time.sleep(backoff)
        finally:
            with self._pending_lock:
                    for entry in entries:
                        try:
                            self._pending_entries.remove(entry)
                        except ValueError:
                            pass

    def _drain_and_report(self):
        """排空缓冲区并尝试上报，用于关闭时。"""
        entries = self._buffer.drain_all()
        if entries:
            try:
                success = self._reporter.report(entries)
                if success:
                    with self._stats_lock:
                        self._reported_count += len(entries)
                else:
                    with self._stats_lock:
                        self._failed_count += len(entries)
            except Exception as e:
                logger.error(f"drain report error: {e}")
                with self._stats_lock:
                    self._failed_count += len(entries)

    def drain_all_pending(self) -> List[LogEntry]:
        """
        获取所有待处理的日志（缓冲区 + 正在上报中的）。

        崩溃保护调用，用于异常退出时落盘。
        注意：调用后这些日志就从系统中移除了。
        """
        all_entries = []

        with self._pending_lock:
            all_entries.extend(self._pending_entries)
            self._pending_entries = []

        buffer_entries = self._buffer.drain_all()
        all_entries.extend(buffer_entries)

        return all_entries

    def get_stats(self) -> dict:
        with self._stats_lock:
            return {
                "reported_count": self._reported_count,
                "failed_count": self._failed_count,
                "retry_count": self._retry_count,
            }
