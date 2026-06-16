import threading
import time
import logging
from typing import Optional

from ring_buffer import RingBuffer
from config import LogAgentConfig, LogEntry
from reporter import ReporterWorker, HttpLogReporter, ConsoleLogReporter, LogReporter
from crash_protector import CrashProtector

logger = logging.getLogger(__name__)


class LogAgent:
    """
    高吞吐日志收集 Agent。

    核心特性：
    1. 异步写入：业务线程写入日志仅需将日志放入环形缓冲区，几乎零阻塞
    2. 环形缓冲区：多生产者单消费者设计，支持高并发写入
    3. 满溢策略可配置：丢弃最老 / 丢弃最新 / 阻塞
    4. 崩溃保护：进程异常退出时缓冲区日志自动落盘
    5. 批量上报：减少网络 IO 次数，提高吞吐

    典型用法：
        agent = LogAgent(config)
        agent.start()

        # 业务代码中写日志
        agent.info("hello world")

        # 程序结束时优雅关闭
        agent.stop()
    """

    def __init__(self, config: Optional[LogAgentConfig] = None):
        self._config = config or LogAgentConfig()

        self._buffer = RingBuffer(
            capacity=self._config.buffer_capacity,
            overflow_strategy=self._config.overflow_strategy
        )

        self._reporter = self._create_reporter()
        self._reporter_worker = ReporterWorker(self._buffer, self._reporter, self._config)

        self._crash_protector = CrashProtector(dump_dir=self._config.crash_dump_dir)

        self._started = False
        self._start_lock = threading.Lock()

        self._write_count = 0
        self._stats_lock = threading.Lock()

    def _create_reporter(self) -> LogReporter:
        reporter_type = self._config.reporter_type.lower()
        if reporter_type == "console":
            return ConsoleLogReporter()
        elif reporter_type == "http":
            return HttpLogReporter(self._config)
        else:
            raise ValueError(f"unknown reporter type: {reporter_type}")

    def start(self):
        """启动 Agent，开始后台上报。"""
        with self._start_lock:
            if self._started:
                return

            if self._config.crash_dump_enabled:
                recovered = self._crash_protector.recover_latest_dump()
                if recovered:
                    for entry in recovered:
                        self._buffer.put(entry, block=False)
                    logger.info(f"recovered {len(recovered)} logs from crash dump")

                self._crash_protector.register(self._buffer, self._reporter_worker)

            self._reporter_worker.start()
            self._started = True
            logger.info("log agent started")

    def stop(self, timeout: float = 5.0):
        """优雅停止 Agent，等待所有日志上报完毕。"""
        with self._start_lock:
            if not self._started:
                return
            self._started = False

        if self._config.crash_dump_enabled:
            self._crash_protector.unregister()

        self._reporter_worker.stop(timeout=timeout)
        self._reporter.close()
        logger.info("log agent stopped")

    def log(self, level: str, message: str, trace_id: Optional[str] = None,
            extra: Optional[dict] = None, service: Optional[str] = None,
            block: bool = False, timeout: Optional[float] = None) -> str:
        """
        写一条日志。

        这是业务线程调用的主要接口。
        默认非阻塞模式：仅将日志放入环形缓冲区，耗时极短，几乎不阻塞业务线程。
        阻塞模式：缓冲区满时会等待空位，直到超时（仅 BLOCK 策略有效）。

        Args:
            level: 日志级别，如 DEBUG/INFO/WARN/ERROR
            message: 日志内容
            trace_id: 追踪 ID
            extra: 额外字段
            service: 服务名
            block: 是否阻塞等待空位（仅 BLOCK 策略有效）
                   DROP_OLDEST / DROP_NEWEST 策略下，此参数无效，总是立即返回
            timeout: 阻塞超时时间（秒），None 表示一直等

        Returns:
            "success" - 写入成功
            "dropped" - 被丢弃（缓冲区满）
            "timeout" - 阻塞等待超时（仅 BLOCK + block=True 时可能返回）
        """
        entry = LogEntry(
            level=level,
            message=message,
            trace_id=trace_id,
            extra=extra or {},
            service=service or self._config.service if hasattr(self._config, 'service') else "default",
        )

        result = self._buffer.put(entry, block=block, timeout=timeout)

        if result == RingBuffer.PUT_SUCCESS:
            with self._stats_lock:
                self._write_count += 1

        return result

    def debug(self, message: str, **kwargs):
        return self.log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs):
        return self.log("INFO", message, **kwargs)

    def warn(self, message: str, **kwargs):
        return self.log("WARN", message, **kwargs)

    def warning(self, message: str, **kwargs):
        return self.log("WARN", message, **kwargs)

    def error(self, message: str, **kwargs):
        return self.log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs):
        return self.log("CRITICAL", message, **kwargs)

    def set_overflow_strategy(self, strategy: str):
        """动态设置满溢策略。"""
        self._buffer.overflow_strategy = strategy

    def flush(self):
        """触发立即上报。"""
        self._reporter_worker.flush()

    def crash_dump(self) -> int:
        """手动触发崩溃转储（用于测试）。"""
        return self._crash_protector.dump_to_disk()

    def get_stats(self) -> dict:
        """获取运行统计。"""
        with self._stats_lock:
            write_count = self._write_count

        buffer_stats = self._buffer.get_stats()
        reporter_stats = self._reporter_worker.get_stats()

        return {
            "write_count": write_count,
            "buffer": buffer_stats,
            "reporter": reporter_stats,
        }

    @property
    def buffer(self) -> RingBuffer:
        return self._buffer

    @property
    def reporter(self):
        return self._reporter

    @property
    def crash_protector(self) -> CrashProtector:
        return self._crash_protector
