from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class LogAgentConfig:
    """
    日志收集 Agent 配置。
    """

    buffer_capacity: int = 100000

    overflow_strategy: str = "drop_oldest"

    reporter_type: str = "http"

    reporter_endpoint: str = "http://localhost:8080/logs"

    reporter_batch_size: int = 100

    reporter_flush_interval_ms: int = 100

    reporter_max_retries: int = 3

    reporter_retry_backoff_ms: int = 1000

    crash_dump_dir: str = "./crash_logs"

    crash_dump_enabled: bool = True

    enable_metrics: bool = True

    metrics_report_interval_sec: int = 60


@dataclass
class LogEntry:
    """
    日志条目。
    """

    level: str
    message: str
    timestamp: float = field(default_factory=time.time)
    service: str = "default"
    trace_id: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "message": self.message,
            "timestamp": self.timestamp,
            "service": self.service,
            "trace_id": self.trace_id,
            "extra": self.extra,
        }
