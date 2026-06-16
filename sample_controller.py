"""
动态日志级别和采样控制器。

功能：
1. 全局最小日志级别（低于此级别的日志直接丢弃）
2. 全局采样率（0.0~1.0，1.0 表示全采样）
3. 按服务名覆盖采样率
4. 按 trace_id 覆盖采样率（特定请求全量采样）
5. 按关键词提高采样率

所有规则可动态调整，新写入的日志立即按新规则生效。
"""

import threading
import re
import random
from typing import Optional, Dict, Tuple

from config import LogEntry


_LEVEL_RANK = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
    "FATAL": 50,
}


class SampleController:
    """
    动态日志级别和采样控制器。

    判断逻辑（任一条件命中即保留）：
    1. 首先检查级别：低于全局最小级别的直接丢弃
    2. 然后检查规则（优先级从高到低）：
       a. trace_id 在白名单中 → 全保留
       b. 关键词命中 → 用关键词采样率
       c. 服务名匹配 → 用服务采样率
       d. 否则用全局采样率
    """

    def __init__(self, min_level: str = "DEBUG", global_sample_rate: float = 1.0):
        self._lock = threading.RLock()

        self._min_level = min_level.upper()
        self._global_sample_rate = self._validate_rate(global_sample_rate)

        self._service_rates: Dict[str, float] = {}
        self._trace_whitelist: set = set()
        self._keyword_rules: list = []

        self._sampled_count = 0
        self._dropped_by_level = 0
        self._dropped_by_sample = 0

    @staticmethod
    def _validate_rate(rate: float) -> float:
        if not (0.0 <= rate <= 1.0):
            raise ValueError(f"sample rate must be in [0.0, 1.0], got {rate}")
        return rate

    @staticmethod
    def _level_value(level: str) -> int:
        return _LEVEL_RANK.get(level.upper(), 0)

    def set_min_level(self, level: str):
        """动态设置全局最小日志级别。"""
        level = level.upper()
        if level not in _LEVEL_RANK:
            raise ValueError(f"invalid level: {level}, valid: {list(_LEVEL_RANK.keys())}")
        with self._lock:
            self._min_level = level

    def get_min_level(self) -> str:
        with self._lock:
            return self._min_level

    def set_global_sample_rate(self, rate: float):
        """动态设置全局采样率。"""
        rate = self._validate_rate(rate)
        with self._lock:
            self._global_sample_rate = rate

    def get_global_sample_rate(self) -> float:
        with self._lock:
            return self._global_sample_rate

    def set_service_sample_rate(self, service: str, rate: float):
        """设置特定服务的采样率，None 表示移除规则。"""
        rate = self._validate_rate(rate)
        with self._lock:
            self._service_rates[service] = rate

    def remove_service_sample_rate(self, service: str):
        with self._lock:
            self._service_rates.pop(service, None)

    def add_trace_whitelist(self, trace_id: str):
        """添加 trace_id 到白名单，这些 trace 的日志全保留。"""
        with self._lock:
            self._trace_whitelist.add(trace_id)

    def remove_trace_whitelist(self, trace_id: str):
        with self._lock:
            self._trace_whitelist.discard(trace_id)

    def clear_trace_whitelist(self):
        with self._lock:
            self._trace_whitelist.clear()

    def add_keyword_rule(self, keyword: str, rate: float, case_sensitive: bool = False):
        """
        添加关键词采样规则。

        匹配到关键词的日志使用指定的采样率（通常高于全局）。
        """
        rate = self._validate_rate(rate)
        with self._lock:
            for rule in self._keyword_rules:
                if rule["keyword"] == keyword and rule["case_sensitive"] == case_sensitive:
                    rule["rate"] = rate
                    return
            self._keyword_rules.append({
                "keyword": keyword,
                "rate": rate,
                "case_sensitive": case_sensitive,
                "pattern": re.compile(re.escape(keyword), 0 if case_sensitive else re.IGNORECASE),
            })

    def remove_keyword_rule(self, keyword: str):
        with self._lock:
            self._keyword_rules = [r for r in self._keyword_rules if r["keyword"] != keyword]

    def should_keep(self, entry: LogEntry) -> Tuple[bool, str]:
        """
        判断一条日志是否应该保留。

        Returns:
            (keep, reason)
            reason 用于调试："level" / "trace_whitelist" / "keyword" / "service" / "global"
        """
        with self._lock:
            if self._level_value(entry.level) < self._level_value(self._min_level):
                self._dropped_by_level += 1
                return False, "level"

            if entry.trace_id and entry.trace_id in self._trace_whitelist:
                self._sampled_count += 1
                return True, "trace_whitelist"

            for rule in self._keyword_rules:
                if rule["pattern"].search(entry.message):
                    if random.random() < rule["rate"]:
                        self._sampled_count += 1
                        return True, "keyword"
                    else:
                        self._dropped_by_sample += 1
                        return False, "keyword"

            if entry.service and entry.service in self._service_rates:
                rate = self._service_rates[entry.service]
                if random.random() < rate:
                    self._sampled_count += 1
                    return True, "service"
                else:
                    self._dropped_by_sample += 1
                    return False, "service"

            if random.random() < self._global_sample_rate:
                self._sampled_count += 1
                return True, "global"
            else:
                self._dropped_by_sample += 1
                return False, "global"

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "min_level": self._min_level,
                "global_sample_rate": self._global_sample_rate,
                "service_rules": dict(self._service_rates),
                "trace_whitelist_count": len(self._trace_whitelist),
                "keyword_rules": [
                    {"keyword": r["keyword"], "rate": r["rate"], "case_sensitive": r["case_sensitive"]}
                    for r in self._keyword_rules
                ],
                "sampled_count": self._sampled_count,
                "dropped_by_level": self._dropped_by_level,
                "dropped_by_sample": self._dropped_by_sample,
            }

    def reset_stats(self):
        with self._lock:
            self._sampled_count = 0
            self._dropped_by_level = 0
            self._dropped_by_sample = 0
