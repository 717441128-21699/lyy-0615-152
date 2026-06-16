import threading
import time
from typing import Any, Optional, Tuple


class RingBufferFullError(Exception):
    pass


class RingBuffer:
    """
    多生产者单消费者（MPSC）环形缓冲区。

    设计要点：
    - 使用数组作为底层存储，读写指针循环移动
    - 生产者（业务线程）写入时仅持有锁极短时间（指针移动+数据拷贝）
    - 消费者（上报线程）批量读取，减少锁竞争
    - 支持三种满溢策略：丢弃最老、丢弃最新、阻塞

    put() 返回值说明：
    - PUT_SUCCESS: 写入成功
    - PUT_DROPPED: 因缓冲区满被丢弃（丢弃最新 / 非阻塞模式）
    - PUT_TIMEOUT: 阻塞等待超时，未能写入
    """

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"

    PUT_SUCCESS = "success"
    PUT_DROPPED = "dropped"
    PUT_TIMEOUT = "timeout"

    def __init__(self, capacity: int = 10000, overflow_strategy: str = DROP_OLDEST):
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        self._capacity = capacity
        self._buffer = [None] * capacity
        self._read_pos = 0
        self._write_pos = 0
        self._size = 0
        self._overflow_strategy = overflow_strategy

        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)

        self._overflow_count = 0
        self._dropped_oldest_count = 0
        self._dropped_newest_count = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def overflow_strategy(self) -> str:
        return self._overflow_strategy

    @overflow_strategy.setter
    def overflow_strategy(self, strategy: str):
        valid = {self.DROP_OLDEST, self.DROP_NEWEST, self.BLOCK}
        if strategy not in valid:
            raise ValueError(f"invalid strategy: {strategy}, must be one of {valid}")
        with self._lock:
            self._overflow_strategy = strategy

    def __len__(self) -> int:
        with self._lock:
            return self._size

    def is_empty(self) -> bool:
        with self._lock:
            return self._size == 0

    def is_full(self) -> bool:
        with self._lock:
            return self._size == self._capacity

    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None) -> str:
        """
        写入一条日志。

        Args:
            item: 日志条目
            block: 缓冲区满时是否阻塞等待（仅 BLOCK 策略有效）
                   DROP_OLDEST 和 DROP_NEWEST 策略下，此参数无效，
                   总是立即返回，不阻塞业务线程。
            timeout: 阻塞超时时间（秒），仅 BLOCK + block=True 时有效

        Returns:
            PUT_SUCCESS: 写入成功
            PUT_DROPPED: 因缓冲区满被丢弃
            PUT_TIMEOUT: 阻塞等待超时（仅 BLOCK 策略 + block=True 时可能返回）
        """
        with self._lock:
            if self._size < self._capacity:
                return self._do_put(item)

            strategy = self._overflow_strategy

            if strategy == self.DROP_NEWEST:
                self._overflow_count += 1
                self._dropped_newest_count += 1
                return self.PUT_DROPPED

            elif strategy == self.DROP_OLDEST:
                self._do_drop_oldest()
                return self._do_put(item)

            else:  # BLOCK
                if not block:
                    self._overflow_count += 1
                    return self.PUT_DROPPED

                deadline = None
                remaining = timeout
                if timeout is not None:
                    deadline = time.monotonic() + timeout

                while self._size >= self._capacity:
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._overflow_count += 1
                            return self.PUT_TIMEOUT

                    self._not_full.wait(timeout=remaining)

                return self._do_put(item)

    def _do_put(self, item: Any) -> str:
        self._buffer[self._write_pos] = item
        self._write_pos = (self._write_pos + 1) % self._capacity
        self._size += 1
        self._not_empty.notify()
        return self.PUT_SUCCESS

    def _do_drop_oldest(self):
        if self._size > 0:
            self._buffer[self._read_pos] = None
            self._read_pos = (self._read_pos + 1) % self._capacity
            self._size -= 1
            self._overflow_count += 1
            self._dropped_oldest_count += 1

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Any]:
        """
        读取一条日志（单消费者使用）。
        """
        with self._lock:
            if self._size == 0:
                if not block:
                    return None
                self._not_empty.wait(timeout=timeout)
                if self._size == 0:
                    return None

            item = self._buffer[self._read_pos]
            self._buffer[self._read_pos] = None
            self._read_pos = (self._read_pos + 1) % self._capacity
            self._size -= 1
            self._not_full.notify()
            return item

    def get_batch(self, max_count: int, block: bool = True,
                  timeout: Optional[float] = None) -> list:
        """
        批量读取日志，减少锁竞争。

        Args:
            max_count: 最多读取多少条
            block: 没有数据时是否阻塞
            timeout: 阻塞超时

        Returns:
            日志列表
        """
        with self._lock:
            if self._size == 0:
                if not block:
                    return []
                self._not_empty.wait(timeout=timeout)
                if self._size == 0:
                    return []

            count = min(max_count, self._size)
            result = []

            for _ in range(count):
                item = self._buffer[self._read_pos]
                self._buffer[self._read_pos] = None
                self._read_pos = (self._read_pos + 1) % self._capacity
                result.append(item)

            self._size -= count
            self._not_full.notify_all()
            return result

    def drain_all(self) -> list:
        """
        排空缓冲区所有数据，用于崩溃时落盘。
        不需要条件变量，直接全部取出。
        """
        with self._lock:
            if self._size == 0:
                return []

            result = []
            while self._size > 0:
                item = self._buffer[self._read_pos]
                self._buffer[self._read_pos] = None
                self._read_pos = (self._read_pos + 1) % self._capacity
                result.append(item)
                self._size -= 1

            self._not_full.notify_all()
            return result

    def get_stats(self) -> dict:
        """获取统计信息。"""
        with self._lock:
            return {
                "capacity": self._capacity,
                "size": self._size,
                "overflow_count": self._overflow_count,
                "dropped_oldest_count": self._dropped_oldest_count,
                "dropped_newest_count": self._dropped_newest_count,
                "overflow_strategy": self._overflow_strategy,
            }
