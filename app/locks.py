"""按会话互斥的锁。

同一个会话的分片提交、完成、放弃、懒过期与后台清理互斥，
不同会话之间完全并行。锁本身只是协调者：真正的区间冲突判定
仍然在 BEGIN IMMEDIATE 事务中完成。

需要同时触及两个会话的流程（目前为跨会话复制分片）必须经
:meth:`SessionLocks.hold` 按会话 ID 全局定序加锁，避免
“A 持 B 等、B 持 A 等”式的循环等待死锁。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from collections.abc import Iterator


class SessionLocks:
    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def get(self, session_id: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[session_id] = lock
            return lock

    def discard(self, session_id: str) -> None:
        """会话进入终态后移除锁条目。

        调用方必须已经不持有该锁；若锁正好被其他线程持有则保留条目，
        下一个生命周期的访问会重新拿到同一个锁，不影响正确性。
        """
        with self._guard:
            lock = self._locks.get(session_id)
            if lock is not None and not lock.locked():
                self._locks.pop(session_id, None)

    @contextmanager
    def hold(self, *session_ids: str) -> Iterator[None]:
        """按会话 ID 字典序同时持有多个会话锁。

        所有需要加锁多个会话的代码路径都必须走这里：全局唯一的加锁
        顺序排除了循环等待，因此两个反向并发的复制（A→B 与 B→A）
        只会排队与冲突失败，不可能互相死锁。去重后同一会话只加锁一次。
        """
        ordered_ids = sorted(set(session_ids))
        # 加锁前一次性取出锁对象：释放时仍用这些同一对象，
        # 不重新走 get()——期间条目可能已被 discard 移除，重新获取
        # 会得到一把全新的未上锁，导致“释放别人的锁/重复释放”。
        held = [(session_id, self.get(session_id)) for session_id in ordered_ids]
        for _, lock in held:
            lock.acquire()
        try:
            yield
        finally:
            for _, lock in reversed(held):
                lock.release()


session_locks = SessionLocks()
