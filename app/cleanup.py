"""会话过期与临时文件的后台清理。"""
from __future__ import annotations

import threading
from datetime import timedelta

from . import db, settings, storage
from .locks import session_locks


def expire_due_sessions(now: storage.datetime | None = None) -> list[str]:
    """把已到期且仍处于 uploading 的会话标记为 expired，并删除其分片。

    返回被过期的会话 id 列表。逐会话加会话锁，避免与正在进行的提交冲突。
    """
    now = now or storage.utcnow()
    now_iso = storage.iso(now)

    conn = db.get_conn()
    rows = conn.execute(
        "SELECT id FROM upload_sessions WHERE status = 'uploading' AND expires_at <= ?",
        (now_iso,),
    ).fetchall()

    expired: list[str] = []
    for row in rows:
        session_id = row["id"]
        lock = session_locks.get(session_id)
        # 若某会话正在提交分片，跳过本轮，下轮再清理。
        if not lock.acquire(blocking=False):
            continue
        try:
            with db.immediate() as tx:
                status = tx.execute(
                    "SELECT status, expires_at FROM upload_sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
                if (
                    status is not None
                    and status["status"] == "uploading"
                    and status["expires_at"] <= now_iso
                ):
                    tx.execute(
                        "UPDATE upload_sessions SET status = 'expired' WHERE id = ?",
                        (session_id,),
                    )
                    # 会话结束即认领失效：同事务删除其全部认领令牌。
                    tx.execute(
                        "DELETE FROM chunk_claims WHERE session_id = ?",
                        (session_id,),
                    )
                else:
                    continue
            # 状态落库后再删磁盘分片；过期会话不会再复活，残留分片可由后续轮次兜底。
            import shutil

            shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
            expired.append(session_id)
        finally:
            lock.release()
        session_locks.discard(session_id)
    return expired


def sweep_completed_parts() -> None:
    """已完成/放弃的会话，其分片目录在主流程后若仍残留，在此兜底删除。"""
    import shutil

    conn = db.get_conn()
    rows = conn.execute(
        "SELECT id FROM upload_sessions WHERE status IN ('completed', 'aborted')"
    ).fetchall()
    parts_root = settings.data_dir / "parts"
    for row in rows:
        shutil.rmtree(storage.parts_dir(row["id"]), ignore_errors=True)
    if parts_root.exists():
        # 数据库中已不存在的孤儿会话目录（例如硬删除后残留）。
        known = {
            r["id"]
            for r in conn.execute("SELECT id FROM upload_sessions").fetchall()
        }
        for child in parts_root.iterdir():
            if child.is_dir() and child.name not in known:
                shutil.rmtree(child, ignore_errors=True)


def sweep_expired_claims(now: storage.datetime | None = None) -> int:
    """删除所有已到期的分片认领行，返回删除条数。

    过期认领在认领创建/分片提交时本就不再被接受（按 expires_at 即时判定），
    这里只回收行空间；与并发提交经 BEGIN IMMEDIATE 串行，互不影响正确性。
    """
    now_iso = storage.iso(now or storage.utcnow())
    with db.immediate() as tx:
        cur = tx.execute(
            "DELETE FROM chunk_claims WHERE expires_at <= ?", (now_iso,)
        )
    return cur.rowcount


def run_cleanup(now: storage.datetime | None = None) -> list[str]:
    """执行一轮完整清理，返回被过期的会话 id（供测试调用）。"""
    storage.ensure_dirs()
    expired = expire_due_sessions(now=now)
    sweep_completed_parts()
    sweep_expired_claims(now=now)
    storage.sweep_old_tempfiles(
        (now or storage.utcnow()) - timedelta(hours=1)
    )
    # 原位修复崩溃可能残留的旧分片备份（正常成功/失败路径都会立即回收）。
    storage.sweep_repair_backups(
        (now or storage.utcnow()) - timedelta(hours=1)
    )
    return expired


def start_janitor(stop_event: threading.Event | None = None) -> tuple[threading.Thread, threading.Event]:
    """启动后台守护清理线程，返回 (线程, 停止事件)。"""
    stop_event = stop_event or threading.Event()

    def _loop() -> None:
        while not stop_event.wait(settings.cleanup_interval_seconds):
            try:
                run_cleanup()
            except Exception:  # noqa: BLE001 - 清理失败不能让线程死掉
                import logging

                logging.getLogger("resumable.cleanup").exception("cleanup round failed")

    thread = threading.Thread(target=_loop, name="session-janitor", daemon=True)
    thread.start()
    return thread, stop_event
