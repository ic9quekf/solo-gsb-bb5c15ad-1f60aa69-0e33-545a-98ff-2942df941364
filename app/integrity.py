"""成品文件完整性巡检的隔离标记存取。

巡检流程本身（加载会话、流式算摘要、组装响应）编排在 main.integrity_check；
本模块只负责 quarantine_marks 表的读写与结论判定，保持无 HTTP 依赖：

  * 写入：仅由巡检失败路径以 UPSERT 写入（每会话至多一条，重复巡检刷新
    原因与时间）；调用方必须持有会话锁并处于 IMMEDIATE 事务中，且在同事务
    复查会话仍存在——巡检与删除会话按同一会话锁串行，已删除会话不会被
    重新写入标记。
  * 读取：会话下载、票据签发与凭票下载在各自流程中查询标记并拒绝服务；
    凭票下载的判定在原子扣减之前，拒绝绝不扣减次数。
  * 清除：删除会话（DELETE /sessions/{id}）时标记经 ON DELETE CASCADE
    与会话行同事务删除，无需显式清理；校验通过不改变任何状态（包括不
    清除已有标记）——已完成会话的成品不可变，标记只能随会话删除消失。
"""
from __future__ import annotations

import sqlite3

# 巡检结论取值：ok 不落库；其余三种即隔离标记的 reason 取值。
OK = "ok"
MISSING = "missing"
LENGTH_MISMATCH = "length_mismatch"
SHA256_MISMATCH = "sha256_mismatch"


def evaluate(
    disk: tuple[int, str] | None, total_size: int, sha256_expected: str
) -> str:
    """把磁盘核对结果归结为巡检结论。

    ``disk`` 为 storage.hash_artifact_file 的返回：(实际字节数, 实际摘要)，
    文件缺失时为 None。长度核对优先于摘要核对（与逐片校验同一顺序）。
    """
    if disk is None:
        return MISSING
    size, actual = disk
    if size != total_size:
        return LENGTH_MISMATCH
    if actual != sha256_expected:
        return SHA256_MISMATCH
    return OK


def get_mark(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    """读取会话的隔离标记；未隔离返回 None。可在普通连接或事务连接上执行。"""
    return conn.execute(
        "SELECT session_id, reason, created_at FROM quarantine_marks "
        "WHERE session_id = ?",
        (session_id,),
    ).fetchone()


def persist_mark(
    tx: sqlite3.Connection, session_id: str, reason: str, now_iso: str
) -> None:
    """在 IMMEDIATE 事务内写入/刷新隔离标记（每会话至多一条）。

    调用方必须持有会话锁，且本事务已复查会话行仍存在（删除会话与巡检
    按同一会话锁串行，此处只是防御性兜底）：外键约束使已删除会话的
    写入直接失败，绝不会为已删除会话重新写入标记。
    """
    tx.execute(
        "INSERT INTO quarantine_marks (session_id, reason, created_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET "
        "reason = excluded.reason, created_at = excluded.created_at",
        (session_id, reason, now_iso),
    )
