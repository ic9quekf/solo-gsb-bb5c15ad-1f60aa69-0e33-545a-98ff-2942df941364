"""FastAPI 入口：弱网客户端的断点续传文件 API。

接口概览（前缀 /api/v1）：
  POST   /sessions                      创建上传会话（可选 Idempotency-Key 幂等创建）
  GET    /sessions/{id}                 查询状态与缺失区间
  GET    /sessions/{id}/progress        进度快照：同一只读读事务返回已收区间与事件游标
  GET    /sessions/{id}/events          按游标分页拉取游标之后的分片接收事件（断线重连增量同步）
  PUT    /sessions/{id}/chunks          提交二进制分片（?start=&end=，半开区间；可选 X-Chunk-SHA256 逐片校验；可选 X-Chunk-Claim 认领令牌）
  PUT    /sessions/{id}/chunks/repair   已收分片原位修复（区间必须与某已登记分片完全一致；可选 X-Chunk-SHA256）
  POST   /sessions/{id}/chunks/audit    上传中会话的分片完整性诊断（按清单逐条核对磁盘分片，只读）
  POST   /sessions/{id}/chunk-claims    认领尚未接收的半开区间，返回一次性随机令牌（5 分钟有效）
  POST   /sessions/{id}/copy-chunk      从已完成来源会话的成品服务端复制一段为目标会话分片
  POST   /sessions/{id}/complete        原子完成（区间全覆盖 + SHA-256 校验）
  DELETE /sessions/{id}                 放弃会话
  GET    /sessions/{id}/download        会话下载（原有接口，不计数；支持单 Range 断点续传：206/416）
  POST   /sessions/{id}/integrity-check 成品完整性巡检：流式核对长度与 SHA-256，异常即隔离
  POST   /sessions/{id}/tickets         签发受控下载票据（有效期 + 次数上限）
  GET    /tickets/{ticket}/download     凭票下载（有效范围原子扣减一次，支持 Range；非法范围/隔离不扣次）
  POST   /tickets/{ticket}/revoke       撤销票据
  GET    /storage/summary              运维只读：按四种状态汇总会话与字节数
"""
from __future__ import annotations

import hashlib
import secrets
import shutil
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from anyio.to_thread import run_sync
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse

from . import claims, db, idempotency, integrity, ranges, schemas, settings, storage
from .cleanup import start_janitor
from .locks import session_locks


# ---------------------------------------------------------------------------
# 响应装配
# ---------------------------------------------------------------------------

def _session_payload(row: sqlite3.Row) -> dict:
    conn = db.get_conn()
    chunk_rows = conn.execute(
        "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
        (row["id"],),
    ).fetchall()
    received = [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
    missing = storage.missing_intervals(received, row["total_size"])
    complete = row["status"] == "completed"
    return {
        "session_id": row["id"],
        "filename": row["filename"],
        "total_size": row["total_size"],
        "sha256_expected": row["sha256_expected"],
        "chunk_size": row["chunk_size"],
        "status": row["status"],
        "bytes_received": row["bytes_received"],
        "complete": complete,
        "fully_received": not missing,
        "missing_ranges": [{"start": s, "end": e} for s, e in missing],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_access_at": row["last_access_at"],
        "completed_at": row["completed_at"],
        "download_url": f"/api/v1/sessions/{row['id']}/download" if complete else None,
    }


def _http(status: int, error: str, **extra: object) -> HTTPException:
    detail: dict[str, object] = {"error": error}
    detail.update(extra)
    return HTTPException(status_code=status, detail=detail)


def _load_session(session_id: str) -> sqlite3.Row:
    """读取会话；调用方必须持有该会话的锁。

    顺带做懒过期：uploading 且已过 expires_at 时立即标记 expired 并删除分片。
    completed 正常返回；aborted/expired 返回 410；不存在返回 404。
    """
    conn = db.get_conn()
    row = conn.execute(
        "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise _http(404, "session not found")
    if row["status"] in ("aborted", "expired"):
        raise _http(410, f"session {row['status']}")
    if row["status"] == "completed":
        return row

    if storage.parse_iso(row["expires_at"]) <= storage.utcnow():
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET status = 'expired' "
                "WHERE id = ? AND status = 'uploading'",
                (session_id,),
            )
            # 会话结束即认领失效：同事务删除其全部认领令牌。
            tx.execute(
                "DELETE FROM chunk_claims WHERE session_id = ?", (session_id,)
            )
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        raise _http(410, "session expired")
    return row


def _existing_intervals(tx: sqlite3.Connection, session_id: str) -> list[tuple[int, int]]:
    return [
        (r["start_offset"], r["end_offset"])
        for r in tx.execute(
            "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    ]


def _record_chunk_event(
    tx: sqlite3.Connection,
    session_id: str,
    start: int,
    end: int,
    source: str,
    created_at_iso: str,
) -> None:
    """在分片登记事务内写入一条分片接收事件（同会话序号严格连续、单调递增）。

    调用方必须处于已开启的 IMMEDIATE 事务中，且本事务刚刚完成该分片的
    chunks 行登记：事件与登记同生共死，事务回滚则事件也不存在。
    ``COALESCE(MAX(seq), 0) + 1`` 配合 (session_id, seq) 唯一约束与
    IMMEDIATE 的写串行化，保证序号无洞、不重复。只允许两类成功登记调用：
    普通 PUT 分片（source='upload'）与服务端 copy-chunk（source='copy'）；
    失败请求、重叠冲突与 chunks/repair 原位修复一律不调用本函数。
    旧会话（v5 之前）事件表为空，首个新分片自然取得 seq=1，已有 chunks
    即为游标 0 的快照基线。
    """
    tx.execute(
        "INSERT INTO chunk_events "
        "(session_id, seq, start_offset, end_offset, source, created_at) "
        "SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ? "
        "FROM chunk_events WHERE session_id = ?",
        (session_id, start, end, source, created_at_iso, session_id),
    )


# ---------------------------------------------------------------------------
# 生命周期：初始化存储/数据库，启动后台清理
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.ensure_dirs()
    db.init_db()
    _, stop_event = start_janitor()
    try:
        yield
    finally:
        stop_event.set()


app = FastAPI(
    title="Resumable Upload API",
    version="1.0.0",
    description="弱网客户端断点续传文件 API：分片乱序提交、区间校验、原子完成。",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 会话管理
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions",
    status_code=201,
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def create_session(
    body: schemas.CreateSessionRequest,
    idempotency_key: str | None = Header(
        default=None,
        description="可选幂等键：1～128 个可见 ASCII 字符；相同键 + 相同请求返回首次创建的会话",
    ),
) -> dict:
    storage.ensure_dirs()

    # 幂等键：只保存 SHA-256 与规范化请求指纹，明文键绝不落库。
    key_hash: str | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        if not idempotency.is_valid_key(idempotency_key):
            raise _http(
                400,
                "invalid Idempotency-Key header",
                requirement=idempotency.KEY_REQUIREMENT,
            )
        key_hash = idempotency.key_hash(idempotency_key)
        fingerprint = idempotency.request_fingerprint(
            filename=body.filename,
            total_size=body.total_size,
            sha256=body.sha256,
            chunk_size=body.chunk_size,
        )

    session_id = uuid.uuid4().hex
    now_iso, expires_iso = storage.future_expiry()
    # BEGIN IMMEDIATE 把整个“查键 → 建会话 → 登记键”串行化：
    # 并发重试在此排队，后到者读到已提交的键记录后直接重放，不会重复建会话。
    with db.immediate() as conn:
        if key_hash is not None:
            existing = conn.execute(
                "SELECT session_id, request_fingerprint FROM idempotency_keys "
                "WHERE key_hash = ?",
                (key_hash,),
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    # 同键不同请求：冲突，回滚且不新建任何会话。
                    raise _http(
                        409,
                        "Idempotency-Key already used with a different request",
                    )
                row = conn.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?",
                    (existing["session_id"],),
                ).fetchone()
                if row is not None:
                    # 重放（首次响应丢失/并发重试）：返回首次创建的会话，仍为 201。
                    return _session_payload(row)
                # 键记录残留而会话已不存在（正常会被 ON DELETE CASCADE 清除）：
                # 防御性清理后按首次请求处理。
                conn.execute(
                    "DELETE FROM idempotency_keys WHERE key_hash = ?", (key_hash,)
                )

        conn.execute(
            "INSERT INTO upload_sessions ("
            "id, filename, total_size, sha256_expected, chunk_size, "
            "status, bytes_received, created_at, expires_at, last_access_at"
            ") VALUES (?, ?, ?, ?, ?, 'uploading', 0, ?, ?, ?)",
            (
                session_id,
                body.filename,
                body.total_size,
                body.sha256,
                body.chunk_size,
                now_iso,
                expires_iso,
                now_iso,
            ),
        )
        if key_hash is not None:
            # 键记录与会话同事务落库：要么都成功，要么一起回滚。
            conn.execute(
                "INSERT INTO idempotency_keys "
                "(key_hash, session_id, request_fingerprint, created_at) "
                "VALUES (?, ?, ?, ?)",
                (key_hash, session_id, fingerprint, now_iso),
            )
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
    storage.parts_dir(session_id).mkdir(parents=True, exist_ok=True)
    return _session_payload(row)


@app.get(
    "/api/v1/sessions/{session_id}",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def get_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        return _session_payload(row)


# ---------------------------------------------------------------------------
# 分片进度增量同步（断线重连 / 多端协作）
#
# 多端在断线重连后需要知道“别的客户端在我离线期间成功提交了哪些区间”。
# 同步模型为“快照 + 游标续读”：
#   1. GET /sessions/{id}/progress 在同一个只读事务（同一一致性快照）中
#      返回当前已收区间（基线）与该会话的事件游标；
#   2. 之后 GET /sessions/{id}/events?cursor=N 按序返回游标之后成功接收
#      的区间（普通上传与服务端复制分片在登记事务内各写一条同会话单调
#      递增的事件；失败、重试冲突与原位修复不产生事件）。
# 不漏提交的依据（SQLite WAL + IMMEDIATE 写串行化）：快照与事件页都在
# 只读事务中读取已提交数据。若事件 E 在快照读之后提交，则其 seq 必然
# 大于快照游标，会出现在游标续读中；若 E 在快照读之前提交，则其区间
# 已包含在快照基线里。不存在“基线没有、事件也读不到”的提交。
# 旧会话不回填事件：迁移前已有的 chunks 即为游标 0 的基线，首个新分片
# 取得 seq=1 后续号。
# ---------------------------------------------------------------------------


def _progress_snapshot(session_id: str) -> dict:
    """在同一个只读事务内读取会话状态、已收区间（合并）与事件游标。"""
    with db.snapshot_read() as conn:
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise _http(404, "session not found")
        if row["status"] in ("aborted", "expired"):
            raise _http(410, f"session {row['status']}")

        # 区间与游标必须取自同一读事务：二者在同一条时间线上，
        # 快照之后提交的分片其事件 seq 一定大于这里读到的游标。
        chunk_rows = conn.execute(
            "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        received = [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
        cursor_row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS cursor FROM chunk_events "
            "WHERE session_id = ?",
            (session_id,),
        ).fetchone()

    merged = storage.merge_intervals(received)
    missing = storage.missing_intervals(received, row["total_size"])
    return {
        "session_id": session_id,
        "status": row["status"],
        "total_size": row["total_size"],
        "bytes_received": row["bytes_received"],
        "received_ranges": [{"start": s, "end": e} for s, e in merged],
        "missing_ranges": [{"start": s, "end": e} for s, e in missing],
        "event_cursor": cursor_row["cursor"],
        # 完成后的会话不再过期；uploading 会话把过期时间回给客户端判断新鲜度。
        "expires_at": row["expires_at"] if row["status"] == "uploading" else None,
    }


@app.get(
    "/api/v1/sessions/{session_id}/progress",
    response_model=schemas.ProgressSnapshotResponse,
    tags=["progress"],
)
def get_progress_snapshot(session_id: str) -> dict:
    """进度快照：同一只读读事务内返回当前已收区间与事件游标（不滑动 TTL）。"""
    return _progress_snapshot(session_id)


def _list_chunk_events(session_id: str, cursor: int, limit: int) -> dict:
    """按游标分页读取事件：按序返回 cursor 之后成功接收的区间。"""
    with db.snapshot_read() as conn:
        row = conn.execute(
            "SELECT status FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise _http(404, "session not found")
        if row["status"] in ("aborted", "expired"):
            raise _http(410, f"session {row['status']}")

        # 多取 1 条判断是否还有下一页，避免额外 COUNT。
        rows = conn.execute(
            "SELECT seq, start_offset, end_offset, source, created_at "
            "FROM chunk_events WHERE session_id = ? AND seq > ? "
            "ORDER BY seq ASC LIMIT ?",
            (session_id, cursor, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    events = [
        {
            "seq": r["seq"],
            "start": r["start_offset"],
            "end": r["end_offset"],
            "source": r["source"],
            "created_at": r["created_at"],
        }
        for r in page
    ]
    return {
        "session_id": session_id,
        "cursor": cursor,
        "events": events,
        # 游标即“本会话已读位置”：只要本页非空就返回末条事件 seq，
        # 客户端总能前移游标；是否立即翻页由 has_more 决定。
        "next_cursor": page[-1]["seq"] if page else None,
        "has_more": has_more,
    }


@app.get(
    "/api/v1/sessions/{session_id}/events",
    response_model=schemas.ChunkEventListResponse,
    tags=["progress"],
)
def list_chunk_events(
    session_id: str,
    cursor: int = Query(
        0, ge=0, description="事件游标：只返回序号严格大于该游标的事件"
    ),
    limit: int = Query(
        100, ge=1, le=100, description="每页事件条数，范围 1～100（默认 100）"
    ),
) -> dict:
    """按游标分页返回成功接收区间事件（只读、按 seq 升序，页大小 1～100）。"""
    return _list_chunk_events(session_id, cursor, limit)


@app.delete("/api/v1/sessions/{session_id}", status_code=204, tags=["sessions"])
def abort_session(session_id: str) -> Response:
    with session_locks.get(session_id):
        with db.immediate() as tx:
            tx.execute(
                "DELETE FROM upload_sessions WHERE id = ?", (session_id,)
            )  # chunks 由 ON DELETE CASCADE 删除
        storage.remove_session_tree(session_id)
    session_locks.discard(session_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# 分片认领（chunk-claims）
#
# 上传中会话可认领一个尚未接收的半开区间，获得一次性随机令牌（默认 5 分钟
# 有效）。认领用于多上传者协调：有效（未过期、未消费）认领覆盖的区间内，
# 只有持令牌的请求且区间与认领完全一致才能提交分片；未认领区间沿用原有
# 行为。不变式：有效认领覆盖的区间必然尚未接收——
#   * 认领创建在单个 IMMEDIATE 事务内复核会话状态、已收区间与有效认领；
#   * 分片落盘登记（PUT /chunks 与 copy-chunk）在同一事务内复核认领规则，
#     成功提交即消费（删除）认领，失败/断流回滚不消费；
#   * 会话完成/过期/放弃时认领一并失效（显式删除或外键级联）。
# 服务端只保存令牌明文的 SHA-256，明文仅在认领响应中返回一次。
# ---------------------------------------------------------------------------


def _check_chunk_claim(
    conn: sqlite3.Connection,
    session_id: str,
    start: int,
    end: int,
    presented: tuple[str, str] | None,
) -> str | None:
    """分片区间与认领规则的权威校验；调用方必须持有该会话的锁。

    ``presented`` 为请求解析出的 ``(claim_id, token_hash)``，未带头为 None。
    返回成功提交时应消费的认领 id；无需消费时返回 None。
    可在普通连接（预校验）或事务连接（登记前复核）上执行。
    """
    now_iso = storage.iso(storage.utcnow())
    if presented is not None:
        claim_id, token_hash = presented
        row = conn.execute(
            "SELECT * FROM chunk_claims "
            "WHERE id = ? AND session_id = ? AND token_hash = ?",
            (claim_id, session_id, token_hash),
        ).fetchone()
        if row is None:
            raise _http(403, "invalid X-Chunk-Claim credential")
        if row["expires_at"] <= now_iso:
            # 到期后旧令牌不可用；区间可在过期后重新认领或直接提交。
            raise _http(410, "chunk claim expired")
        if row["start_offset"] != start or row["end_offset"] != end:
            raise _http(
                409,
                "chunk range does not exactly match the claim",
                claim_start=row["start_offset"],
                claim_end=row["end_offset"],
            )
        # 有效认领互不相交，区间与认领完全一致时不可能再撞上其他有效认领。
        return claim_id

    blocking = conn.execute(
        "SELECT start_offset, end_offset FROM chunk_claims "
        "WHERE session_id = ? AND expires_at > ? "
        "AND start_offset < ? AND end_offset > ?",
        (session_id, now_iso, end, start),
    ).fetchone()
    if blocking is not None:
        raise _http(
            409,
            "chunk range intersects an active claim",
            claim_start=blocking["start_offset"],
            claim_end=blocking["end_offset"],
        )
    return None


@app.post(
    "/api/v1/sessions/{session_id}/chunk-claims",
    status_code=201,
    response_model=schemas.ChunkClaimResponse,
    tags=["claims"],
)
def create_chunk_claim(session_id: str, body: schemas.ChunkClaimRequest) -> dict:
    """认领一个尚未接收的半开区间，返回一次性随机令牌（默认 5 分钟有效）。"""
    start, end = body.start, body.end
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")
        total = row["total_size"]
        if end > total:
            raise _http(
                400,
                "claim range exceeds total_size",
                start=start,
                end=end,
                total_size=total,
            )
        if settings.max_chunk_bytes and end - start > settings.max_chunk_bytes:
            raise _http(
                413,
                "claimed range larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )

        claim_id = uuid.uuid4().hex
        token = claims.new_token()
        now = storage.utcnow()
        now_iso = storage.iso(now)
        expires_iso = storage.iso(
            now + timedelta(seconds=settings.chunk_claim_ttl_seconds)
        )

        # 单个 IMMEDIATE 事务内复核会话状态、已收区间与有效认领后登记：
        # 并发认领同一区间在此串行，后到者看到已提交的认领而冲突失败。
        with db.immediate() as tx:
            current = tx.execute(
                "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if current is None:
                raise _http(404, "session not found")
            if current["status"] in ("aborted", "expired") or (
                current["status"] == "uploading"
                and storage.parse_iso(current["expires_at"]) <= storage.utcnow()
            ):
                raise _http(410, "session no longer active")
            if current["status"] == "completed":
                raise _http(409, "session already completed")

            # 顺手回收本会话已到期的旧认领：过期区间自此可重新认领。
            tx.execute(
                "DELETE FROM chunk_claims WHERE session_id = ? AND expires_at <= ?",
                (session_id, now_iso),
            )

            # 只能认领尚未接收的区间（与已收区间相邻允许）。
            if storage.overlaps_any(start, end, _existing_intervals(tx, session_id)):
                raise _http(
                    409,
                    "claim range overlaps an already received range",
                    start=start,
                    end=end,
                )

            # 相交的有效认领只能有一个；过期认领已在上面回收，不参与判定。
            blocking = tx.execute(
                "SELECT start_offset, end_offset FROM chunk_claims "
                "WHERE session_id = ? AND expires_at > ? "
                "AND start_offset < ? AND end_offset > ?",
                (session_id, now_iso, end, start),
            ).fetchone()
            if blocking is not None:
                raise _http(
                    409,
                    "range intersects an active claim",
                    claim_start=blocking["start_offset"],
                    claim_end=blocking["end_offset"],
                )

            tx.execute(
                "INSERT INTO chunk_claims "
                "(id, session_id, start_offset, end_offset, token_hash, "
                "created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    claim_id,
                    session_id,
                    start,
                    end,
                    claims.token_hash(token),
                    now_iso,
                    expires_iso,
                ),
            )

    return {
        "claim_id": claim_id,
        "session_id": session_id,
        "start": start,
        "end": end,
        "created_at": now_iso,
        "expires_at": expires_iso,
        # 令牌明文只出现这一次；库里只有它的 SHA-256。
        "claim": f"{claim_id}.{token}",
    }


# ---------------------------------------------------------------------------
# 分片提交
# ---------------------------------------------------------------------------

def _prepare_chunk(
    session_id: str,
    start: int,
    end: int,
    declared_length: int,
    presented_claim: tuple[str, str] | None = None,
) -> None:
    """在线程池中执行：会话状态、越界、重叠、认领与大小的首次权威校验，并滑动 TTL。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")
        total = row["total_size"]
        if end > total:
            raise _http(
                400,
                "chunk range exceeds total_size",
                start=start,
                end=end,
                total_size=total,
            )
        if settings.max_chunk_bytes and end - start > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        if declared_length != end - start:
            raise _http(
                400,
                "content-length does not match end-start",
                content_length=declared_length,
                expected=end - start,
            )
        conn = db.get_conn()
        existing = [
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        ]
        if storage.overlaps_any(start, end, existing):
            raise _http(
                409,
                "chunk overlaps an already received range",
                start=start,
                end=end,
            )
        # 认领规则首次校验（接收请求体之前拒绝，权威复核在提交事务内）。
        _check_chunk_claim(conn, session_id, start, end, presented_claim)
        now = storage.utcnow()
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET last_access_at = ?, expires_at = ? "
                "WHERE id = ? AND status = 'uploading'",
                (
                    storage.iso(now),
                    storage.iso(now + timedelta(seconds=settings.ttl_seconds)),
                    session_id,
                ),
            )


def _commit_chunk(
    session_id: str,
    start: int,
    end: int,
    tmp_file: str,
    presented_claim: tuple[str, str] | None = None,
) -> dict:
    """落盘临时分片并在单个 IMMEDIATE 事务中二次校验 + 登记（成功即消费认领）。"""
    tmp_path = Path(tmp_file)
    with session_locks.get(session_id):
        try:
            with db.immediate() as tx:
                row = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if row is None:
                    raise _http(404, "session not found")
                if row["status"] in ("aborted", "expired") or (
                    row["status"] == "uploading"
                    and storage.parse_iso(row["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if row["status"] == "completed":
                    raise _http(409, "session already completed")
                if end > row["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                existing = _existing_intervals(tx, session_id)
                if storage.overlaps_any(start, end, existing):
                    raise _http(
                        409,
                        "chunk overlaps an already received range",
                        start=start,
                        end=end,
                    )

                # 事务内复核认领规则：预校验之后认领可能已过期/被消费/新建。
                consume_claim_id = _check_chunk_claim(
                    tx, session_id, start, end, presented_claim
                )

                # 事务内移动文件：若后续 SQL 失败，事务回滚且下面的 except 会删除文件。
                final = storage.part_path(session_id, start, end)
                final.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.replace(final)

                now = storage.utcnow()
                now_iso = storage.iso(now)
                expires_iso = storage.iso(
                    now + timedelta(seconds=settings.ttl_seconds)
                )
                tx.execute(
                    "INSERT INTO chunks "
                    "(session_id, start_offset, end_offset, size, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (session_id, start, end, end - start, now_iso),
                )
                # 成功登记才写事件：与 chunks 行同一事务，同会话序号连续递增。
                _record_chunk_event(tx, session_id, start, end, "upload", now_iso)
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET bytes_received = bytes_received + ?, "
                    "last_access_at = ?, expires_at = ? WHERE id = ?",
                    (end - start, now_iso, expires_iso, session_id),
                )
                # 成功提交消费令牌：与分片登记同一事务，回滚则不消费。
                if consume_claim_id is not None:
                    tx.execute(
                        "DELETE FROM chunk_claims WHERE id = ?",
                        (consume_claim_id,),
                    )
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    return _chunk_accepted_payload(session_id, start, end)


def _chunk_accepted_payload(session_id: str, start: int, end: int) -> dict:
    """分片登记成功后的统一响应体（PUT 分片与 copy-chunk 共用同一形态）。"""
    fresh = db.get_conn().execute(
        "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    payload = _session_payload(fresh)
    return {
        "session_id": session_id,
        "received": {"start": start, "end": end},
        "bytes_received": payload["bytes_received"],
        "missing_ranges": payload["missing_ranges"],
        "fully_received": payload["fully_received"],
        "expires_at": payload["expires_at"],
    }


# ---------------------------------------------------------------------------
# 已收分片原位修复（PUT /chunks/repair）
#
# 与普通 PUT 分片的区别只有一点：区间必须与一条**已登记**分片完全一致，
# 新内容等长地**替换**旧分片，不新增区间、不改变 bytes_received 与缺失
# 区间。整体两阶段结构沿用 PUT 分片：
#   1. _prepare_chunk_repair 持会话锁做首次权威校验（会话仍在上传、
#      区间与某已登记分片完全一致、长度/单片上限合法），但**不**滑动
#      TTL——任何修复失败都保持会话与分片原样，包括过期时间。
#   2. 请求体流式落到 tmp（可增量计算 X-Chunk-SHA256），随后
#      _commit_chunk_repair 再次持会话锁，在单个 BEGIN IMMEDIATE 事务内
#      二次校验，并用“旧分片同目录备份 → 新分片原子 rename 落位 →
#      更新 last_access/expires_at”的顺序完成替换；任一步失败都把旧
#      分片原子恢复回来，客户端可用旧内容重试。
# 流式接收期间（不持锁）并发完成/放弃只会得到两种干净结局：
# 完成先发生 → 修复在事务内看到 completed 返回 409，磁盘分片随完成清理
# 被删除，临时文件由修复自行删除，成品早已发布；放弃先发生 → 修复看到
# 404，分片目录被硬删除。任何一方都读不到“半替换”的分片。
# ---------------------------------------------------------------------------


def _prepare_chunk_repair(
    session_id: str, start: int, end: int, declared_length: int
) -> None:
    """线程池中执行：修复前的首次权威校验（不滑动 TTL、不改任何状态）。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")
        if end > row["total_size"]:
            raise _http(
                400,
                "chunk range exceeds total_size",
                start=start,
                end=end,
                total_size=row["total_size"],
            )
        if settings.max_chunk_bytes and end - start > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        if declared_length != end - start:
            raise _http(
                400,
                "content-length does not match end-start",
                content_length=declared_length,
                expected=end - start,
            )
        # 区间必须与一条已登记分片**完全一致**（同一半开区间）；
        # 仅相交、相邻或落在合并区间内但跨过分片边界都不算。
        conn = db.get_conn()
        exact = conn.execute(
            "SELECT 1 FROM chunks "
            "WHERE session_id = ? AND start_offset = ? AND end_offset = ?",
            (session_id, start, end),
        ).fetchone()
        if exact is None:
            raise _http(
                404,
                "no registered chunk matches the range exactly",
                start=start,
                end=end,
            )


def _commit_chunk_repair(
    session_id: str, start: int, end: int, tmp_file: str
) -> dict:
    """事务内把旧分片原子替换为新内容；任何失败都恢复旧分片。"""
    tmp_path = Path(tmp_file)
    final = storage.part_path(session_id, start, end)
    backup = storage.part_repair_backup_path(session_id, start, end)
    with session_locks.get(session_id):
        try:
            with db.immediate() as tx:
                row = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if row is None:
                    raise _http(404, "session not found")
                if row["status"] in ("aborted", "expired") or (
                    row["status"] == "uploading"
                    and storage.parse_iso(row["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if row["status"] == "completed":
                    raise _http(409, "session already completed")
                if end > row["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                # 事务内复查登记仍在（并发放弃会整行删除会话与级联分片）。
                exact = tx.execute(
                    "SELECT 1 FROM chunks "
                    "WHERE session_id = ? AND start_offset = ? AND end_offset = ?",
                    (session_id, start, end),
                ).fetchone()
                if exact is None:
                    raise _http(
                        404,
                        "no registered chunk matches the range exactly",
                        start=start,
                        end=end,
                    )

                # 文件替换全程在 parts/<sid>/ 同一目录内：先把旧分片改名
                # 备份，再让新分片落位，两步都是同卷原子 rename，
                # 磁盘上任何瞬间要么是旧分片、要么是新分片，不存在半成品。
                final.parent.mkdir(parents=True, exist_ok=True)
                if final.is_file():
                    final.replace(backup)
                tmp_path.replace(final)

                # 只滑动活动时间：区间登记（chunks 行）与 bytes_received
                # 一律不动，缺失区间自然保持不变。
                now = storage.utcnow()
                now_iso = storage.iso(now)
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET last_access_at = ?, expires_at = ? WHERE id = ?",
                    (
                        now_iso,
                        storage.iso(now + timedelta(seconds=settings.ttl_seconds)),
                        session_id,
                    ),
                )
        except BaseException:
            # 事务已回滚（登记与字节数保持旧值）。磁盘恢复只看备份是否
            # 存在：备份在，说明旧分片已被改名——无论新分片是否落位，
            # 都删掉正式路径并把备份原子改回，覆盖两次 rename 之间失败的
            # 极端情况；备份不在，说明替换从未开始，旧分片原封不动。
            if backup.exists():
                final.unlink(missing_ok=True)
                backup.replace(final)
            tmp_path.unlink(missing_ok=True)
            raise

        # 提交成功后备份才完成使命（旧分片已被新版本取代）。
        backup.unlink(missing_ok=True)

    return _chunk_accepted_payload(session_id, start, end)


@app.put(
    "/api/v1/sessions/{session_id}/chunks/repair",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
async def repair_chunk(
    session_id: str,
    request: Request,
    start: int = Query(..., ge=0, description="起始偏移（含）"),
    end: int = Query(..., ge=0, description="结束偏移（不含），半开区间 [start, end)"),
    chunk_sha256: str | None = Header(
        default=None,
        alias="X-Chunk-SHA256",
        description=(
            "可选：新分片内容的 SHA-256（64 位十六进制，可带 sha256: 前缀）。"
            "提供时服务端流式校验长度与摘要；格式错误返回 400、长度不符返回 "
            "400/413、摘要不匹配返回 422，均保留原分片且不滑动 TTL。"
        ),
    ),
) -> dict:
    if end <= start:
        raise _http(
            400, "invalid range: require 0 <= start < end", start=start, end=end
        )
    declared_raw = request.headers.get("content-length")
    if declared_raw is None:
        raise _http(411, "content-length header is required")
    try:
        declared_length = int(declared_raw)
    except ValueError:
        raise _http(400, "invalid content-length header")
    if declared_length != end - start:
        raise _http(
            400,
            "content-length does not match end-start",
            content_length=declared_length,
            expected=end - start,
        )
    if settings.max_chunk_bytes and declared_length > settings.max_chunk_bytes:
        raise _http(
            413, "chunk larger than MAX_CHUNK_BYTES",
            max_chunk_bytes=settings.max_chunk_bytes,
        )
    # 与普通 PUT 一致：头格式错误在读体之前拒绝，不滑动 TTL、不接触存储。
    if chunk_sha256 is not None:
        try:
            expected_chunk_digest = schemas.normalize_sha256(chunk_sha256)
        except ValueError:
            raise _http(
                400,
                "invalid X-Chunk-SHA256 header",
                requirement="64 lowercase hex characters, optional sha256: prefix",
            )
    else:
        expected_chunk_digest = None

    await run_sync(_prepare_chunk_repair, session_id, start, end, declared_length)

    # 流式接收与逐片摘要计算与普通 PUT 完全相同：失败只清理临时文件，
    # 旧分片与其登记从这一刻起根本还没被触碰。
    tmp = storage.new_temp_file()
    received = 0
    digest = hashlib.sha256() if expected_chunk_digest is not None else None
    try:
        with tmp.open("wb") as out:
            async for block in request.stream():
                received += len(block)
                if received > declared_length:
                    raise _http(
                        413,
                        "request body longer than declared range",
                        declared=declared_length,
                    )
                out.write(block)
                if digest is not None:
                    digest.update(block)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if received != end - start:
        tmp.unlink(missing_ok=True)
        raise _http(
            400,
            "body length does not match declared range",
            bytes_received=received,
            expected=end - start,
        )

    if digest is not None:
        actual_chunk_digest = digest.hexdigest()
        if actual_chunk_digest != expected_chunk_digest:
            # 摘要不符：临时文件删除即可，旧分片、登记与字节数原封不动。
            tmp.unlink(missing_ok=True)
            raise _http(
                422,
                "chunk sha256 mismatch",
                start=start,
                end=end,
                expected=expected_chunk_digest,
                actual=actual_chunk_digest,
            )

    return await run_sync(_commit_chunk_repair, session_id, start, end, str(tmp))


@app.put(
    "/api/v1/sessions/{session_id}/chunks",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
async def put_chunk(
    session_id: str,
    request: Request,
    start: int = Query(..., ge=0, description="起始偏移（含）"),
    end: int = Query(..., ge=0, description="结束偏移（不含），半开区间 [start, end)"),
    chunk_sha256: str | None = Header(
        default=None,
        alias="X-Chunk-SHA256",
        description=(
            "可选：本分片原始字节的 SHA-256（64 位十六进制，可带 sha256: 前缀）。"
            "提供时服务端在写入分片前校验请求体长度与摘要；格式错误返回 400，"
            "摘要不匹配返回 422，且不落盘、不登记区间，重传正确内容即可恢复。"
        ),
    ),
    chunk_claim: str | None = Header(
        default=None,
        alias="X-Chunk-Claim",
        description=(
            "可选：分片认领令牌 <claim_id>.<token>（POST chunk-claims 返回）。"
            "区间与有效认领相交时必须携带：令牌匹配且区间与认领完全一致才接受，"
            "成功提交即消费令牌；未认领区间不携带该头，行为不变。"
        ),
    ),
) -> dict:
    if end <= start:
        raise _http(
            400, "invalid range: require 0 <= start < end", start=start, end=end
        )
    declared_raw = request.headers.get("content-length")
    if declared_raw is None:
        raise _http(411, "content-length header is required")
    try:
        declared_length = int(declared_raw)
    except ValueError:
        raise _http(400, "invalid content-length header")
    if declared_length != end - start:
        raise _http(
            400,
            "content-length does not match end-start",
            content_length=declared_length,
            expected=end - start,
        )
    if settings.max_chunk_bytes and declared_length > settings.max_chunk_bytes:
        raise _http(
            413, "chunk larger than MAX_CHUNK_BYTES",
            max_chunk_bytes=settings.max_chunk_bytes,
        )
    # 头格式错误在读体之前拒绝：不消耗请求体、不滑动 TTL、不接触存储。
    if chunk_sha256 is not None:
        try:
            expected_chunk_digest = schemas.normalize_sha256(chunk_sha256)
        except ValueError:
            raise _http(
                400,
                "invalid X-Chunk-SHA256 header",
                requirement="64 lowercase hex characters, optional sha256: prefix",
            )
    else:
        expected_chunk_digest = None
    # 认领头同样在读体之前解析：格式非法直接 400，不消耗请求体。
    presented_claim: tuple[str, str] | None = None
    if chunk_claim is not None:
        try:
            claim_id, claim_token = claims.parse_credential(chunk_claim)
        except ValueError:
            raise _http(
                400,
                "invalid X-Chunk-Claim header",
                requirement=claims.CREDENTIAL_REQUIREMENT,
            )
        presented_claim = (claim_id, claims.token_hash(claim_token))

    # 首次校验 + TTL 滑动（阻塞 DB 操作丢到线程池，避免卡住事件循环）。
    await run_sync(
        _prepare_chunk, session_id, start, end, declared_length, presented_claim
    )

    # 流式落临时文件，弱网下也不会把整分片读进内存。
    # 提供 X-Chunk-SHA256 时同步增量计算摘要：校验不过只删除临时文件，
    # 绝不移动到分片目录、绝不登记区间，因此重传正确内容后仍可正常完成。
    tmp = storage.new_temp_file()
    received = 0
    digest = hashlib.sha256() if expected_chunk_digest is not None else None
    try:
        with tmp.open("wb") as out:
            async for block in request.stream():
                received += len(block)
                if received > declared_length:
                    raise _http(
                        413,
                        "request body longer than declared range",
                        declared=declared_length,
                    )
                out.write(block)
                if digest is not None:
                    digest.update(block)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if received != end - start:
        tmp.unlink(missing_ok=True)
        raise _http(
            400,
            "body length does not match declared range",
            bytes_received=received,
            expected=end - start,
        )

    if digest is not None:
        actual_chunk_digest = digest.hexdigest()
        if actual_chunk_digest != expected_chunk_digest:
            tmp.unlink(missing_ok=True)
            raise _http(
                422,
                "chunk sha256 mismatch",
                start=start,
                end=end,
                expected=expected_chunk_digest,
                actual=actual_chunk_digest,
            )

    return await run_sync(
        _commit_chunk, session_id, start, end, str(tmp), presented_claim
    )


# ---------------------------------------------------------------------------
# 分片完整性诊断（POST /chunks/audit）
#
# 客户端提交 {start, end, sha256} 清单（1～32 条），服务端按起点顺序流式
# 读取磁盘分片并逐条比对，返回每条的核对结论与实际摘要，供客户端圈定
# chunks/repair 的目标区间。诊断是纯只读：不滑动 TTL、不改分片登记与
# 已收字节；全过程持有会话锁，修复/完成/放弃都无法在核对中途插入，
# 因此不会把替换中的瞬时状态误报为缺失或损坏。
# 清单本身必须合法：每条区间都要与某已登记分片完全一致且不可重复，
# 任一不符整批拒绝（一条都不核对）。
# ---------------------------------------------------------------------------


def _audit_chunks(session_id: str, entries: list[schemas.ChunkAuditEntry]) -> dict:
    """持会话锁完成全部磁盘核对（只读，不触碰 TTL、登记与已收字节）。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed")

        # 清单与已登记分片逐条精确对应；任一区间无精确匹配即整批拒绝。
        conn = db.get_conn()
        registered = {
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        }
        for entry in entries:
            if (entry.start, entry.end) not in registered:
                raise _http(
                    404,
                    "no registered chunk matches the range exactly",
                    start=entry.start,
                    end=entry.end,
                )

        # 按起点顺序流式核对磁盘分片；会话锁保证读到的是稳定状态。
        results: list[dict] = []
        for entry in sorted(entries, key=lambda e: (e.start, e.end)):
            disk = storage.hash_part_file(session_id, entry.start, entry.end)
            if disk is None:
                results.append(
                    {
                        "start": entry.start,
                        "end": entry.end,
                        "status": "missing",
                        "expected_sha256": entry.sha256,
                        "actual_sha256": None,
                        "size_on_disk": None,
                    }
                )
                continue
            size, actual = disk
            if size != entry.end - entry.start:
                status = "length_mismatch"
            elif actual != entry.sha256:
                status = "sha256_mismatch"
            else:
                status = "ok"
            results.append(
                {
                    "start": entry.start,
                    "end": entry.end,
                    "status": status,
                    "expected_sha256": entry.sha256,
                    "actual_sha256": actual,
                    "size_on_disk": size,
                }
            )

    return {
        "session_id": session_id,
        "all_match": all(r["status"] == "ok" for r in results),
        "results": results,
    }


@app.post(
    "/api/v1/sessions/{session_id}/chunks/audit",
    response_model=schemas.ChunkAuditResponse,
    tags=["chunks"],
)
def audit_chunks(session_id: str, body: schemas.ChunkAuditRequest) -> dict:
    """按客户端清单逐条核对磁盘分片，返回匹配/摘要不符/文件缺失/长度不符。"""
    return _audit_chunks(session_id, body.chunks)


# ---------------------------------------------------------------------------
# 服务端复制分片：从已完成来源会话的成品复制一段到目标会话
#
# 与 PUT 分片的登记规则完全一致（同一校验、同一响应、同一 chunks 表），
# 只是字节来源从客户端请求体换成来源成品文件。流程分两段：
#   1. 同时按 ID 定序持有“来源 + 目标”两把会话锁，完成来源资格、
#      来源区间、目标边界、单片大小、与已收区间重叠的全部预校验，
#      然后流式复制到临时文件；复制期间来源无法被放弃删除、目标无法被放弃。
#   2. 在单个 BEGIN IMMEDIATE 事务中二次校验后移动临时文件并登记，
#      与 _commit_chunk 同样的失败清理：任何异常都回滚事务、删除
#      临时文件（及未登记的落地文件），绝不留下已登记的损坏分片。
# 固定的加锁顺序（ID 字典序）排除了 A→B 与 B→A 反向并发时的循环等待。
# ---------------------------------------------------------------------------


def _copy_chunk(
    target_session_id: str,
    source_session_id: str,
    source_start: int,
    source_end: int,
    target_start: int,
) -> dict:
    length = source_end - source_start
    target_end = target_start + length

    with session_locks.hold(source_session_id, target_session_id):
        # 目标：与 PUT 分片相同的存在性/懒过期加载。
        target = _load_session(target_session_id)
        if target["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")

        # 来源：只允许已完成且成品在盘的会话；未完成/成品缺失一律拒绝。
        conn = db.get_conn()
        source = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (source_session_id,)
        ).fetchone()
        if source is None:
            raise _http(
                404, "source session not found",
                source_session_id=source_session_id,
            )
        if source["status"] != "completed":
            raise _http(
                409,
                "source session is not completed",
                source_session_id=source_session_id,
                source_status=source["status"],
            )
        if not storage.artifact_path(source_session_id).is_file():
            raise _http(
                409,
                "source completed artifact is missing",
                source_session_id=source_session_id,
            )

        # 来源半开区间必须完整落在来源成品大小内。
        if source_end > source["total_size"]:
            raise _http(
                400,
                "source range exceeds source total_size",
                source_start=source_start,
                source_end=source_end,
                source_total_size=source["total_size"],
            )

        # 目标边界：[target_start, target_end) 不越界。
        if target_end > target["total_size"]:
            raise _http(
                400,
                "copied chunk range exceeds target total_size",
                start=target_start,
                end=target_end,
                total_size=target["total_size"],
            )
        # 单片大小限制与 PUT 分片一致。
        if settings.max_chunk_bytes and length > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        # 与目标已收区间不得重叠（相邻允许）。
        existing = [
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks "
                "WHERE session_id = ?",
                (target_session_id,),
            ).fetchall()
        ]
        if storage.overlaps_any(target_start, target_end, existing):
            raise _http(
                409,
                "chunk overlaps an already received range",
                start=target_start,
                end=target_end,
            )
        # 认领区间只能由持令牌者经 PUT /chunks 提交；copy-chunk 不携带令牌，
        # 与有效认领相交的目标区间在此拒绝（权威复核在提交事务内）。
        _check_chunk_claim(
            conn, target_session_id, target_start, target_end, None
        )

        # 预校验全部通过才复制（仍在双锁内）：流式切片到 tmp，
        # 成品被并发截断/替换会在此抛错，临时文件由辅助函数自行清理。
        try:
            tmp_file = storage.copy_artifact_segment(
                source_session_id, source_start, source_end
            )
        except (ValueError, OSError) as exc:
            raise _http(
                409,
                f"cannot copy source artifact segment: {exc}",
                source_session_id=source_session_id,
            ) from exc
        tmp_path = Path(tmp_file)
        final = storage.part_path(target_session_id, target_start, target_end)

        # 单事务二次校验 + 落盘 + 登记：与 _commit_chunk 同一原子性保证。
        # 失败清理必须仍在双锁内完成：否则刚 replace 落地、事务却回滚的
        # part 文件可能与并发 PUT 同路径竞争，误删别人刚登记的分片。
        try:
            with db.immediate() as tx:
                trow = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?",
                    (target_session_id,),
                ).fetchone()
                if trow is None:
                    raise _http(404, "session not found")
                if trow["status"] in ("aborted", "expired") or (
                    trow["status"] == "uploading"
                    and storage.parse_iso(trow["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if trow["status"] == "completed":
                    raise _http(409, "session already completed")
                if target_end > trow["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                if storage.overlaps_any(
                    target_start, target_end, _existing_intervals(tx, target_session_id)
                ):
                    raise _http(
                        409,
                        "chunk overlaps an already received range",
                        start=target_start,
                        end=target_end,
                    )

                # 事务内复核认领规则：复制期间目标区间可能已被他人认领。
                _check_chunk_claim(
                    tx, target_session_id, target_start, target_end, None
                )

                # 来源资格在事务内复查：来源若已被并发放弃删除，此处拿到
                # 状态变化/成品缺失，事务回滚，不登记任何分片。
                srow = tx.execute(
                    "SELECT status, total_size FROM upload_sessions WHERE id = ?",
                    (source_session_id,),
                ).fetchone()
                if srow is None or srow["status"] != "completed":
                    raise _http(
                        409,
                        "source session is not completed",
                        source_session_id=source_session_id,
                    )
                if source_end > srow["total_size"]:
                    raise _http(400, "source range exceeds source total_size")
                if not storage.artifact_path(source_session_id).is_file():
                    raise _http(
                        409,
                        "source completed artifact is missing",
                        source_session_id=source_session_id,
                    )

                # 事务内移动文件：后续任一 SQL 失败都会回滚并由 except 删除文件。
                final.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.replace(final)

                now = storage.utcnow()
                now_iso = storage.iso(now)
                expires_iso = storage.iso(
                    now + timedelta(seconds=settings.ttl_seconds)
                )
                tx.execute(
                    "INSERT INTO chunks "
                    "(session_id, start_offset, end_offset, size, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (target_session_id, target_start, target_end, length, now_iso),
                )
                # 服务端复制分片成功登记同样产生事件（source='copy'）；
                # 与登记同一事务，失败回滚则不产生事件。
                _record_chunk_event(
                    tx, target_session_id, target_start, target_end, "copy", now_iso
                )
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET bytes_received = bytes_received + ?, "
                    "last_access_at = ?, expires_at = ? WHERE id = ?",
                    (length, now_iso, expires_iso, target_session_id),
                )
        except BaseException:
            # 仍持有两把会话锁：临时文件与可能已 replace 但未登记的落地
            # 文件一并删除，绝不留下已登记/半登记的损坏分片。
            tmp_path.unlink(missing_ok=True)
            final.unlink(missing_ok=True)
            raise

        return _chunk_accepted_payload(target_session_id, target_start, target_end)


@app.post(
    "/api/v1/sessions/{session_id}/copy-chunk",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
def copy_chunk(session_id: str, body: schemas.CopyChunkRequest) -> dict:
    """从已完成来源会话的成品服务端复制一段，登记为本会话的一个分片。"""
    if body.source_session_id == session_id:
        raise _http(
            400,
            "source and target session must differ",
            source_session_id=body.source_session_id,
        )
    return _copy_chunk(
        target_session_id=session_id,
        source_session_id=body.source_session_id,
        source_start=body.source_start,
        source_end=body.source_end,
        target_start=body.target_start,
    )


# ---------------------------------------------------------------------------
# 完成与下载
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions/{session_id}/complete",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def complete_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            return _session_payload(row)

        conn = db.get_conn()
        intervals = sorted(
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        )
        missing = storage.missing_intervals(intervals, row["total_size"])
        if missing:
            raise _http(
                409,
                "range incomplete; missing ranges remain",
                missing_ranges=[{"start": s, "end": e} for s, e in missing],
            )

        # 拼装 + 整文件 SHA-256。成品先落在 tmp，绝不提前暴露。
        try:
            tmp_file, digest = storage.assemble_artifact(
                session_id, intervals, row["total_size"]
            )
        except (ValueError, FileNotFoundError, OSError) as exc:
            raise _http(409, f"cannot assemble artifact: {exc}") from exc

        if digest != row["sha256_expected"]:
            Path(tmp_file).unlink(missing_ok=True)
            raise _http(
                422,
                "sha256 mismatch",
                expected=row["sha256_expected"],
                actual=digest,
            )

        storage.files_dir().mkdir(parents=True, exist_ok=True)
        # 与状态翻转放进同一个事务窗口：提交成功前成品不可见；
        # 任一步失败都回滚并删除刚落地的文件，保证“失败没有可下载成品”。
        try:
            with db.immediate() as tx:
                current = tx.execute(
                    "SELECT status FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if current is None or current["status"] != "uploading":
                    raise _http(409, "session is not uploadable")
                storage.atomic_publish(tmp_file, session_id)
                now_iso = storage.iso(storage.utcnow())
                tx.execute(
                    "UPDATE upload_sessions SET "
                    "status = 'completed', bytes_received = ?, "
                    "completed_at = ?, last_access_at = ? WHERE id = ?",
                    (row["total_size"], now_iso, now_iso, session_id),
                )
                # 会话结束即认领失效：与状态翻转同事务删除全部认领令牌。
                tx.execute(
                    "DELETE FROM chunk_claims WHERE session_id = ?", (session_id,)
                )
        except BaseException:
            storage.artifact_path(session_id).unlink(missing_ok=True)
            Path(tmp_file).unlink(missing_ok=True)
            raise

        # 提交成功后才删除分片，节省磁盘。
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        fresh = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        return _session_payload(fresh)


@app.get("/api/v1/sessions/{session_id}/download", tags=["sessions"])
def download_session(
    session_id: str,
    request: Request,
) -> Response:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT filename FROM upload_sessions WHERE id = ? AND status = 'completed'",
        (session_id,),
    ).fetchone()
    if row is None:
        raise _http(404, "no completed artifact for this session")
    # 隔离会话拒绝下载：成品未通过完整性巡检，不得再对外分发。
    # 标记优先于成品存在性判定：无论文件是否还在，隔离都是拒绝的权威原因。
    mark = integrity.get_mark(conn, session_id)
    if mark is not None:
        raise _http(
            423,
            "session quarantined",
            reason=mark["reason"],
            quarantined_at=mark["created_at"],
        )
    path = storage.artifact_path(session_id)
    if not path.is_file():
        raise _http(404, "no completed artifact for this session")
    # 断点下载：无 Range 返回完整文件（200）；单个合法 Range 返回 206，
    # 多范围/越界返回 416（不读文件内容）。本接口不计数。
    return ranges.build_download_response(
        path,
        storage.safe_filename(row["filename"]),
        request.headers.get("range"),
    )


# ---------------------------------------------------------------------------
# 成品完整性巡检（POST /integrity-check）与隔离
#
# 对已完成会话的成品流式计算 SHA-256，核对声明长度（total_size）与预期摘要
# （sha256_expected）：
#   * 校验通过：返回实际长度与摘要，不改变会话的任何状态（也不清除已有
#     隔离标记——已完成会话的成品不可变，标记只能随会话删除消失）；
#   * 文件缺失 / 长度不符 / 摘要不符：在会话锁内的单个 IMMEDIATE 事务中
#     持久化隔离标记（quarantine_marks，每会话至多一条，重复巡检 UPSERT
#     刷新），并返回具体原因。被隔离会话的会话下载、票据签发与凭票下载
#     一律拒绝（423）；凭票下载的判定在原子扣减之前，拒绝绝不扣减次数。
# 巡检全程持有会话锁，与删除会话（DELETE /sessions/{id}）按同一把锁串行：
# 删除先则巡检看到 404、不写任何标记；巡检先则删除排队，随后删除时标记
# 经 ON DELETE CASCADE 与会话行同事务清除，不可能为已删除会话重新写入。
# ---------------------------------------------------------------------------


def _integrity_check(session_id: str) -> dict:
    """持会话锁完成成品巡检；失败时在同一锁内持久化隔离标记。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] != "completed":
            raise _http(409, "session is not completed; no artifact to inspect")

        checked_at = storage.iso(storage.utcnow())
        disk = storage.hash_artifact_file(session_id)
        status = integrity.evaluate(
            disk, row["total_size"], row["sha256_expected"]
        )
        size_on_disk, actual_sha256 = disk if disk is not None else (None, None)

        if status == integrity.OK:
            # 校验通过：只读返回，不改变会话（已有隔离标记也原样保留）。
            mark = integrity.get_mark(db.get_conn(), session_id)
            return {
                "session_id": session_id,
                "status": status,
                "quarantined": mark is not None,
                "declared_size": row["total_size"],
                "size_on_disk": size_on_disk,
                "expected_sha256": row["sha256_expected"],
                "actual_sha256": actual_sha256,
                "quarantined_at": mark["created_at"] if mark is not None else None,
                "checked_at": checked_at,
            }

        # 失败：持锁在单个 IMMEDIATE 事务内持久化隔离标记。事务内复查会话
        # 仍存在且仍为 completed——与删除会话按同一把会话锁串行，这里只是
        # 防御性复核；会话若已删除则抛错回滚，绝不重新写入标记。
        now_iso = storage.iso(storage.utcnow())
        with db.immediate() as tx:
            current = tx.execute(
                "SELECT status FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if current is None:
                raise _http(404, "session not found")
            if current["status"] != "completed":
                raise _http(409, "session is not completed; no artifact to inspect")
            integrity.persist_mark(tx, session_id, status, now_iso)

        return {
            "session_id": session_id,
            "status": status,
            "quarantined": True,
            "declared_size": row["total_size"],
            "size_on_disk": size_on_disk,
            "expected_sha256": row["sha256_expected"],
            "actual_sha256": actual_sha256,
            "quarantined_at": now_iso,
            "checked_at": checked_at,
        }


@app.post(
    "/api/v1/sessions/{session_id}/integrity-check",
    response_model=schemas.IntegrityCheckResponse,
    tags=["sessions"],
)
def integrity_check(session_id: str) -> dict:
    """对已完成会话的成品流式巡检：核对声明长度与预期 SHA-256，异常即隔离。"""
    return _integrity_check(session_id)


# ---------------------------------------------------------------------------
# 受控下载票据
#
# 明文票据 = 32 字节 URL-safe 随机串（约 256 bit 熵），仅在签发响应中返回一次；
# 库里只存 SHA-256(token)。凭票下载在单个 BEGIN IMMEDIATE 事务内完成
# “存在/未撤销/未过期/剩余次数 > 0”全部校验与原子扣减，
# 条件 UPDATE 配合行级写锁保证并发请求不可能突破次数上限。
# ---------------------------------------------------------------------------


def _ticket_payload(row: sqlite3.Row) -> dict:
    return {
        "ticket_id": row["id"],
        "session_id": row["session_id"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "max_downloads": row["max_downloads"],
        "remaining_downloads": row["remaining_downloads"],
        "revoked": bool(row["revoked"]),
        "download_url": f"/api/v1/tickets/{row['id']}/download",
    }


@app.post(
    "/api/v1/sessions/{session_id}/tickets",
    status_code=201,
    response_model=schemas.TicketResponse,
    tags=["tickets"],
)
def create_ticket(
    session_id: str, body: schemas.CreateTicketRequest
) -> dict:
    now = storage.utcnow()
    now_iso = storage.iso(now)
    expires_iso = storage.iso(
        now + timedelta(minutes=body.expires_in_minutes)
    )

    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    ticket_id = uuid.uuid4().hex

    # 会话状态/成品检查与票据登记放在同一个 IMMEDIATE 事务，
    # 票据外键指向 completed 会话；不存在成品则不签发。
    with db.immediate() as tx:
        row = tx.execute(
            "SELECT id FROM upload_sessions WHERE id = ? AND status = 'completed'",
            (session_id,),
        ).fetchone()
        if row is None:
            raise _http(409, "no completed artifact for this session")
        # 隔离会话拒绝签发新票据：成品未通过完整性巡检。
        mark = integrity.get_mark(tx, session_id)
        if mark is not None:
            raise _http(
                423,
                "session quarantined",
                reason=mark["reason"],
                quarantined_at=mark["created_at"],
            )
        if not storage.artifact_path(session_id).is_file():
            raise _http(409, "no completed artifact for this session")
        tx.execute(
            "INSERT INTO download_tickets ("
            "id, session_id, token_hash, max_downloads, remaining_downloads, "
            "revoked, created_at, expires_at"
            ") VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
            (
                ticket_id,
                session_id,
                token_hash,
                body.max_downloads,
                body.max_downloads,
                now_iso,
                expires_iso,
            ),
        )
        saved = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket_id,)
        ).fetchone()

    payload = _ticket_payload(saved)
    # 票据随机串本身才是下载凭证；id 只用于拼 URL。
    payload["ticket"] = f"{ticket_id}.{token}"
    return payload


def _claim_ticket_download(
    credential: str, raw_range: str | None
) -> tuple[sqlite3.Row, sqlite3.Row]:
    """在单个 IMMEDIATE 事务中校验票据、规范化 Range 并原子扣减一次。

    返回 (票据行, 会话行)。任一校验失败都抛 HTTPException 且事务回滚，
    绝不扣减次数。条件 UPDATE 的 rowcount 是并发上限的最终防线。
    Range 的语法/可满足性在扣减之前按成品实际长度判定：多范围、越界等
    416 请求与隔离（423）一样只回滚、不扣次；判定只需 stat 长度，
    不读取文件内容。
    """
    ticket_id, _, token = credential.partition(".")
    if not ticket_id or not token:
        raise _http(404, "ticket not found")
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    now_iso = storage.iso(storage.utcnow())

    with db.immediate() as tx:
        ticket = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ? AND token_hash = ?",
            (ticket_id, token_hash),
        ).fetchone()
        if ticket is None:
            raise _http(404, "ticket not found")
        if ticket["revoked"]:
            raise _http(403, "ticket revoked")
        if ticket["expires_at"] <= now_iso:
            raise _http(410, "ticket expired")
        if ticket["remaining_downloads"] <= 0:
            raise _http(429, "ticket download limit reached")

        session = tx.execute(
            "SELECT id, filename FROM upload_sessions "
            "WHERE id = ? AND status = 'completed'",
            (ticket["session_id"],),
        ).fetchone()
        if session is None:
            raise _http(404, "no completed artifact for this session")

        # 隔离会话拒绝凭票下载：判定在扣减之前，抛错即回滚，绝不扣减次数。
        mark = integrity.get_mark(tx, ticket["session_id"])
        if mark is not None:
            raise _http(
                423,
                "session quarantined",
                reason=mark["reason"],
                quarantined_at=mark["created_at"],
            )

        path = storage.artifact_path(ticket["session_id"])
        if not path.is_file():
            raise _http(404, "no completed artifact for this session")

        # Range 在扣减之前解析：多范围/越界（416）与其他拒绝一样回滚，
        # 绝不扣减次数。这里只取长度做规范化，不打开文件读取内容。
        ranges.parse_range_header(raw_range, path.stat().st_size)

        # 原子扣减：WHERE 重放全部前置条件，两个并发请求在此串行，
        # 后到者的 remaining_downloads 已是 0，rowcount=0 → 429，绝不超额。
        cur = tx.execute(
            "UPDATE download_tickets SET remaining_downloads = remaining_downloads - 1 "
            "WHERE id = ? AND token_hash = ? AND revoked = 0 "
            "AND expires_at > ? AND remaining_downloads > 0",
            (ticket["id"], token_hash, now_iso),
        )
        if cur.rowcount != 1:
            raise _http(429, "ticket download limit reached")
        ticket = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket["id"],)
        ).fetchone()
    return ticket, session


@app.get("/api/v1/tickets/{ticket}/download", tags=["tickets"])
def download_with_ticket(ticket: str, request: Request) -> Response:
    raw_range = request.headers.get("range")
    _, session = _claim_ticket_download(ticket, raw_range)
    session_id = session["id"]
    # 校验、Range 规范化与原子扣减已在同一事务中完成：合法 Range 按一次
    # 计数（下载器断点续传消耗一次），范围非法（416）/隔离（423）等拒绝
    # 在扣减前抛错，不计数。此处按已校验的 Range 流式输出对应字节。
    return ranges.build_download_response(
        storage.artifact_path(session_id),
        storage.safe_filename(session["filename"]),
        raw_range,
    )


@app.post(
    "/api/v1/tickets/{ticket}/revoke",
    response_model=schemas.TicketInfo,
    tags=["tickets"],
)
def revoke_ticket(ticket: str) -> dict:
    ticket_id, _, token = ticket.partition(".")
    if not ticket_id or not token:
        raise _http(404, "ticket not found")
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()

    with db.immediate() as tx:
        row = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ? AND token_hash = ?",
            (ticket_id, token_hash),
        ).fetchone()
        if row is None:
            raise _http(404, "ticket not found")
        # 幂等：已撤销不报错。
        tx.execute(
            "UPDATE download_tickets SET revoked = 1 WHERE id = ?", (ticket_id,)
        )
        row = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket_id,)
        ).fetchone()
    return _ticket_payload(row)


# ---------------------------------------------------------------------------
# 运维只读汇总
#
# 仅在单个 SQLite 只读快照中做聚合：不调用 _load_session（不触发懒过期）、
# 不更新 last_access_at、不滑动 TTL，也完全不接触文件系统。
# ---------------------------------------------------------------------------

# 固定四种状态及其输出顺序；缺失状态以零补齐，保证响应恒为四行。
_SUMMARY_STATUSES = ("uploading", "completed", "aborted", "expired")


@app.get(
    "/api/v1/storage/summary",
    response_model=schemas.StorageSummaryResponse,
    tags=["ops"],
)
def storage_summary() -> dict:
    # 单条 GROUP BY 语句在只读连接上一次执行完成，四行计数取自同一快照；
    # SQLite 默认每条语句隐式包裹自己的读事务，读期间看到一致的数据库版本。
    stats: dict[str, dict[str, int]] = {
        status: {"session_count": 0, "declared_bytes": 0, "stored_bytes": 0}
        for status in _SUMMARY_STATUSES
    }
    with db.read_only() as conn:
        rows = conn.execute(
            "SELECT status, "
            "COUNT(*) AS session_count, "
            "COALESCE(SUM(total_size), 0) AS declared_bytes, "
            # stored_bytes 仅按状态语义取字节数：
            # uploading 取已收字节，completed 取成品总大小，其余状态恒为 0。
            "COALESCE(SUM(CASE status "
            "WHEN 'uploading' THEN bytes_received "
            "WHEN 'completed' THEN total_size "
            "ELSE 0 END), 0) AS stored_bytes "
            "FROM upload_sessions GROUP BY status"
        ).fetchall()

    for row in rows:
        stats[row["status"]] = {
            "session_count": row["session_count"],
            "declared_bytes": row["declared_bytes"],
            "stored_bytes": row["stored_bytes"],
        }

    statuses = [
        {
            "status": status,
            "session_count": stats[status]["session_count"],
            "declared_bytes": stats[status]["declared_bytes"],
            "stored_bytes": stats[status]["stored_bytes"],
        }
        for status in _SUMMARY_STATUSES
    ]
    total = {
        "status": "total",
        "session_count": sum(item["session_count"] for item in statuses),
        "declared_bytes": sum(item["declared_bytes"] for item in statuses),
        "stored_bytes": sum(item["stored_bytes"] for item in statuses),
    }
    return {"statuses": statuses, "total": total}


# ---------------------------------------------------------------------------
# 健康检查与文档
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")
