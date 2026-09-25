"""运维只读汇总接口 GET /api/v1/storage/summary 的测试。"""
from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient

from app import db


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _create(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": _sha(data),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def _put(client: TestClient, sid: str, start: int, end: int, data: bytes) -> None:
    resp = client.put(
        f"/api/v1/sessions/{sid}/chunks?start={start}&end={end}",
        content=data,
    )
    assert resp.status_code == 200, resp.text


def _insert_status_row(
    status: str,
    total_size: int,
    bytes_received: int,
    expires_at: str = "2099-01-01T00:00:00Z",
) -> str:
    """绕过 HTTP 流程直接插入一条会话（用于构造 aborted/expired 等状态）。"""
    import uuid

    sid = uuid.uuid4().hex
    ts = "2026-01-01T00:00:00Z"
    conn = db.get_conn()
    conn.execute(
        "INSERT INTO upload_sessions ("
        "id, filename, total_size, sha256_expected, chunk_size, "
        "status, bytes_received, created_at, expires_at, last_access_at"
        ") VALUES (?, 'x.bin', ?, ?, 0, ?, ?, ?, ?, ?)",
        (sid, total_size, "a" * 64, status, bytes_received, ts, expires_at, ts),
    )
    return sid


def _summary(client: TestClient) -> dict:
    resp = client.get("/api/v1/storage/summary")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _by_status(body: dict) -> dict:
    return {row["status"]: row for row in body["statuses"]}


def test_empty_or_unknown_states_still_have_four_rows(client: TestClient) -> None:
    """没有任何会话（或缺少某状态）时仍固定返回四种状态的零行。"""
    body = _summary(client)
    rows = body["statuses"]

    # 固定四种状态、固定顺序。
    assert [r["status"] for r in rows] == [
        "uploading",
        "completed",
        "aborted",
        "expired",
    ]
    for row in rows:
        assert set(row) == {
            "status",
            "session_count",
            "declared_bytes",
            "stored_bytes",
        }

    # aborted 状态正常流程不可见（删除接口硬删除），该状态行必须仍以零存在。
    aborted = _by_status(body)["aborted"]
    assert aborted == {
        "status": "aborted",
        "session_count": 0,
        "declared_bytes": 0,
        "stored_bytes": 0,
    }

    # 总计行：无论当前数据如何，等于四个状态行之和。
    assert body["total"]["status"] == "total"
    assert body["total"]["session_count"] == sum(
        r["session_count"] for r in rows
    )
    assert body["total"]["declared_bytes"] == sum(
        r["declared_bytes"] for r in rows
    )
    assert body["total"]["stored_bytes"] == sum(
        r["stored_bytes"] for r in rows
    )


def test_summary_aggregates_all_four_statuses(client: TestClient) -> None:
    before = _summary(client)

    # uploading：声明 100，已收 60 → stored 60
    up = _create(client, b"x" * 100, name="up.bin")
    _put(client, up, 0, 60, b"x" * 60)

    # 另一个尚无分片的 uploading：声明 10，已收 0
    _create(client, b"y" * 10, name="empty-up.bin")

    # completed：声明 10 且全部收完 → stored 10
    comp_data = b"z" * 10
    comp = _create(client, comp_data, name="comp.bin")
    _put(client, comp, 0, 10, comp_data)
    done = client.post(f"/api/v1/sessions/{comp}/complete")
    assert done.status_code == 200, done.text

    # aborted：直接构造（正常接口硬删除，仅库中可能存在历史行），stored 恒 0
    _insert_status_row("aborted", total_size=30, bytes_received=25)

    # expired：直接构造，stored 恒 0
    _insert_status_row("expired", total_size=50, bytes_received=40)

    body = _summary(client)
    rows = _by_status(body)

    def delta(status: str, field: str) -> int:
        return rows[status][field] - _by_status(before)[status][field]

    # uploading：2 个会话，声明 100+10=110，已收 60+0=60
    assert delta("uploading", "session_count") == 2
    assert delta("uploading", "declared_bytes") == 110
    assert delta("uploading", "stored_bytes") == 60

    # completed：1 个，声明/存储均为 10
    assert delta("completed", "session_count") == 1
    assert delta("completed", "declared_bytes") == 10
    assert delta("completed", "stored_bytes") == 10

    # aborted / expired：stored 一律 0，无论 bytes_received 是多少
    assert delta("aborted", "session_count") == 1
    assert delta("aborted", "declared_bytes") == 30
    assert delta("aborted", "stored_bytes") == 0

    assert delta("expired", "session_count") == 1
    assert delta("expired", "declared_bytes") == 50
    assert delta("expired", "stored_bytes") == 0

    # 总计由四个状态行求和
    total = body["total"]
    assert total["session_count"] == sum(
        r["session_count"] for r in body["statuses"]
    )
    assert total["declared_bytes"] == sum(
        r["declared_bytes"] for r in body["statuses"]
    )
    assert total["stored_bytes"] == sum(
        r["stored_bytes"] for r in body["statuses"]
    )
    assert total["status"] == "total"


def test_summary_is_read_only(client: TestClient) -> None:
    """只读快照：不更新 last_access_at、不滑动 TTL、不做懒过期。"""
    before = _summary(client)

    # 构造一个已过 expires_at 但尚未被后台清理的 uploading 会话。
    stale_expires = "2000-01-01T00:00:00Z"
    stale_last = "2000-01-01T00:00:00Z"
    sid = uuid_hex()
    conn = db.get_conn()
    conn.execute(
        "INSERT INTO upload_sessions ("
        "id, filename, total_size, sha256_expected, chunk_size, "
        "status, bytes_received, created_at, expires_at, last_access_at"
        ") VALUES (?, 'stale.bin', 80, ?, 0, 'uploading', 70, ?, ?, ?)",
        (sid, "a" * 64, stale_last, stale_expires, stale_last),
    )

    # 再取一个正常 uploading 的 last_access_at 作对照。
    up = _create(client, b"m" * 100, name="ro.bin")
    _put(client, up, 0, 60, b"m" * 60)
    before_access = db.get_conn().execute(
        "SELECT last_access_at, expires_at FROM upload_sessions WHERE id = ?",
        (up,),
    ).fetchone()

    body = _summary(client)
    rows = _by_status(body)

    def delta(status: str, field: str) -> int:
        return rows[status][field] - _by_status(before)[status][field]

    # 已过期但仍标记 uploading 的会话：不被懒过期翻转，仍计入 uploading，
    # stored 取其 bytes_received=70；expired 计数不因它而增加。
    assert delta("expired", "session_count") == 0
    assert delta("uploading", "session_count") == 2
    assert delta("uploading", "declared_bytes") == 80 + 100
    assert delta("uploading", "stored_bytes") == 70 + 60

    # last_access_at / expires_at 完全不变。
    after_access = db.get_conn().execute(
        "SELECT last_access_at, expires_at FROM upload_sessions WHERE id = ?",
        (up,),
    ).fetchone()
    assert tuple(after_access) == tuple(before_access)

    stale = db.get_conn().execute(
        "SELECT status, last_access_at, expires_at FROM upload_sessions WHERE id = ?",
        (sid,),
    ).fetchone()
    assert tuple(stale) == ("uploading", stale_last, stale_expires)


def uuid_hex() -> str:
    import uuid

    return uuid.uuid4().hex
