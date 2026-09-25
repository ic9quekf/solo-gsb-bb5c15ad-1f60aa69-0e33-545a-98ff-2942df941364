"""受控下载票据测试：签发/凭票下载/撤销/并发上限/迁移。"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from pathlib import Path

from fastapi.testclient import TestClient

from app import db, settings, storage


def _completed_session(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    """上传一份完整文件并完成，返回 session_id。"""
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        },
    )
    assert resp.status_code == 201, resp.text
    sid = resp.json()["session_id"]
    if data:
        r = client.put(
            f"/api/v1/sessions/{sid}/chunks?start=0&end={len(data)}", content=data
        )
        assert r.status_code == 200, r.text
    done = client.post(f"/api/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    return sid


def _issue(client: TestClient, sid: str, *, minutes: int = 60, max_downloads: int = 1):
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": minutes, "max_downloads": max_downloads},
    )
    assert r.status_code == 201, r.text
    return r.json()


def test_issue_ticket_validates_bounds(client: TestClient) -> None:
    sid = _completed_session(client, b"hello")

    # 越界参数由 Pydantic 拒绝（1～1440 分钟、1～100 次）。
    for payload in (
        {"expires_in_minutes": 0, "max_downloads": 1},
        {"expires_in_minutes": 1441, "max_downloads": 1},
        {"expires_in_minutes": 60, "max_downloads": 0},
        {"expires_in_minutes": 60, "max_downloads": 101},
    ):
        r = client.post(f"/api/v1/sessions/{sid}/tickets", json=payload)
        assert r.status_code == 422, payload

    # 边界值合法。
    body = _issue(client, sid, minutes=1, max_downloads=1)
    assert body["max_downloads"] == 1
    assert body["remaining_downloads"] == 1
    assert body["revoked"] is False
    assert body["ticket"].count(".") == 1
    ticket_id, token = body["ticket"].split(".")
    assert ticket_id == body["ticket_id"]
    assert body["download_url"] == f"/api/v1/tickets/{ticket_id}/download"
    # 到期时间约在 1 分钟后（时间戳按秒存储）。
    created = storage.parse_iso(body["created_at"])
    expiry = storage.parse_iso(body["expires_at"])
    assert 59 <= (expiry - created).total_seconds() <= 61


def test_issue_requires_completed_session(client: TestClient) -> None:
    # uploading 会话不允许签票。
    resp = client.post(
        "/api/v1/sessions",
        json={"filename": "x", "total_size": 1, "sha256": "a" * 64},
    )
    sid = resp.json()["session_id"]
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 1},
    )
    assert r.status_code == 409

    # 不存在的会话。
    r = client.post(
        "/api/v1/sessions/deadbeef/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 1},
    )
    assert r.status_code == 409


def test_server_only_stores_token_hash(client: TestClient) -> None:
    data = b"hash me"
    sid = _completed_session(client, data)
    body = _issue(client, sid, max_downloads=2)
    _, token = body["ticket"].split(".")

    conn = db.get_conn()
    rows = conn.execute("SELECT token_hash FROM download_tickets").fetchall()
    stored = [r["token_hash"] for r in rows]
    # 明文绝不出现在数据库中。
    assert token not in stored
    assert hashlib.sha256(token.encode("ascii")).hexdigest() in stored


def test_ticket_download_decrements_once_per_request(client: TestClient) -> None:
    data = bytes(range(256)) * 40  # 10 KiB
    sid = _completed_session(client, data)
    body = _issue(client, sid, max_downloads=3)
    url = f"/api/v1/tickets/{body['ticket']}/download"

    r1 = client.get(url)
    assert r1.status_code == 200
    assert r1.content == data
    assert "attachment" in r1.headers["content-disposition"]

    # Range 请求同样按一次计数，返回 206 与对应片段。
    r2 = client.get(url, headers={"Range": "bytes=10-19"})
    assert r2.status_code == 206
    assert r2.content == data[10:20]

    # 还剩最后一次。
    r3 = client.get(url)
    assert r3.status_code == 200
    assert r3.content == data

    # 第 4 次被拒，且不扣减（剩余保持 0）。
    r4 = client.get(url)
    assert r4.status_code == 429
    assert r4.json()["detail"]["error"] == "ticket download limit reached"
    r5 = client.get(url)
    assert r5.status_code == 429

    conn = db.get_conn()
    row = conn.execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (body["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 0


def test_revoked_ticket_rejected_without_decrement(client: TestClient) -> None:
    sid = _completed_session(client, b"revoke me")
    body = _issue(client, sid, max_downloads=2)
    ticket = body["ticket"]

    r = client.post(f"/api/v1/tickets/{ticket}/revoke")
    assert r.status_code == 200
    assert r.json()["revoked"] is True
    assert r.json()["remaining_downloads"] == 2  # 撤销不消耗次数

    # 撤销后下载被拒，次数不变。
    r = client.get(f"/api/v1/tickets/{ticket}/download")
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "ticket revoked"
    conn = db.get_conn()
    row = conn.execute(
        "SELECT remaining_downloads, revoked FROM download_tickets WHERE id = ?",
        (body["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 2
    assert row["revoked"] == 1

    # 重复撤销幂等。
    again = client.post(f"/api/v1/tickets/{ticket}/revoke")
    assert again.status_code == 200
    assert again.json()["revoked"] is True


def test_expired_ticket_rejected_without_decrement(client: TestClient) -> None:
    sid = _completed_session(client, b"old ticket")
    body = _issue(client, sid, minutes=1, max_downloads=1)

    # 直接把到期时间改到过去，模拟过期（测试无需真的等待）。
    conn = db.get_conn()
    conn.execute(
        "UPDATE download_tickets SET expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", body["ticket_id"]),
    )

    r = client.get(f"/api/v1/tickets/{body['ticket']}/download")
    assert r.status_code == 410
    assert r.json()["detail"]["error"] == "ticket expired"
    row = conn.execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (body["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 1


def test_unknown_or_garbled_ticket_returns_404(client: TestClient) -> None:
    assert client.get("/api/v1/tickets/not-a-ticket/download").status_code == 404
    assert client.post("/api/v1/tickets/not-a-ticket/revoke").status_code == 404

    sid = _completed_session(client, b"x")
    body = _issue(client, sid, max_downloads=1)
    ticket_id, token = body["ticket"].split(".")
    # 篡改随机串 → 哈希不匹配，等同不存在，且不得扣减。
    forged = f"{ticket_id}.{'a' if token[0] != 'a' else 'b'}{token[1:]}"
    r = client.get(f"/api/v1/tickets/{forged}/download")
    assert r.status_code == 404
    conn = db.get_conn()
    row = conn.execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()
    assert row["remaining_downloads"] == 1


def test_concurrent_downloads_cannot_exceed_limit(client: TestClient) -> None:
    data = b"z" * 4096
    sid = _completed_session(client, data)
    body = _issue(client, sid, max_downloads=10)
    url = f"/api/v1/tickets/{body['ticket']}/download"

    results: list[int] = []
    barrier = threading.Barrier(30)

    def worker() -> None:
        barrier.wait()  # 放大同一时刻的竞态窗口
        r = client.get(url)
        results.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == 10
    assert results.count(429) == 20
    conn = db.get_conn()
    row = conn.execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (body["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 0


def test_independent_tickets_have_independent_quota(client: TestClient) -> None:
    sid = _completed_session(client, b"quota")
    t1 = _issue(client, sid, max_downloads=1)
    t2 = _issue(client, sid, max_downloads=1)

    assert client.get(f"/api/v1/tickets/{t1['ticket']}/download").status_code == 200
    assert client.get(f"/api/v1/tickets/{t1['ticket']}/download").status_code == 429
    # 另一张票据不受影响。
    assert client.get(f"/api/v1/tickets/{t2['ticket']}/download").status_code == 200
    assert client.get(f"/api/v1/tickets/{t2['ticket']}/download").status_code == 429


def test_legacy_session_download_unchanged_and_uncounted(client: TestClient) -> None:
    data = b"legacy download"
    sid = _completed_session(client, data)
    # 原有会话下载接口不受票据影响：可无限次下载，无需票据。
    for _ in range(3):
        r = client.get(f"/api/v1/sessions/{sid}/download")
        assert r.status_code == 200
        assert r.content == data


def test_deleting_session_cascades_ticket(client: TestClient) -> None:
    sid = _completed_session(client, b"bye")
    body = _issue(client, sid, max_downloads=5)
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    r = client.get(f"/api/v1/tickets/{body['ticket']}/download")
    assert r.status_code == 404
    conn = db.get_conn()
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM download_tickets WHERE id = ?",
            (body["ticket_id"],),
        ).fetchone()["n"]
        == 0
    )


def test_empty_file_ticket_download(client: TestClient) -> None:
    sid = _completed_session(client, b"", name="empty.txt")
    body = _issue(client, sid, max_downloads=2)
    r = client.get(f"/api/v1/tickets/{body['ticket']}/download")
    assert r.status_code == 200
    assert r.content == b""
    conn = db.get_conn()
    row = conn.execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (body["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 1


def test_init_db_migrates_legacy_v1_database(monkeypatch, tmp_path: Path) -> None:
    """老结构（无 download_tickets、user_version=0 的历史库）可平滑升级。"""
    db_file = tmp_path / "legacy.db"
    raw = sqlite3.connect(str(db_file))
    raw.execute(
        "CREATE TABLE upload_sessions ("
        "id TEXT PRIMARY KEY, filename TEXT NOT NULL, total_size INTEGER NOT NULL, "
        "sha256_expected TEXT NOT NULL, chunk_size INTEGER NOT NULL, status TEXT NOT NULL, "
        "bytes_received INTEGER NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL, "
        "last_access_at TEXT NOT NULL, completed_at TEXT)"
    )
    raw.execute(
        "INSERT INTO upload_sessions VALUES ("
        "'sid1','f.bin',10,'" + "a" * 64 + "',4,'completed',10,"
        "'2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','2026-01-01T00:00:00Z',"
        "'2026-01-01T00:00:00Z')"
    )
    raw.commit()
    raw.close()

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    monkeypatch.setattr(settings, "database_url", f"sqlite:///{db_file}")
    # 丢弃可能存在的线程缓存连接，强制按新 DATABASE_URL 建连。
    db._local.conn = None
    try:
        db.init_db()
        conn = db.get_conn()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
        # 老数据保留。
        assert (
            conn.execute(
                "SELECT filename FROM upload_sessions WHERE id = 'sid1'"
            ).fetchone()["filename"]
            == "f.bin"
        )
        # 新表可用，且再次初始化幂等。
        conn.execute(
            "INSERT INTO download_tickets ("
            "id, session_id, token_hash, max_downloads, remaining_downloads, "
            "revoked, created_at, expires_at) "
            "VALUES ('t1','sid1','" + "b" * 64 + "',1,1,0,"
            "'2026-01-01T00:00:00Z','2026-02-01T00:00:00Z')"
        )
        db.init_db()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
    finally:
        db._local.conn = None
