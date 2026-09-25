"""成品完整性巡检与隔离标记测试：巡检结论、隔离拒绝、删除清理与迁移。"""
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


def _mark_row(sid: str):
    return db.get_conn().execute(
        "SELECT * FROM quarantine_marks WHERE session_id = ?", (sid,)
    ).fetchone()


def test_healthy_artifact_passes_and_changes_nothing(client: TestClient) -> None:
    data = b"integrity ok" * 100
    sid = _completed_session(client, data)
    before = client.get(f"/api/v1/sessions/{sid}").json()

    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["quarantined"] is False
    assert body["quarantined_at"] is None
    assert body["declared_size"] == len(data)
    assert body["size_on_disk"] == len(data)
    assert body["expected_sha256"] == hashlib.sha256(data).hexdigest()
    assert body["actual_sha256"] == hashlib.sha256(data).hexdigest()
    assert body["checked_at"]

    # 校验通过不改变会话：状态查询逐字段一致，下载照常，库中无隔离标记。
    after = client.get(f"/api/v1/sessions/{sid}").json()
    assert after == before
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == data
    assert _mark_row(sid) is None


def test_empty_file_artifact_passes(client: TestClient) -> None:
    sid = _completed_session(client, b"", name="empty.txt")
    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["size_on_disk"] == 0
    assert body["actual_sha256"] == hashlib.sha256(b"").hexdigest()
    assert body["quarantined"] is False


def test_missing_artifact_quarantines_and_blocks_downloads(client: TestClient) -> None:
    data = b"will vanish"
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=2)

    storage.artifact_path(sid).unlink()

    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "missing"
    assert body["quarantined"] is True
    assert body["quarantined_at"]
    assert body["size_on_disk"] is None
    assert body["actual_sha256"] is None
    assert body["declared_size"] == len(data)

    # 隔离标记已持久化。
    mark = _mark_row(sid)
    assert mark is not None
    assert mark["reason"] == "missing"
    assert mark["created_at"] == body["quarantined_at"]

    # 会话下载被拒（423），状态查询不受影响。
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 423
    assert dl.json()["detail"]["error"] == "session quarantined"
    assert dl.json()["detail"]["reason"] == "missing"
    assert client.get(f"/api/v1/sessions/{sid}").status_code == 200

    # 新票据签发被拒。
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 1},
    )
    assert r.status_code == 423
    assert r.json()["detail"]["error"] == "session quarantined"

    # 既有票据凭票下载被拒，且不得扣减次数。
    r = client.get(f"/api/v1/tickets/{ticket['ticket']}/download")
    assert r.status_code == 423
    assert r.json()["detail"]["error"] == "session quarantined"
    row = db.get_conn().execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (ticket["ticket_id"],),
    ).fetchone()
    assert row["remaining_downloads"] == 2

    # 撤销票据等管理操作不受隔离影响。
    assert client.post(f"/api/v1/tickets/{ticket['ticket']}/revoke").status_code == 200


def test_length_mismatch_quarantines(client: TestClient) -> None:
    data = b"original payload" * 10
    sid = _completed_session(client, data)

    # 成品被截断：长度与声明不符。
    artifact = storage.artifact_path(sid)
    artifact.write_bytes(data[:-3])

    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "length_mismatch"
    assert body["quarantined"] is True
    assert body["size_on_disk"] == len(data) - 3
    assert body["actual_sha256"] == hashlib.sha256(data[:-3]).hexdigest()
    assert _mark_row(sid)["reason"] == "length_mismatch"

    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 423
    assert dl.json()["detail"]["reason"] == "length_mismatch"


def test_sha256_mismatch_quarantines(client: TestClient) -> None:
    data = b"x" * 4096
    sid = _completed_session(client, data)

    # 等长替换内容：长度一致但摘要不符。
    corrupted = b"y" * 4096
    storage.artifact_path(sid).write_bytes(corrupted)

    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "sha256_mismatch"
    assert body["quarantined"] is True
    assert body["size_on_disk"] == len(data)
    assert body["expected_sha256"] == hashlib.sha256(data).hexdigest()
    assert body["actual_sha256"] == hashlib.sha256(corrupted).hexdigest()
    assert _mark_row(sid)["reason"] == "sha256_mismatch"


def test_recheck_refreshes_single_mark(client: TestClient) -> None:
    data = b"recheck me"
    sid = _completed_session(client, data)
    storage.artifact_path(sid).unlink()

    first = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert first.status_code == 200
    second = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert second.status_code == 200
    assert second.json()["status"] == "missing"
    assert second.json()["quarantined"] is True
    # 每会话至多一条标记：重复巡检刷新而非堆叠。
    n = db.get_conn().execute(
        "SELECT COUNT(*) AS n FROM quarantine_marks WHERE session_id = ?", (sid,)
    ).fetchone()["n"]
    assert n == 1


def test_passing_recheck_does_not_clear_mark(client: TestClient) -> None:
    data = b"restore me"
    sid = _completed_session(client, data)
    artifact = storage.artifact_path(sid)
    artifact.write_bytes(b"z" * len(data))

    bad = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert bad.json()["status"] == "sha256_mismatch"
    assert bad.json()["quarantined"] is True

    # 运维在磁盘上修复成品后复检通过：返回实际长度与摘要，
    # 但不改变会话——已有隔离标记保留，下载仍被拒绝。
    artifact.write_bytes(data)
    good = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert good.status_code == 200
    body = good.json()
    assert body["status"] == "ok"
    assert body["size_on_disk"] == len(data)
    assert body["actual_sha256"] == hashlib.sha256(data).hexdigest()
    assert body["quarantined"] is True
    assert body["quarantined_at"] == bad.json()["quarantined_at"]
    assert _mark_row(sid) is not None
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 423


def test_check_rejects_non_completed_and_unknown_sessions(client: TestClient) -> None:
    # 上传中会话：无成品可巡检。
    resp = client.post(
        "/api/v1/sessions",
        json={"filename": "x", "total_size": 10, "sha256": "a" * 64},
    )
    sid = resp.json()["session_id"]
    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 409
    assert _mark_row(sid) is None

    # 不存在的会话。
    assert client.post("/api/v1/sessions/deadbeef/integrity-check").status_code == 404

    # 已过期的上传中会话：懒过期 → 410。
    db.get_conn().execute(
        "UPDATE upload_sessions SET expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", sid),
    )
    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 410


def test_delete_session_clears_mark_and_never_rewrites(client: TestClient) -> None:
    sid = _completed_session(client, b"delete me")
    storage.artifact_path(sid).unlink()
    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.json()["quarantined"] is True
    assert _mark_row(sid) is not None

    # 删除会话清除隔离标记。
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert _mark_row(sid) is None

    # 已删除会话的巡检得到 404，且不会重新写入标记。
    r = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert r.status_code == 404
    assert _mark_row(sid) is None


def test_integrity_check_and_delete_serialize_on_session_lock(
    client: TestClient, monkeypatch
) -> None:
    data = b"serialize me"
    sid = _completed_session(client, data)
    # 成品变长 → 巡检走向隔离写库路径。
    storage.artifact_path(sid).write_bytes(data + b"corrupted")

    entered = threading.Event()
    release = threading.Event()
    real_hash = storage.hash_artifact_file

    def slow_hash(session_id: str):
        entered.set()
        assert release.wait(timeout=5)
        return real_hash(session_id)

    monkeypatch.setattr(storage, "hash_artifact_file", slow_hash)

    check_resp: dict = {}

    def do_check() -> None:
        check_resp["resp"] = client.post(f"/api/v1/sessions/{sid}/integrity-check")

    t = threading.Thread(target=do_check)
    t.start()
    assert entered.wait(timeout=5)  # 巡检已持有会话锁、正在读取成品

    delete_done = threading.Event()

    def do_delete() -> None:
        client.delete(f"/api/v1/sessions/{sid}")
        delete_done.set()

    d = threading.Thread(target=do_delete)
    d.start()
    # 同一把会话锁：巡检未放行前删除必须排队，不能完成。
    assert not delete_done.wait(timeout=0.3)
    release.set()
    t.join(timeout=5)
    d.join(timeout=5)

    resp = check_resp["resp"]
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "length_mismatch"
    assert resp.json()["quarantined"] is True
    assert delete_done.is_set()

    # 删除最终生效：会话 404，隔离标记级联清除且未被重写。
    assert client.get(f"/api/v1/sessions/{sid}").status_code == 404
    assert _mark_row(sid) is None


def test_init_db_migrates_legacy_v5_database(monkeypatch, tmp_path: Path) -> None:
    """v5 结构的老库可平滑升级到 v6（quarantine_marks），老数据保留。"""
    db_file = tmp_path / "legacy-v5.db"
    raw = sqlite3.connect(str(db_file))
    for step in range(1, 6):
        raw.executescript(db._MIGRATIONS[step])
    raw.execute("PRAGMA user_version = 5")
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
        # 新表可用：写入并读回隔离标记。
        conn.execute(
            "INSERT INTO quarantine_marks (session_id, reason, created_at) "
            "VALUES ('sid1', 'sha256_mismatch', '2026-01-03T00:00:00Z')"
        )
        mark = conn.execute(
            "SELECT reason FROM quarantine_marks WHERE session_id = 'sid1'"
        ).fetchone()
        assert mark["reason"] == "sha256_mismatch"
        # 再次初始化幂等。
        db.init_db()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
    finally:
        db._local.conn = None
