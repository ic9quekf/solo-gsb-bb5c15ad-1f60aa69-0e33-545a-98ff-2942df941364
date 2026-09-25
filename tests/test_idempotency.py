"""创建会话幂等键测试：重放/冲突/格式校验/只存哈希/并发/级联/迁移。"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from pathlib import Path

from fastapi.testclient import TestClient

from app import db, settings


def _body(
    name: str = "data.bin",
    size: int = 100,
    sha: str | None = None,
    chunk: int = 0,
) -> dict:
    return {
        "filename": name,
        "total_size": size,
        "sha256": sha if sha is not None else hashlib.sha256(b"x" * size).hexdigest(),
        "chunk_size": chunk,
    }


def _post(client: TestClient, payload: dict, key: str | bytes | None = None):
    headers = {"Idempotency-Key": key} if key is not None else {}
    return client.post("/api/v1/sessions", json=payload, headers=headers)


def _session_count() -> int:
    return db.get_conn().execute(
        "SELECT COUNT(*) AS n FROM upload_sessions"
    ).fetchone()["n"]


def _key_row_count(key: str) -> int:
    khash = hashlib.sha256(key.encode("ascii")).hexdigest()
    return db.get_conn().execute(
        "SELECT COUNT(*) AS n FROM idempotency_keys WHERE key_hash = ?", (khash,)
    ).fetchone()["n"]


def test_same_key_same_request_replays_session(client: TestClient) -> None:
    payload = _body(name="replay-same.bin")
    r1 = _post(client, payload, key="order-0001")
    assert r1.status_code == 201, r1.text
    before = _session_count()

    # 模拟“首次响应丢失后重试”：同键同请求必须返回同一会话，状态码仍是 201。
    r2 = _post(client, payload, key="order-0001")
    assert r2.status_code == 201, r2.text
    assert r2.json()["session_id"] == r1.json()["session_id"]
    assert r2.json()["created_at"] == r1.json()["created_at"]
    assert _session_count() == before  # 没有新建会话

    # 语义相同的请求（sha256 大写、带 sha256: 前缀）指纹一致，同样重放。
    variant = dict(payload)
    variant["sha256"] = "sha256:" + payload["sha256"].upper()
    r3 = _post(client, variant, key="order-0001")
    assert r3.status_code == 201
    assert r3.json()["session_id"] == r1.json()["session_id"]
    assert _session_count() == before


def test_same_key_different_request_conflicts(client: TestClient) -> None:
    key = "conflict-key-1"
    base = _body(name="conflict-a.bin")
    assert _post(client, base, key=key).status_code == 201
    before = _session_count()

    mutated_cases = (
        _body(name="conflict-b.bin"),  # 不同 filename
        {**base, "total_size": base["total_size"] + 1},  # 不同 total_size
        {**base, "sha256": "0" * 64},  # 不同 sha256
        {**base, "chunk_size": 128},  # 不同 chunk_size
    )
    for mutated in mutated_cases:
        r = _post(client, mutated, key=key)
        assert r.status_code == 409, (mutated, r.text)
        assert "different request" in r.json()["detail"]["error"]
    # 冲突不得新建任何会话。
    assert _session_count() == before


def test_different_keys_create_distinct_sessions(client: TestClient) -> None:
    payload = _body(name="distinct.bin")
    r1 = _post(client, payload, key="key-one")
    r2 = _post(client, payload, key="key-two")
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["session_id"] != r2.json()["session_id"]


def test_missing_key_keeps_old_behavior(client: TestClient) -> None:
    payload = _body(name="nokey.bin")
    r1 = _post(client, payload)
    r2 = _post(client, payload)
    assert r1.status_code == r2.status_code == 201
    # 不带幂等键时行为不变：每次都创建新会话。
    assert r1.json()["session_id"] != r2.json()["session_id"]


def test_key_format_validation(client: TestClient) -> None:
    payload = _body(name="fmt.bin")
    before = _session_count()
    # 空、超长（129）、含空格、含制表符、含 DEL、非 ASCII → 400，且不建会话。
    bad_keys: list[str | bytes] = [
        "",
        "k" * 129,
        "has space",
        "tab\there",
        "del\x7fhere",
        "中文键".encode("utf-8"),
    ]
    for bad in bad_keys:
        r = _post(client, payload, key=bad)
        assert r.status_code == 400, (bad, r.status_code)
        assert "Idempotency-Key" in r.json()["detail"]["error"]
    assert _session_count() == before

    # 边界：1 个与 128 个可见 ASCII 均合法。
    assert _post(client, payload, key="k").status_code == 201
    assert _post(client, payload, key="k" * 128).status_code == 201


def test_server_stores_only_hashes(client: TestClient) -> None:
    key = "super-secret-key-123"
    payload = _body(name="hashonly.bin")
    r = _post(client, payload, key=key)
    assert r.status_code == 201, r.text

    conn = db.get_conn()
    rows = conn.execute(
        "SELECT key_hash, session_id, request_fingerprint FROM idempotency_keys"
    ).fetchall()
    # 明文键绝不出现在库中；保存的是它的 SHA-256。
    for row in rows:
        assert key not in row["key_hash"]
        assert key not in row["request_fingerprint"]
    khash = hashlib.sha256(key.encode("ascii")).hexdigest()
    stored = {row["key_hash"]: row for row in rows}
    assert khash in stored
    assert stored[khash]["session_id"] == r.json()["session_id"]
    # 指纹同样是哈希，不含明文文件名。
    assert "hashonly" not in stored[khash]["request_fingerprint"]
    assert len(stored[khash]["request_fingerprint"]) == 64


def test_concurrent_same_key_same_request_single_session(client: TestClient) -> None:
    payload = _body(name="race-same.bin")
    key = "race-same-key"
    results: list[tuple[int, str | None]] = []
    barrier = threading.Barrier(10)

    def worker() -> None:
        barrier.wait()  # 放大并发重试的竞态窗口
        r = _post(client, payload, key=key)
        sid = r.json().get("session_id") if r.status_code == 201 else None
        results.append((r.status_code, sid))

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 并发重试：全部 201，且全部拿到同一个 session_id。
    assert all(code == 201 for code, _ in results), results
    assert len({sid for _, sid in results}) == 1
    assert _key_row_count(key) == 1


def test_concurrent_same_key_different_requests_exactly_one_wins(
    client: TestClient,
) -> None:
    key = "race-conflict-key"
    bodies = [_body(name=f"race-{i}.bin") for i in range(8)]
    codes: list[int] = []
    barrier = threading.Barrier(len(bodies))

    def worker(p: dict) -> None:
        barrier.wait()
        codes.append(_post(client, p, key=key).status_code)

    threads = [threading.Thread(target=worker, args=(p,)) for p in bodies]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 同键不同请求并发：恰好一个成功，其余全部 409。
    assert codes.count(201) == 1, codes
    assert codes.count(409) == len(bodies) - 1, codes
    assert _key_row_count(key) == 1


def test_delete_session_cascades_key_record(client: TestClient) -> None:
    payload = _body(name="cascade.bin")
    key = "cascade-key"
    sid = _post(client, payload, key=key).json()["session_id"]
    assert _key_row_count(key) == 1

    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    # 键记录随会话级联删除。
    assert _key_row_count(key) == 0
    # 键已随会话遗忘：同键同请求按全新请求处理，创建新会话。
    r = _post(client, payload, key=key)
    assert r.status_code == 201
    assert r.json()["session_id"] != sid


def test_replay_after_upload_and_complete(client: TestClient) -> None:
    data = b"replay me" * 10
    payload = _body(
        name="replay-done.bin",
        size=len(data),
        sha=hashlib.sha256(data).hexdigest(),
    )
    key = "replay-done-key"
    sid = _post(client, payload, key=key).json()["session_id"]
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end={len(data)}", content=data
    )
    assert r.status_code == 200, r.text
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200

    # 会话完成后重放创建请求，仍返回同一会话。
    replay = _post(client, payload, key=key)
    assert replay.status_code == 201
    assert replay.json()["session_id"] == sid
    assert replay.json()["status"] == "completed"


def test_init_db_migrates_v2_database(monkeypatch, tmp_path: Path) -> None:
    """v2 老库（含 download_tickets）平滑升级到 v3，老数据保留。"""
    db_file = tmp_path / "v2.db"
    raw = sqlite3.connect(str(db_file))
    raw.executescript(db.MIGRATION_1)
    raw.executescript(db.MIGRATION_2)
    raw.execute("PRAGMA user_version = 2")
    raw.execute(
        "INSERT INTO upload_sessions VALUES ("
        "'sid9','f.bin',10,'" + "a" * 64 + "',4,'completed',10,"
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
                "SELECT filename FROM upload_sessions WHERE id = 'sid9'"
            ).fetchone()["filename"]
            == "f.bin"
        )
        # 新表可用，且再次初始化幂等。
        conn.execute(
            "INSERT INTO idempotency_keys ("
            "key_hash, session_id, request_fingerprint, created_at) "
            "VALUES ('" + "c" * 64 + "','sid9','" + "d" * 64 + "',"
            "'2026-01-01T00:00:00Z')"
        )
        db.init_db()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
    finally:
        db._local.conn = None
