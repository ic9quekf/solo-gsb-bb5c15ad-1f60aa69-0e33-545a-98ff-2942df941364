"""分片认领（chunk-claims）：认领/相交/过期重领/令牌消费/会话结束失效/迁移。"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app import db, settings, storage
from app.cleanup import run_cleanup


def _create(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def _claim(client: TestClient, sid: str, start: int, end: int):
    return client.post(
        f"/api/v1/sessions/{sid}/chunk-claims", json={"start": start, "end": end}
    )


def _put(
    client: TestClient,
    sid: str,
    start: int,
    end: int,
    content: bytes,
    claim: str | None = None,
    sha: str | None = None,
):
    headers = {}
    if claim is not None:
        headers["X-Chunk-Claim"] = claim
    if sha is not None:
        headers["X-Chunk-SHA256"] = sha
    return client.put(
        f"/api/v1/sessions/{sid}/chunks?start={start}&end={end}",
        content=content,
        headers=headers,
    )


def _claim_rows(sid: str) -> list[sqlite3.Row]:
    return db.get_conn().execute(
        "SELECT * FROM chunk_claims WHERE session_id = ?", (sid,)
    ).fetchall()


# ---------------------------------------------------------------------------
# 正常路径：认领 → 持令牌提交 → 消费 → 完成
# ---------------------------------------------------------------------------


def test_claim_happy_path_and_token_consumed(client: TestClient) -> None:
    data = bytes((i * 7 + 1) % 256 for i in range(1000))
    sid = _create(client, data)

    r = _claim(client, sid, 0, 100)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["start"] == 0 and body["end"] == 100
    assert body["claim"].startswith(body["claim_id"] + ".")
    # 默认 5 分钟到期。
    created = storage.parse_iso(body["created_at"])
    expires = storage.parse_iso(body["expires_at"])
    assert expires - created == timedelta(seconds=300)

    # 与有效认领相交：无令牌 → 409。
    r = _put(client, sid, 0, 100, data[:100])
    assert r.status_code == 409
    assert "claim" in r.json()["detail"]["error"]

    # 令牌匹配且区间完全一致 → 200；成功提交即消费令牌。
    r = _put(client, sid, 0, 100, data[:100], claim=body["claim"])
    assert r.status_code == 200, r.text
    assert r.json()["received"] == {"start": 0, "end": 100}
    assert _claim_rows(sid) == []

    # 令牌已消费：再次使用同一令牌 → 区间已接收，按现有行为 409。
    r = _put(client, sid, 0, 100, data[:100], claim=body["claim"])
    assert r.status_code == 409

    # 已消费区间不可再认领（与已收区间相交）。
    assert _claim(client, sid, 0, 100).status_code == 409

    # 未认领区间沿用现有行为：不带头直接提交，随后正常完成。
    r = _put(client, sid, 100, 1000, data[100:])
    assert r.status_code == 200, r.text
    done = client.post(f"/api/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data


def test_server_stores_only_token_hash(client: TestClient) -> None:
    sid = _create(client, b"x" * 100)
    body = _claim(client, sid, 0, 50).json()
    token = body["claim"].split(".", 1)[1]
    rows = _claim_rows(sid)
    assert len(rows) == 1
    # 库里只有令牌明文的 SHA-256，明文不出现。
    assert rows[0]["token_hash"] == hashlib.sha256(token.encode("ascii")).hexdigest()
    assert token not in rows[0]["token_hash"]
    assert rows[0]["start_offset"] == 0 and rows[0]["end_offset"] == 50


# ---------------------------------------------------------------------------
# 认领创建的校验
# ---------------------------------------------------------------------------


def test_claim_validation_errors(client: TestClient) -> None:
    data = b"abcdefghij" * 100  # 1000 字节
    sid = _create(client, data)

    # start >= end → 422（请求模型校验）
    assert _claim(client, sid, 100, 100).status_code == 422
    assert _claim(client, sid, 200, 100).status_code == 422
    # 越界 → 400
    r = _claim(client, sid, 900, 1001)
    assert r.status_code == 400
    assert "exceeds total_size" in r.json()["detail"]["error"]

    # 只能认领尚未接收的区间：与已收区间相交 → 409
    assert _put(client, sid, 0, 100, data[:100]).status_code == 200
    r = _claim(client, sid, 50, 150)
    assert r.status_code == 409
    assert "already received" in r.json()["detail"]["error"]
    # 与已收区间相邻允许
    assert _claim(client, sid, 100, 200).status_code == 201


def test_intersecting_active_claims_rejected(client: TestClient) -> None:
    sid = _create(client, b"x" * 1000)
    assert _claim(client, sid, 100, 200).status_code == 201

    # 相交的有效认领只能有一个：子集/超集/部分重叠/全包一律 409。
    for start, end in ((150, 250), (50, 150), (100, 200), (120, 180), (0, 1000)):
        r = _claim(client, sid, start, end)
        assert r.status_code == 409, (start, end, r.text)
        assert "active claim" in r.json()["detail"]["error"]

    # 相邻不相交 → 201。
    assert _claim(client, sid, 200, 300).status_code == 201
    assert _claim(client, sid, 0, 100).status_code == 201
    assert len(_claim_rows(sid)) == 3


def test_claim_larger_than_max_chunk_rejected(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(settings, "max_chunk_bytes", 100)
    sid = _create(client, b"m" * 1000)
    r = _claim(client, sid, 0, 101)
    assert r.status_code == 413
    assert _claim(client, sid, 0, 100).status_code == 201


def test_claim_does_not_slide_session_ttl(client: TestClient) -> None:
    sid = _create(client, b"s" * 100)
    before = client.get(f"/api/v1/sessions/{sid}").json()
    assert _claim(client, sid, 0, 50).status_code == 201
    after = client.get(f"/api/v1/sessions/{sid}").json()
    # 认领不是分片活动：不滑动会话过期时间与活动时间。
    assert after["expires_at"] == before["expires_at"]
    assert after["last_access_at"] == before["last_access_at"]


# ---------------------------------------------------------------------------
# 过期与重领
# ---------------------------------------------------------------------------


def test_expired_claim_can_be_reclaimed(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(settings, "chunk_claim_ttl_seconds", 1)
    data = b"y" * 100
    sid = _create(client, data)
    first = _claim(client, sid, 0, 100).json()

    time.sleep(1.1)
    # 过期后同一区间可重新认领。
    second = _claim(client, sid, 0, 100)
    assert second.status_code == 201, second.text
    assert second.json()["claim_id"] != first["claim_id"]

    # 到期后旧令牌不可用（旧行已回收 → 凭证无效）。
    r = _put(client, sid, 0, 100, data, claim=first["claim"])
    assert r.status_code == 403
    # 新令牌可用。
    r = _put(client, sid, 0, 100, data, claim=second.json()["claim"])
    assert r.status_code == 200, r.text


def test_expired_token_rejected_then_range_open(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(settings, "chunk_claim_ttl_seconds", 1)
    data = b"z" * 100
    sid = _create(client, data)
    claim = _claim(client, sid, 0, 100).json()["claim"]

    time.sleep(1.1)
    # 到期后旧令牌不可用 → 410。
    r = _put(client, sid, 0, 100, data, claim=claim)
    assert r.status_code == 410
    assert r.json()["detail"]["error"] == "chunk claim expired"
    # 认领已过期：区间回到未认领状态，不带头沿用现有行为。
    r = _put(client, sid, 0, 100, data)
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# PUT /chunks 携带 X-Chunk-Claim 的校验
# ---------------------------------------------------------------------------


def test_put_with_wrong_or_inexact_token_rejected(client: TestClient) -> None:
    data = bytes(range(256)) * 4  # 1024 字节
    sid = _create(client, data)
    claim = _claim(client, sid, 0, 100).json()["claim"]
    claim_id = claim.split(".", 1)[0]

    # 伪造/篡改令牌 → 403。
    forged_token = claim_id + "." + "A" * 43
    assert _put(client, sid, 0, 100, data[:100], claim=forged_token).status_code == 403
    forged_id = "0" * 32 + "." + claim.split(".", 1)[1]
    assert _put(client, sid, 0, 100, data[:100], claim=forged_id).status_code == 403

    # 别的会话的令牌 → 403。
    other_sid = _create(client, data)
    other_claim = _claim(client, other_sid, 0, 100).json()["claim"]
    r = _put(client, sid, 0, 100, data[:100], claim=other_claim)
    assert r.status_code == 403

    # 令牌有效但区间不完全一致（子集/超集/部分相交）→ 409。
    for start, end in ((0, 50), (0, 200), (50, 150)):
        r = _put(client, sid, start, end, data[start:end], claim=claim)
        assert r.status_code == 409, (start, end, r.text)
        assert "exactly match" in r.json()["detail"]["error"]

    # 同一会话另一区间的认领令牌不能混用 → 409。
    second = _claim(client, sid, 100, 200).json()["claim"]
    r = _put(client, sid, 100, 200, data[100:200], claim=claim)
    assert r.status_code == 409
    assert "exactly match" in r.json()["detail"]["error"]

    # 以上失败都不消费认领：两个认领仍可按各自区间提交成功。
    assert _put(client, sid, 0, 100, data[:100], claim=claim).status_code == 200
    assert _put(client, sid, 100, 200, data[100:200], claim=second).status_code == 200
    assert _claim_rows(sid) == []


def test_malformed_claim_header_is_400(client: TestClient) -> None:
    data = b"q" * 100
    sid = _create(client, data)
    for bad in ("not-a-claim", "abc.def", "g" * 32 + "." + "x" * 43, "", "   "):
        r = _put(client, sid, 0, 100, data, claim=bad)
        assert r.status_code == 400, (bad, r.text)
        assert "X-Chunk-Claim" in r.json()["detail"]["error"]
    # 去掉头后行为回退到旧逻辑，同一片照常接收。
    assert _put(client, sid, 0, 100, data).status_code == 200


def test_failure_does_not_consume_claim(client: TestClient) -> None:
    data = bytes((i * 3 + 5) % 256 for i in range(100))
    sid = _create(client, data)
    claim = _claim(client, sid, 0, 100).json()["claim"]

    # Content-Length 与区间不符 → 400，认领保留。
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=100",
        content=data[:50],
        headers={"X-Chunk-Claim": claim},
    )
    assert r.status_code == 400
    assert len(_claim_rows(sid)) == 1

    # 逐片摘要不符 → 422，认领保留。
    r = _put(client, sid, 0, 100, data, claim=claim, sha="0" * 64)
    assert r.status_code == 422
    assert len(_claim_rows(sid)) == 1

    # 修正后重传成功（两个头可组合使用），成功才消费。
    r = _put(
        client, sid, 0, 100, data,
        claim=claim, sha=hashlib.sha256(data).hexdigest(),
    )
    assert r.status_code == 200, r.text
    assert _claim_rows(sid) == []


# ---------------------------------------------------------------------------
# 会话结束认领失效
# ---------------------------------------------------------------------------


def test_claim_on_terminal_or_unknown_session(client: TestClient, monkeypatch) -> None:
    data = b"w" * 100
    # 未知会话 → 404。
    assert _claim(client, "deadbeef" * 4, 0, 10).status_code == 404

    # 放弃（硬删除）后 → 404。
    sid = _create(client, data)
    assert _claim(client, sid, 0, 10).status_code == 201
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert _claim(client, sid, 0, 10).status_code == 404

    # 完成后 → 409。
    sid2 = _create(client, data)
    assert _put(client, sid2, 0, 100, data).status_code == 200
    assert client.post(f"/api/v1/sessions/{sid2}/complete").status_code == 200
    r = _claim(client, sid2, 0, 10)
    assert r.status_code == 409

    # 过期后 → 410。
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid3 = _create(client, data)
    time.sleep(1.1)
    assert _claim(client, sid3, 0, 10).status_code == 410


def test_session_end_invalidates_claims(client: TestClient, monkeypatch) -> None:
    data = b"v" * 100

    # 放弃：认领随会话级联删除。
    sid = _create(client, data)
    _claim(client, sid, 0, 50)
    assert len(_claim_rows(sid)) == 1
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert _claim_rows(sid) == []

    # 过期：后台清理把会话标记 expired 并删除其认领；旧令牌不可用。
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid2 = _create(client, data)
    claim = _claim(client, sid2, 0, 50).json()["claim"]
    time.sleep(1.1)
    expired = run_cleanup()
    assert sid2 in expired
    assert _claim_rows(sid2) == []
    r = _put(client, sid2, 0, 50, data[:50], claim=claim)
    assert r.status_code == 410


def test_complete_deletes_leftover_claim_rows(client: TestClient) -> None:
    """完成时删除残留认领行（经 API 完成前必然已消费，这里直接插行验证兜底）。"""
    data = b"u" * 100
    sid = _create(client, data)
    assert _put(client, sid, 0, 100, data).status_code == 200
    with db.immediate() as tx:
        tx.execute(
            "INSERT INTO chunk_claims ("
            "id, session_id, start_offset, end_offset, token_hash, "
            "created_at, expires_at"
            ") VALUES ('c1', ?, 0, 100, ?, "
            "'2026-01-01T00:00:00Z', '2999-01-01T00:00:00Z')",
            (sid, "a" * 64),
        )
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200
    assert _claim_rows(sid) == []


def test_janitor_sweeps_expired_claim_rows(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(settings, "chunk_claim_ttl_seconds", 1)
    sid = _create(client, b"t" * 100)
    _claim(client, sid, 0, 50)
    time.sleep(1.1)
    run_cleanup()
    # 会话仍在上传中，但过期认领行已被清理线程回收。
    assert _claim_rows(sid) == []
    assert client.get(f"/api/v1/sessions/{sid}").json()["status"] == "uploading"


# ---------------------------------------------------------------------------
# copy-chunk 与并发
# ---------------------------------------------------------------------------


def _make_completed(client: TestClient, data: bytes) -> str:
    sid = _create(client, data)
    r = _put(client, sid, 0, len(data), data)
    assert r.status_code == 200, r.text
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200
    return sid


def test_copy_chunk_respects_active_claims(client: TestClient) -> None:
    data = bytes((i * 11 + 3) % 256 for i in range(500))
    source = _make_completed(client, data)
    target = _create(client, data)
    _claim(client, target, 0, 100)

    # 与有效认领相交的目标区间：copy-chunk 不携带令牌 → 409。
    r = client.post(
        f"/api/v1/sessions/{target}/copy-chunk",
        json={
            "source_session_id": source,
            "source_start": 0,
            "source_end": 100,
            "target_start": 0,
        },
    )
    assert r.status_code == 409
    assert "claim" in r.json()["detail"]["error"]

    # 未认领区间复制不受影响。
    r = client.post(
        f"/api/v1/sessions/{target}/copy-chunk",
        json={
            "source_session_id": source,
            "source_start": 100,
            "source_end": 200,
            "target_start": 100,
        },
    )
    assert r.status_code == 200, r.text


def test_concurrent_claims_single_winner(client: TestClient) -> None:
    sid = _create(client, b"c" * 1000)
    codes: list[int] = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()  # 放大并发认领同一区间的竞态窗口
        codes.append(_claim(client, sid, 100, 200).status_code)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 相交的有效认领只能有一个：恰一个成功，其余全部 409。
    assert codes.count(201) == 1, codes
    assert codes.count(409) == len(codes) - 1, codes
    assert len(_claim_rows(sid)) == 1


# ---------------------------------------------------------------------------
# 老库自动迁移
# ---------------------------------------------------------------------------


def test_init_db_migrates_v3_database(monkeypatch, tmp_path: Path) -> None:
    """v3 老库（含 idempotency_keys）平滑升级到 v4，老数据保留。"""
    db_file = tmp_path / "v3.db"
    raw = sqlite3.connect(str(db_file))
    raw.executescript(db.MIGRATION_1)
    raw.executescript(db.MIGRATION_2)
    raw.executescript(db.MIGRATION_3)
    raw.execute("PRAGMA user_version = 3")
    raw.execute(
        "INSERT INTO upload_sessions VALUES ("
        "'sid8','old.bin',10,'" + "a" * 64 + "',4,'completed',10,"
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
                "SELECT filename FROM upload_sessions WHERE id = 'sid8'"
            ).fetchone()["filename"]
            == "old.bin"
        )
        # 新表可用，且再次初始化幂等。
        conn.execute(
            "INSERT INTO chunk_claims ("
            "id, session_id, start_offset, end_offset, token_hash, "
            "created_at, expires_at) VALUES ("
            "'c9','sid8',0,10,'" + "b" * 64 + "',"
            "'2026-01-01T00:00:00Z','2026-01-01T00:05:00Z')"
        )
        db.init_db()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
    finally:
        db._local.conn = None
