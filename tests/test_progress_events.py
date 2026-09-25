"""分片进度增量同步：快照基线 + 游标事件流（上传/复制/修复/失败/并发/迁移）。"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from pathlib import Path

from fastapi.testclient import TestClient

from app import db, settings


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


def _put(client: TestClient, sid: str, start: int, end: int, content: bytes, **headers):
    return client.put(
        f"/api/v1/sessions/{sid}/chunks?start={start}&end={end}",
        content=content,
        headers=headers,
    )


def _progress(client: TestClient, sid: str):
    return client.get(f"/api/v1/sessions/{sid}/progress")


def _events(client: TestClient, sid: str, cursor: int = 0, limit: int | None = None):
    url = f"/api/v1/sessions/{sid}/events?cursor={cursor}"
    if limit is not None:
        url += f"&limit={limit}"
    return client.get(url)


def _event_count(sid: str) -> int:
    return db.get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunk_events WHERE session_id = ?", (sid,)
    ).fetchone()["n"]


# ---------------------------------------------------------------------------
# 快照：已收区间与游标在同一读事务；旧区间作为基线
# ---------------------------------------------------------------------------


def test_snapshot_initial_empty(client: TestClient) -> None:
    sid = _create(client, b"x" * 100)
    r = _progress(client, sid)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["status"] == "uploading"
    assert body["event_cursor"] == 0
    assert body["received_ranges"] == []
    assert body["missing_ranges"] == [{"start": 0, "end": 100}]
    assert body["bytes_received"] == 0


def test_snapshot_merges_adjacent_ranges_and_advances_cursor(client: TestClient) -> None:
    data = bytes(range(100))
    sid = _create(client, data)
    assert _put(client, sid, 0, 10, data[0:10]).status_code == 200
    assert _put(client, sid, 20, 30, data[20:30]).status_code == 200
    # 相邻区间在快照中合并为一个；中间缺口保留为缺失区间。
    assert _put(client, sid, 10, 20, data[10:20]).status_code == 200

    body = _progress(client, sid).json()
    assert body["event_cursor"] == 3
    assert body["received_ranges"] == [{"start": 0, "end": 30}]
    assert body["missing_ranges"] == [{"start": 30, "end": 100}]
    assert body["bytes_received"] == 30


def test_snapshot_and_events_do_not_slide_ttl(client: TestClient) -> None:
    sid = _create(client, b"y" * 10)
    conn = db.get_conn()
    before = conn.execute(
        "SELECT expires_at, last_access_at FROM upload_sessions WHERE id = ?", (sid,)
    ).fetchone()
    assert _progress(client, sid).status_code == 200
    assert _events(client, sid).status_code == 200
    after = conn.execute(
        "SELECT expires_at, last_access_at FROM upload_sessions WHERE id = ?", (sid,)
    ).fetchone()
    # 纯只读：不滑动 TTL、不更新活动时间。
    assert tuple(after) == tuple(before)


# ---------------------------------------------------------------------------
# 事件流：按游标分页、序号严格连续、不同会话各自从 1 开始
# ---------------------------------------------------------------------------


def test_events_record_uploads_in_order_with_gapless_seq(client: TestClient) -> None:
    data = bytes(range(60))
    sid = _create(client, data)
    # 乱序提交，事件仍按提交顺序排列，序号 1..N 严格连续。
    for start, end in [(20, 30), (0, 10), (40, 60), (10, 20), (30, 40)]:
        assert _put(client, sid, start, end, data[start:end]).status_code == 200

    r = _events(client, sid, cursor=0)
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert [e["seq"] for e in events] == [1, 2, 3, 4, 5]
    assert all(e["source"] == "upload" for e in events)
    assert [(e["start"], e["end"]) for e in events] == [
        (20, 30), (0, 10), (40, 60), (10, 20), (30, 40),
    ]
    assert r.json()["has_more"] is False
    assert r.json()["next_cursor"] == 5


def test_events_respect_cursor_position(client: TestClient) -> None:
    data = bytes(range(30))
    sid = _create(client, data)
    for start, end in [(0, 10), (10, 20), (20, 30)]:
        _put(client, sid, start, end, data[start:end])

    r = _events(client, sid, cursor=2)
    body = r.json()
    assert body["cursor"] == 2
    assert [e["seq"] for e in body["events"]] == [3]
    assert body["events"][0]["start"] == 20
    assert body["has_more"] is False

    # 游标领先于最新事件：空页。
    tail = _events(client, sid, cursor=3).json()
    assert tail["events"] == []
    assert tail["next_cursor"] is None
    assert tail["has_more"] is False


def test_events_pagination_limit_boundaries(client: TestClient) -> None:
    data = bytes(range(5))
    sid = _create(client, data)
    for i in range(5):
        assert _put(client, sid, i, i + 1, data[i:i + 1]).status_code == 200

    # limit=2：第一页 seq 1,2 且有下一页，next_cursor 指向下页起点。
    page1 = _events(client, sid, cursor=0, limit=2).json()
    assert [e["seq"] for e in page1["events"]] == [1, 2]
    assert page1["has_more"] is True
    assert page1["next_cursor"] == 2

    page2 = _events(client, sid, cursor=page1["next_cursor"], limit=2).json()
    assert [e["seq"] for e in page2["events"]] == [3, 4]
    assert page2["has_more"] is True
    assert page2["next_cursor"] == 4

    page3 = _events(client, sid, cursor=page2["next_cursor"], limit=2).json()
    assert [e["seq"] for e in page3["events"]] == [5]
    assert page3["has_more"] is False
    assert page3["next_cursor"] == 5  # 末页非空仍可前移游标

    # 恰好取满一页且无更多时，has_more 为 false、next_cursor 指向末条。
    full = _events(client, sid, cursor=0, limit=5).json()
    assert len(full["events"]) == 5
    assert full["has_more"] is False
    assert full["next_cursor"] == 5
    # 用该游标再读得到空页（无新提交）。
    assert _events(client, sid, cursor=5).json()["events"] == []

    # 页大小越界：0、101、负数 → 422。
    assert _events(client, sid, limit=0).status_code == 422
    assert _events(client, sid, limit=101).status_code == 422
    assert client.get(
        f"/api/v1/sessions/{sid}/events?cursor=-1"
    ).status_code == 422


def test_event_seq_is_per_session_monotonic(client: TestClient) -> None:
    a = _create(client, b"a" * 20, name="a.bin")
    b = _create(client, b"b" * 20, name="b.bin")
    _put(client, a, 0, 10, b"a" * 10)
    _put(client, b, 0, 10, b"b" * 10)
    _put(client, a, 10, 20, b"a" * 10)

    seq_a = [e["seq"] for e in _events(client, a).json()["events"]]
    seq_b = [e["seq"] for e in _events(client, b).json()["events"]]
    # 每个会话各自从 1 开始独立连续编号。
    assert seq_a == [1, 2]
    assert seq_b == [1]


def test_concurrent_uploads_produce_contiguous_unique_seq(client: TestClient) -> None:
    n = 12
    data = bytes((i * 13 + 5) % 256 for i in range(n))
    sid = _create(client, data)
    barrier = threading.Barrier(n)
    codes: list[int] = []

    def worker(i: int) -> None:
        barrier.wait()
        r = _put(client, sid, i, i + 1, data[i:i + 1])
        codes.append(r.status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes == [200] * n, codes

    rows = db.get_conn().execute(
        "SELECT seq FROM chunk_events WHERE session_id = ? ORDER BY seq", (sid,)
    ).fetchall()
    # IMMEDIATE 串行化 + COALESCE(MAX(seq))+1：无洞、无重复。
    assert [r["seq"] for r in rows] == list(range(1, n + 1))
    assert _progress(client, sid).json()["event_cursor"] == n


# ---------------------------------------------------------------------------
# 快照 + 游标续读不漏提交（断线重连核心语义）
# ---------------------------------------------------------------------------


def test_snapshot_then_cursor_tail_does_not_miss_commit(client: TestClient) -> None:
    data = bytes(range(40))
    sid = _create(client, data)
    _put(client, sid, 0, 10, data[0:10])
    _put(client, sid, 10, 20, data[10:20])

    # 重连：先取快照（基线 + 游标）。
    snap = _progress(client, sid).json()
    assert snap["event_cursor"] == 2
    assert snap["received_ranges"] == [{"start": 0, "end": 20}]

    # 快照之后另一客户端提交了新区间。
    _put(client, sid, 20, 30, data[20:30])

    # 用快照游标续读：恰好拿到快照之后的提交，不会与基线重叠或遗漏。
    tail = _events(client, sid, cursor=snap["event_cursor"]).json()
    assert [(e["seq"], e["start"], e["end"]) for e in tail["events"]] == [
        (3, 20, 30)
    ]

    # 基线 ∪ 增量 即当前全部已收区间。
    _put(client, sid, 30, 40, data[30:40])
    final = _events(client, sid, cursor=tail["next_cursor"] or 3).json()
    assert [(e["start"], e["end"]) for e in final["events"]] == [(30, 40)]
    assert _progress(client, sid).json()["received_ranges"] == [
        {"start": 0, "end": 40}
    ]


# ---------------------------------------------------------------------------
# copy-chunk 产生 source='copy' 事件；失败/冲突/修复不产生事件
# ---------------------------------------------------------------------------


def test_copy_chunk_records_copy_event(client: TestClient) -> None:
    source_data = bytes(range(100))
    src = _create(client, source_data, name="src.bin")
    _put(client, src, 0, 100, source_data)
    assert client.post(f"/api/v1/sessions/{src}/complete").status_code == 200

    target_data = source_data  # 目标文件内容与来源一致
    dst = _create(client, target_data, name="dst.bin")
    r = client.post(
        f"/api/v1/sessions/{dst}/copy-chunk",
        json={
            "source_session_id": src,
            "source_start": 10,
            "source_end": 40,
            "target_start": 10,
        },
    )
    assert r.status_code == 200, r.text

    events = _events(client, dst).json()["events"]
    assert len(events) == 1
    assert events[0]["source"] == "copy"
    assert events[0]["seq"] == 1
    assert (events[0]["start"], events[0]["end"]) == (10, 40)
    assert _progress(client, dst).json()["event_cursor"] == 1


def test_failures_conflicts_and_repair_create_no_events(client: TestClient) -> None:
    data = bytes(range(30))
    sid = _create(client, data)
    assert _put(client, sid, 0, 10, data[0:10]).status_code == 200
    assert _event_count(sid) == 1

    # 重叠冲突 409：不产生事件。
    assert _put(client, sid, 5, 15, data[5:15]).status_code == 409
    # 长度不符 400：不产生事件。
    assert _put(client, sid, 10, 20, b"short").status_code == 400
    # 逐片摘要不符 422：不产生事件。
    assert _put(
        client, sid, 10, 20, data[10:20],
        **{"X-Chunk-SHA256": "0" * 64},
    ).status_code == 422
    assert _event_count(sid) == 1

    # 正确提交第二片，随后原位修复：修复只替换内容、不新增区间，不产生事件。
    assert _put(client, sid, 10, 20, data[10:20]).status_code == 200
    assert _event_count(sid) == 2
    repair = client.put(
        f"/api/v1/sessions/{sid}/chunks/repair?start=10&end=20",
        content=data[10:20],
    )
    assert repair.status_code == 200, repair.text
    assert _event_count(sid) == 2
    # 修复后游标与已收区间不变。
    snap = _progress(client, sid).json()
    assert snap["event_cursor"] == 2
    assert snap["received_ranges"] == [{"start": 0, "end": 20}]


# ---------------------------------------------------------------------------
# 会话生命周期：未知 404、过期 410、完成后仍可查；级联删除
# ---------------------------------------------------------------------------


def test_snapshot_and_events_session_lifecycle(client: TestClient) -> None:
    data = b"lifecycle" * 5
    sid = _create(client, data)
    _put(client, sid, 0, len(data), data)
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200

    # 完成后：chunks 与事件均保留，快照仍可读。
    snap = _progress(client, sid).json()
    assert snap["status"] == "completed"
    assert snap["received_ranges"] == [{"start": 0, "end": len(data)}]
    assert snap["event_cursor"] == 1
    assert snap["expires_at"] is None
    assert len(_events(client, sid).json()["events"]) == 1

    # 未知会话 404。
    assert _progress(client, "nope").status_code == 404
    assert _events(client, "nope").status_code == 404

    # 放弃会话（硬删除）：快照/事件 404，事件行级联删除。
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert _progress(client, sid).status_code == 404
    assert _events(client, sid).status_code == 404
    assert _event_count(sid) == 0


def test_expired_session_returns_410(client: TestClient) -> None:
    sid = _create(client, b"z" * 5)
    db.get_conn().execute(
        "UPDATE upload_sessions SET status = 'expired' WHERE id = ?", (sid,)
    )
    assert _progress(client, sid).status_code == 410
    assert _events(client, sid).status_code == 410


# ---------------------------------------------------------------------------
# 旧会话基线：迁移前已有的 chunks 不回填事件，作为游标 0 的基线
# ---------------------------------------------------------------------------


def test_legacy_chunks_are_baseline_and_new_upload_continues_from_seq_one(
    client: TestClient,
) -> None:
    # 直接在当前库里构造“只有 chunks、没有事件”的会话，模拟从 v4 升级的旧会话。
    conn = db.get_conn()
    legacy_id = "legacy-sid"
    with db.immediate() as tx:
        tx.execute(
            "INSERT INTO upload_sessions ("
            "id, filename, total_size, sha256_expected, chunk_size, status, "
            "bytes_received, created_at, expires_at, last_access_at"
            ") VALUES (?, 'old.bin', 30, ?, 0, 'uploading', 20, "
            "'2026-01-01T00:00:00Z', '2099-01-01T00:00:00Z', "
            "'2026-01-01T00:00:00Z')",
            (legacy_id, "a" * 64),
        )
        tx.execute(
            "INSERT INTO chunks "
            "(session_id, start_offset, end_offset, size, received_at) "
            "VALUES (?, 0, 10, 10, '2026-01-01T00:00:00Z')",
            (legacy_id,),
        )
        tx.execute(
            "INSERT INTO chunks "
            "(session_id, start_offset, end_offset, size, received_at) "
            "VALUES (?, 10, 20, 10, '2026-01-01T00:00:00Z')",
            (legacy_id,),
        )

    # 旧区间作为快照基线，游标为 0，事件表为空。
    snap = _progress(client, legacy_id).json()
    assert snap["event_cursor"] == 0
    assert snap["received_ranges"] == [{"start": 0, "end": 20}]
    assert snap["missing_ranges"] == [{"start": 20, "end": 30}]
    assert _events(client, legacy_id).json()["events"] == []

    # 基线之后成功登记的新分片取得 seq=1（从基线继续，不回填旧区间）。
    r = _put(client, legacy_id, 20, 30, bytes(range(20, 30)))
    assert r.status_code == 200, r.text
    events = _events(client, legacy_id, cursor=0).json()["events"]
    assert [(e["seq"], e["start"], e["end"]) for e in events] == [(1, 20, 30)]
    assert _progress(client, legacy_id).json()["received_ranges"] == [
        {"start": 0, "end": 30}
    ]


def test_init_db_migrates_v4_database(monkeypatch, tmp_path: Path) -> None:
    """v4 老库（含 chunk_claims）平滑升级到最新结构：新增 chunk_events 等，老数据保留。"""
    db_file = tmp_path / "v4.db"
    raw = sqlite3.connect(str(db_file))
    for step in (
        db.MIGRATION_1,
        db.MIGRATION_2,
        db.MIGRATION_3,
        db.MIGRATION_4,
    ):
        raw.executescript(step)
    raw.execute("PRAGMA user_version = 4")
    raw.execute(
        "INSERT INTO upload_sessions VALUES ("
        "'sid7','legacy.bin',10,'" + "a" * 64 + "',4,'completed',10,"
        "'2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','2026-01-01T00:00:00Z',"
        "'2026-01-01T00:00:00Z')"
    )
    raw.execute(
        "INSERT INTO chunks "
        "(session_id, start_offset, end_offset, size, received_at) "
        "VALUES ('sid7', 0, 10, 10, '2026-01-01T00:00:00Z')"
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
        # 老数据保留，且旧 chunks 不回填事件。
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM chunks WHERE session_id = 'sid7'"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM chunk_events"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS c FROM chunk_events "
            "WHERE session_id = 'sid7'"
        ).fetchone()["c"] == 0
        # 新表可用：旧会话基线之后首个事件 seq=1，且再次初始化幂等。
        conn.execute(
            "INSERT INTO chunk_events "
            "(session_id, seq, start_offset, end_offset, source, created_at) "
            "VALUES ('sid7', 1, 0, 10, 'upload', '2026-01-03T00:00:00Z')"
        )
        # 同会话序号唯一约束生效。
        import pytest

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO chunk_events "
                "(session_id, seq, start_offset, end_offset, source, created_at) "
                "VALUES ('sid7', 1, 0, 10, 'copy', '2026-01-03T00:01:00Z')"
            )
        db.init_db()
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.LATEST_USER_VERSION
    finally:
        db._local.conn = None
