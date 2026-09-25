"""PUT /sessions/{id}/chunks/repair：已收分片原位修复。"""
from __future__ import annotations

import hashlib
import threading
from contextlib import contextmanager

from fastapi.testclient import TestClient

from app import settings, storage


def _create(client: TestClient, data: bytes, name: str = "data.bin", chunk_size: int = 0):
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "chunk_size": chunk_size,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def _put(client: TestClient, sid: str, start: int, end: int, content: bytes, **kw):
    return client.put(
        f"/api/v1/sessions/{sid}/chunks?start={start}&end={end}",
        content=content,
        **kw,
    )


def _repair(client: TestClient, sid: str, start: int, end: int, content: bytes, **kw):
    return client.put(
        f"/api/v1/sessions/{sid}/chunks/repair?start={start}&end={end}",
        content=content,
        **kw,
    )


def _complete_ok(client: TestClient, sid: str, data: bytes) -> None:
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == data


# ---------------------------------------------------------------------------
# 正常路径
# ---------------------------------------------------------------------------


def test_repair_corrupted_chunk_then_full_verification_passes(client: TestClient) -> None:
    # 典型场景：某分片上传时内容损坏，整文件校验 422；原位修复后重新完成。
    data = bytes((i * 7 + 3) % 256 for i in range(6000))
    sid = _create(client, data)
    bounds = [0, 100, 2500, 6000]
    pieces = [data[s:e] for s, e in zip(bounds, bounds[1:])]
    corrupted = pieces[1][:]
    corrupted = b"!" + corrupted[1:]  # 等长损坏
    _put(client, sid, 0, 100, pieces[0]).raise_for_status()
    _put(client, sid, 100, 2500, corrupted).raise_for_status()
    _put(client, sid, 2500, 6000, pieces[2]).raise_for_status()

    # 整文件校验失败，会话保持 uploading。
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "sha256 mismatch"

    # 用正确内容原位修复中间分片（携带 X-Chunk-SHA256）。
    r = _repair(
        client, sid, 100, 2500, pieces[1],
        headers={"X-Chunk-SHA256": hashlib.sha256(pieces[1]).hexdigest()},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["received"] == {"start": 100, "end": 2500}
    # 缺失区间与已收字节数保持不变（本就全收齐）。
    assert body["bytes_received"] == 6000
    assert body["fully_received"] is True
    assert body["missing_ranges"] == []

    # 磁盘上的分片确实被替换为新内容。
    assert storage.part_path(sid, 100, 2500).read_bytes() == pieces[1]
    # 备份文件不应残留。
    assert not storage.part_repair_backup_path(sid, 100, 2500).exists()

    # 修复后可重新完成整文件校验。
    _complete_ok(client, sid, data)


def test_repair_response_keeps_missing_ranges_and_bytes(client: TestClient) -> None:
    data = bytes(range(256)) * 20  # 5120
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data[:1000]).raise_for_status()
    _put(client, sid, 2000, 4000, data[2000:4000]).raise_for_status()

    new_middle = bytes((b ^ 0x5A) for b in data[2000:4000])
    r = _repair(client, sid, 2000, 4000, new_middle)
    assert r.status_code == 200, r.text
    body = r.json()
    # 区间登记与字节数都不变：仍是修复前的两块、3000 字节。
    assert body["bytes_received"] == 3000
    assert body["fully_received"] is False
    assert body["missing_ranges"] == [
        {"start": 1000, "end": 2000},
        {"start": 4000, "end": 5120},
    ]

    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 3000
    assert status["missing_ranges"] == body["missing_ranges"]
    assert storage.part_path(sid, 2000, 4000).read_bytes() == new_middle


def test_repair_without_sha_header_accepted(client: TestClient) -> None:
    data = b"abcdefgh" * 250
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data[:1000]).raise_for_status()
    replaced = b"Z" * 1000
    r = _repair(client, sid, 0, 1000, replaced)
    assert r.status_code == 200, r.text
    assert storage.part_path(sid, 0, 1000).read_bytes() == replaced


def test_repair_then_repair_back_and_complete(client: TestClient) -> None:
    data = bytes((i * 11) % 256 for i in range(3000))
    sid = _create(client, data)
    _put(client, sid, 0, 3000, data).raise_for_status()

    changed = bytearray(data)
    changed[10] ^= 0xFF
    changed = bytes(changed)
    r = _repair(client, sid, 0, 3000, changed)
    assert r.status_code == 200, r.text
    # 修复成别的内容后整文件校验失败。
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 422

    # 再次原位修复回原始字节，重新完成通过。
    r = _repair(
        client, sid, 0, 3000, data,
        headers={"X-Chunk-SHA256": hashlib.sha256(data).hexdigest()},
    )
    assert r.status_code == 200, r.text
    _complete_ok(client, sid, data)


# ---------------------------------------------------------------------------
# 区间必须与已登记分片完全一致
# ---------------------------------------------------------------------------


def test_repair_requires_exact_registered_range(client: TestClient) -> None:
    data = b"r" * 4000
    sid = _create(client, data)
    _put(client, sid, 1000, 3000, data[1000:3000]).raise_for_status()

    before = client.get(f"/api/v1/sessions/{sid}").json()

    # 完全未登记的区间。
    r = _repair(client, sid, 0, 1000, data[:1000])
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "no registered chunk matches the range exactly"

    # 只是部分重叠（已登记 [1000,3000)，请求 [0,2000) / [2000,4000)）。
    r = _repair(client, sid, 0, 2000, b"q" * 2000)
    assert r.status_code == 404
    r = _repair(client, sid, 2000, 4000, b"q" * 2000)
    assert r.status_code == 404

    # 恰好包含已登记区间但边界更大，也不算完全一致。
    r = _repair(client, sid, 1000, 3001, b"q" * 2001)
    assert r.status_code == 404

    # 非法区间 start >= end。
    r = _repair(client, sid, 1000, 1000, b"")
    assert r.status_code == 400

    # 任何失败都不改变登记与字节数。
    after = client.get(f"/api/v1/sessions/{sid}").json()
    assert after["bytes_received"] == before["bytes_received"] == 2000
    assert after["missing_ranges"] == before["missing_ranges"]
    assert storage.part_path(sid, 1000, 3000).read_bytes() == data[1000:3000]
    assert not list(storage.tmp_dir().glob("*.part"))
    assert not any(
        p.name.startswith(".") for p in storage.parts_dir(sid).iterdir()
    )


# ---------------------------------------------------------------------------
# 长度 / 大小 / 摘要校验：失败一律保留原分片
# ---------------------------------------------------------------------------


def test_repair_wrong_content_length_is_400(client: TestClient) -> None:
    data = b"l" * 1000
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data).raise_for_status()

    r = _repair(client, sid, 0, 1000, b"short")
    assert r.status_code == 400
    assert "content-length" in r.json()["detail"]["error"]
    assert storage.part_path(sid, 0, 1000).read_bytes() == data
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 1000


def test_repair_body_longer_than_range_is_413(client: TestClient) -> None:
    # content-length 与区间一致但流式实际更长：边收边拒，返回 413。
    data = b"b" * 100
    sid = _create(client, data)
    _put(client, sid, 0, 100, data).raise_for_status()

    def overflowing_stream():
        yield b"x" * 100
        yield b"extra"

    r = client.put(
        f"/api/v1/sessions/{sid}/chunks/repair?start=0&end=100",
        content=overflowing_stream(),
        headers={"Content-Length": "100"},
    )
    assert r.status_code == 413, r.text
    assert storage.part_path(sid, 0, 100).read_bytes() == data
    assert not list(storage.tmp_dir().glob("*.part"))


def test_repair_respects_max_chunk_bytes(client: TestClient, monkeypatch) -> None:
    data = b"m" * 200
    sid = _create(client, data)
    _put(client, sid, 0, 200, data).raise_for_status()
    monkeypatch.setattr(settings, "max_chunk_bytes", 100)
    r = _repair(client, sid, 0, 200, data)
    assert r.status_code == 413
    assert storage.part_path(sid, 0, 200).read_bytes() == data


def test_repair_sha256_mismatch_keeps_old_chunk_and_retryable(client: TestClient) -> None:
    data = bytes((i * 17 + 1) % 256 for i in range(2000))
    sid = _create(client, data)
    _put(client, sid, 0, 2000, data).raise_for_status()

    good_digest = hashlib.sha256(data).hexdigest()
    changed = bytearray(data)
    changed[123] ^= 0x01
    changed = bytes(changed)

    r = _repair(
        client, sid, 0, 2000, changed,
        headers={"X-Chunk-SHA256": good_digest},
    )
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "chunk sha256 mismatch"
    assert detail["expected"] == good_digest
    assert detail["actual"] == hashlib.sha256(changed).hexdigest()
    assert detail["start"] == 0
    assert detail["end"] == 2000

    # 原分片内容、登记、字节数、缺失区间全部保留。
    assert storage.part_path(sid, 0, 2000).read_bytes() == data
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 2000
    assert status["missing_ranges"] == []
    assert not list(storage.tmp_dir().glob("*.part"))
    assert not any(p.name.startswith(".") for p in storage.parts_dir(sid).iterdir())

    # 用与摘要一致的内容重试即成功。
    r = _repair(
        client, sid, 0, 2000, data,
        headers={"X-Chunk-SHA256": good_digest},
    )
    assert r.status_code == 200, r.text


def test_repair_malformed_sha_header_is_400(client: TestClient) -> None:
    data = b"h" * 100
    sid = _create(client, data)
    _put(client, sid, 0, 100, data).raise_for_status()
    for bad in ("nope", "a" * 63, "g" * 64, "sha256:" + "b" * 63):
        r = _repair(
            client, sid, 0, 100, data, headers={"X-Chunk-SHA256": bad}
        )
        assert r.status_code == 400, (bad, r.text)
        assert r.json()["detail"]["error"] == "invalid X-Chunk-SHA256 header"
    assert storage.part_path(sid, 0, 100).read_bytes() == data


def test_failed_repair_does_not_slide_ttl(client: TestClient) -> None:
    data = b"t" * 500
    sid = _create(client, data)
    _put(client, sid, 0, 500, data).raise_for_status()
    before = client.get(f"/api/v1/sessions/{sid}").json()["expires_at"]

    # 区间不存在（准备阶段失败）与摘要不符（接收后失败）都不得滑动 TTL。
    _repair(client, sid, 0, 10, b"t" * 10)
    _repair(
        client, sid, 0, 500, b"x" * 500,
        headers={"X-Chunk-SHA256": hashlib.sha256(data).hexdigest()},
    )
    after = client.get(f"/api/v1/sessions/{sid}").json()
    assert after["expires_at"] == before


# ---------------------------------------------------------------------------
# 会话状态
# ---------------------------------------------------------------------------


def test_repair_unknown_session_is_404(client: TestClient) -> None:
    r = _repair(client, "deadbeef" * 4, 0, 10, b"0123456789")
    assert r.status_code == 404


def test_repair_completed_session_rejected(client: TestClient) -> None:
    data = b"done" * 100
    sid = _create(client, data)
    _put(client, sid, 0, len(data), data).raise_for_status()
    _complete_ok(client, sid, data)
    r = _repair(client, sid, 0, len(data), b"x" * len(data))
    assert r.status_code == 409
    # 成品不受影响。
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data


def test_repair_expired_session_is_410(client: TestClient, monkeypatch) -> None:
    data = b"e" * 300
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid = _create(client, data)
    _put(client, sid, 0, 300, data).raise_for_status()
    import time

    time.sleep(1.1)
    r = _repair(client, sid, 0, 300, data)
    assert r.status_code == 410
    assert not storage.parts_dir(sid).exists()


def test_repair_out_of_bounds_range_is_400(client: TestClient) -> None:
    data = b"o" * 100
    sid = _create(client, data)
    _put(client, sid, 0, 100, data).raise_for_status()
    r = _repair(client, sid, 0, 101, b"o" * 101)
    assert r.status_code == 400
    assert "total_size" in r.json()["detail"]["error"]


def test_repair_works_even_when_old_part_missing_on_disk(client: TestClient) -> None:
    # 登记仍在但磁盘分片丢失：修复直接补上正确内容即可恢复。
    data = bytes((i * 19 + 2) % 256 for i in range(700))
    sid = _create(client, data)
    _put(client, sid, 0, 700, data).raise_for_status()
    storage.part_path(sid, 0, 700).unlink()

    r = _repair(
        client, sid, 0, 700, data,
        headers={"X-Chunk-SHA256": hashlib.sha256(data).hexdigest()},
    )
    assert r.status_code == 200, r.text
    assert storage.part_path(sid, 0, 700).read_bytes() == data
    _complete_ok(client, sid, data)


def test_janitor_sweeps_stale_repair_backups(client: TestClient) -> None:
    import os
    import time as _time
    from app.cleanup import run_cleanup

    data = b"s" * 200
    sid = _create(client, data)
    _put(client, sid, 0, 200, data).raise_for_status()
    backup = storage.part_repair_backup_path(sid, 0, 200)
    backup.write_bytes(b"old")
    # 不足 1 小时：保留。
    run_cleanup()
    assert backup.exists()
    # 模拟崩溃后长期残留：mtime 拨到 2 小时前，下一轮清理应删除。
    old_ts = _time.time() - 2 * 3600
    os.utime(backup, (old_ts, old_ts))
    run_cleanup()
    assert not backup.exists()


# ---------------------------------------------------------------------------
# 故障注入：落位后登记失败必须恢复旧分片
# ---------------------------------------------------------------------------


def test_database_failure_after_swap_restores_old_chunk(
    client: TestClient, monkeypatch
) -> None:
    """新分片已 rename 落位、TTL UPDATE 失败：事务回滚且旧分片恢复，
    不留备份/临时文件，随后用同一份新内容重试仍可成功。"""
    from app import db

    class InjectedDBError(RuntimeError):
        pass

    class FailingConn:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *params):
            if sql.startswith("UPDATE upload_sessions"):
                raise InjectedDBError("injected update failure")
            return self._conn.execute(sql, *params)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    @contextmanager
    def flaky_immediate():
        conn = db.get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield FailingConn(conn)
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    data = bytes((i * 23 + 9) % 256 for i in range(1500))
    sid = _create(client, data)
    _put(client, sid, 0, 1500, data).raise_for_status()

    changed = bytearray(data)
    changed[0] ^= 0x7F
    changed = bytes(changed)

    monkeypatch.setattr("app.main.db.immediate", flaky_immediate)
    import pytest

    with pytest.raises(InjectedDBError):
        _repair(client, sid, 0, 1500, changed)
    monkeypatch.undo()

    # 旧分片内容原样恢复，登记/字节数不变，没有备份与临时文件残留。
    assert storage.part_path(sid, 0, 1500).read_bytes() == data
    assert not storage.part_repair_backup_path(sid, 0, 1500).exists()
    assert not list(storage.tmp_dir().glob("*.part"))
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 1500
    assert status["missing_ranges"] == []

    # 旧分片仍可用：把内容修回 data（本身就是 data）或修成 changed 再修回，
    # 最终整文件校验必须通过。
    r = _repair(client, sid, 0, 1500, changed)
    assert r.status_code == 200, r.text
    r = _repair(
        client, sid, 0, 1500, data,
        headers={"X-Chunk-SHA256": hashlib.sha256(data).hexdigest()},
    )
    assert r.status_code == 200, r.text
    _complete_ok(client, sid, data)


# ---------------------------------------------------------------------------
# 并发：与 complete / abort / 另一个 repair 竞争，不得出现半替换内容
#
# complete 全程持会话锁，因此修复与 complete 的真实交错只有两种：
#   1. 修复还在流式接收（不持锁）时 complete 完成 → 用旧内容完成，
#      修复提交时看到 completed → 409（test_complete_during_repair_*）；
#   2. 修复先提交、complete 后执行 → complete 读到完整的新分片，
#      按新内容做整文件校验（顺序场景已由 test_repair_then_repair_back_
#      and_complete 覆盖：422 后可修复重试）。
# 挂起点选在 _prepare_chunk_repair 返回之后：它在工作线程中执行且已
# 释放会话锁，不会阻塞事件循环，也不会卡住 complete/abort 的取锁。
# ---------------------------------------------------------------------------


def _pause_after_repair_prepare(
    monkeypatch,
) -> tuple[threading.Event, threading.Event]:
    """让下一次修复在“首次校验通过、开始流式接收前”挂起。

    返回 (已到达挂起点, 放行) 两个事件；只挂起第一次调用。
    """
    import app.main as main_mod

    reached = threading.Event()
    released = threading.Event()
    real_prepare = main_mod._prepare_chunk_repair
    used = {"hit": False}

    def paused_prepare(session_id, start, end, declared_length):
        real_prepare(session_id, start, end, declared_length)
        if not used["hit"]:
            used["hit"] = True
            reached.set()
            released.wait(timeout=10)

    monkeypatch.setattr(main_mod, "_prepare_chunk_repair", paused_prepare)
    return reached, released


def test_complete_during_repair_streaming_completes_with_old_content(
    client: TestClient, monkeypatch
) -> None:
    """修复还在流式接收（不持锁）时 complete 先发生：
    用旧分片内容完成，修复随后在事务内看到 completed → 409。"""
    data = b"c" * 2000 + b"d" * 2000
    sid = _create(client, data)
    _put(client, sid, 0, 2000, data[:2000]).raise_for_status()
    _put(client, sid, 2000, 4000, data[2000:]).raise_for_status()

    reached, released = _pause_after_repair_prepare(monkeypatch)
    outcome: dict[str, int] = {}

    def do_repair() -> None:
        r = _repair(client, sid, 0, 2000, b"E" * 2000)
        outcome["repair"] = r.status_code

    t = threading.Thread(target=do_repair, daemon=True)
    t.start()
    assert reached.wait(timeout=10), "repair did not reach streaming stage"
    rc = client.post(f"/api/v1/sessions/{sid}/complete")
    assert rc.status_code == 200, rc.text
    released.set()
    t.join(timeout=10)
    assert not t.is_alive()

    assert outcome["repair"] == 409, outcome
    # 成品是修复前的旧内容；临时文件与备份不残留。
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data
    assert not list(storage.tmp_dir().glob("*.part"))


def test_concurrent_repair_and_abort_never_torn(
    client: TestClient, monkeypatch
) -> None:
    """放弃发生在修复流式接收期间：修复在事务内看到会话已删除 → 404。"""
    data = b"a" * 1000
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data).raise_for_status()

    reached, released = _pause_after_repair_prepare(monkeypatch)
    outcome: dict[str, int] = {}

    def do_repair() -> None:
        r = _repair(client, sid, 0, 1000, b"z" * 1000)
        outcome["repair"] = r.status_code

    t = threading.Thread(target=do_repair, daemon=True)
    t.start()
    assert reached.wait(timeout=10)
    rd = client.delete(f"/api/v1/sessions/{sid}")
    released.set()
    t.join(timeout=10)
    assert not t.is_alive()

    assert rd.status_code == 204
    assert outcome["repair"] in (404, 410), outcome
    # 会话与分片目录已删除，临时文件/备份不残留。
    assert client.get(f"/api/v1/sessions/{sid}").status_code == 404
    assert not storage.parts_dir(sid).exists()
    assert not list(storage.tmp_dir().glob("*.part"))


def test_concurrent_double_repair_last_wins_and_never_torn(client: TestClient) -> None:
    data = b"0" * 800
    sid = _create(client, data)
    _put(client, sid, 0, 800, data).raise_for_status()

    payload_a = b"A" * 800
    payload_b = b"B" * 800
    results: list[int] = []
    barrier = threading.Barrier(2)

    def worker(payload: bytes) -> None:
        barrier.wait()
        r = _repair(client, sid, 0, 800, payload)
        results.append(r.status_code)

    threads = [
        threading.Thread(target=worker, args=(payload_a,), daemon=True),
        threading.Thread(target=worker, args=(payload_b,), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive()
    assert sorted(results) == [200, 200]

    final_bytes = storage.part_path(sid, 0, 800).read_bytes()
    # 最终内容恰好是两者之一，绝不会是混合的“半替换”内容。
    assert final_bytes in (payload_a, payload_b)
    assert not storage.part_repair_backup_path(sid, 0, 800).exists()
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 800
    assert status["missing_ranges"] == []
