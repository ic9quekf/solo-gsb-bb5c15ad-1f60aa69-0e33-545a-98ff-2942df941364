"""POST /sessions/{id}/chunks/audit：上传中会话的分片完整性诊断。"""
from __future__ import annotations

import hashlib
import threading
import time
from contextlib import contextmanager

from fastapi.testclient import TestClient

from app import db, settings, storage


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


def _audit(client: TestClient, sid: str, entries: list[dict]):
    return client.post(f"/api/v1/sessions/{sid}/chunks/audit", json={"chunks": entries})


def _entry(start: int, end: int, content: bytes) -> dict:
    return {"start": start, "end": end, "sha256": hashlib.sha256(content).hexdigest()}


# ---------------------------------------------------------------------------
# 正常路径
# ---------------------------------------------------------------------------


def test_audit_all_ok_and_results_sorted_by_start(client: TestClient) -> None:
    data = bytes((i * 7 + 3) % 256 for i in range(6000))
    sid = _create(client, data)
    bounds = [0, 100, 2500, 6000]
    pieces = [data[s:e] for s, e in zip(bounds, bounds[1:])]
    ranges = list(zip(bounds, bounds[1:]))
    for (s, e), piece in zip(ranges, pieces):
        _put(client, sid, s, e, piece).raise_for_status()

    # 清单顺序打乱：响应仍按起点升序逐条返回。
    entries = [_entry(s, e, p) for (s, e), p in zip(ranges, pieces)][::-1]
    r = _audit(client, sid, entries)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["all_match"] is True
    assert [(x["start"], x["end"]) for x in body["results"]] == ranges
    for result, (s, e), piece in zip(body["results"], ranges, pieces):
        assert result["status"] == "ok"
        assert result["expected_sha256"] == hashlib.sha256(piece).hexdigest()
        assert result["actual_sha256"] == result["expected_sha256"]
        assert result["size_on_disk"] == e - s


def test_audit_mixed_statuses_pinpoint_repair_targets(client: TestClient) -> None:
    """一次诊断同时覆盖 ok / sha256_mismatch / length_mismatch / missing，
    客户端据此确定 repair 目标区间，修复后整文件校验通过。"""
    data = bytes((i * 11 + 5) % 256 for i in range(4000))
    sid = _create(client, data)
    chunks = [(0, 1000), (1000, 2000), (2000, 3000), (3000, 4000)]
    for s, e in chunks:
        _put(client, sid, s, e, data[s:e]).raise_for_status()

    # 制造三种磁盘侧异常：内容损坏（等长）、长度不符、文件缺失。
    storage.part_path(sid, 1000, 2000).write_bytes(b"!" + data[1001:2000])
    storage.part_path(sid, 2000, 3000).write_bytes(data[2000:2500])  # 截短
    storage.part_path(sid, 3000, 4000).unlink()

    entries = [_entry(s, e, data[s:e]) for s, e in chunks]
    r = _audit(client, sid, entries)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["all_match"] is False
    results = body["results"]
    assert [x["status"] for x in results] == [
        "ok",
        "sha256_mismatch",
        "length_mismatch",
        "missing",
    ]
    # 摘要不符：返回磁盘实际摘要，长度正确。
    assert results[1]["actual_sha256"] == hashlib.sha256(b"!" + data[1001:2000]).hexdigest()
    assert results[1]["expected_sha256"] == hashlib.sha256(data[1000:2000]).hexdigest()
    assert results[1]["size_on_disk"] == 1000
    # 长度不符：实际长度与实际内容的摘要一并返回。
    assert results[2]["size_on_disk"] == 500
    assert results[2]["actual_sha256"] == hashlib.sha256(data[2000:2500]).hexdigest()
    # 文件缺失：实际摘要与长度均为 null。
    assert results[3]["actual_sha256"] is None
    assert results[3]["size_on_disk"] is None

    # 按诊断结果圈定 repair 目标（非 ok 的三条），修复后完成整文件校验。
    for x in results:
        if x["status"] != "ok":
            rr = client.put(
                f"/api/v1/sessions/{sid}/chunks/repair?start={x['start']}&end={x['end']}",
                content=data[x["start"]:x["end"]],
            )
            assert rr.status_code == 200, rr.text
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data


def test_audit_subset_of_registered_chunks(client: TestClient) -> None:
    """清单可以是已登记分片的任意子集（不必覆盖全部）。"""
    data = b"s" * 3000
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data[:1000]).raise_for_status()
    _put(client, sid, 1000, 2000, data[1000:2000]).raise_for_status()
    _put(client, sid, 2000, 3000, data[2000:3000]).raise_for_status()

    r = _audit(client, sid, [_entry(1000, 2000, data[1000:2000])])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["all_match"] is True
    assert len(body["results"]) == 1
    assert body["results"][0]["start"] == 1000


def test_audit_accepts_sha256_prefix_and_uppercase(client: TestClient) -> None:
    data = b"p" * 500
    sid = _create(client, data)
    _put(client, sid, 0, 500, data).raise_for_status()
    digest = hashlib.sha256(data).hexdigest()
    r = _audit(client, sid, [{"start": 0, "end": 500, "sha256": "sha256:" + digest.upper()}])
    assert r.status_code == 200, r.text
    result = r.json()["results"][0]
    assert result["status"] == "ok"
    assert result["expected_sha256"] == digest  # 响应中为规范化的小写形式


# ---------------------------------------------------------------------------
# 清单合法性：任一不符整批拒绝
# ---------------------------------------------------------------------------


def test_audit_manifest_must_match_registered_exactly(client: TestClient) -> None:
    data = b"r" * 4000
    sid = _create(client, data)
    _put(client, sid, 1000, 3000, data[1000:3000]).raise_for_status()

    good = _entry(1000, 3000, data[1000:3000])
    for bad_start, bad_end in (
        (0, 1000),      # 完全未登记
        (0, 2000),      # 部分重叠
        (2000, 4000),   # 部分重叠
        (1000, 3001),   # 包含已登记区间但边界更大
        (1000, 2000),   # 已登记区间的子段也不算精确对应
    ):
        r = _audit(client, sid, [good, _entry(bad_start, bad_end, b"x" * (bad_end - bad_start))])
        assert r.status_code == 404, (bad_start, bad_end, r.text)
        detail = r.json()["detail"]
        assert detail["error"] == "no registered chunk matches the range exactly"
        assert detail["start"] == bad_start
        assert detail["end"] == bad_end


def test_audit_duplicate_ranges_rejected(client: TestClient) -> None:
    data = b"d" * 100
    sid = _create(client, data)
    _put(client, sid, 0, 100, data).raise_for_status()
    entry = _entry(0, 100, data)
    r = _audit(client, sid, [entry, dict(entry)])
    assert r.status_code == 422


def test_audit_manifest_size_limits(client: TestClient) -> None:
    data = bytes(320)  # 320 字节，切成 32 片 10 字节
    sid = _create(client, data)
    for i in range(32):
        _put(client, sid, i * 10, (i + 1) * 10, data[i * 10:(i + 1) * 10]).raise_for_status()

    entries = [_entry(i * 10, (i + 1) * 10, data[i * 10:(i + 1) * 10]) for i in range(32)]
    # 恰好 32 条：接受。
    r = _audit(client, sid, entries)
    assert r.status_code == 200, r.text
    assert len(r.json()["results"]) == 32
    # 33 条：超出上限，整批拒绝。
    r = _audit(client, sid, entries + [_entry(0, 10, data[:10])])
    assert r.status_code == 422
    # 空清单：拒绝。
    r = _audit(client, sid, [])
    assert r.status_code == 422


def test_audit_invalid_entry_fields_rejected(client: TestClient) -> None:
    data = b"f" * 100
    sid = _create(client, data)
    _put(client, sid, 0, 100, data).raise_for_status()
    digest = hashlib.sha256(data).hexdigest()

    # start >= end。
    r = _audit(client, sid, [{"start": 100, "end": 100, "sha256": digest}])
    assert r.status_code == 422
    r = _audit(client, sid, [{"start": 200, "end": 100, "sha256": digest}])
    assert r.status_code == 422
    # 负偏移。
    r = _audit(client, sid, [{"start": -1, "end": 100, "sha256": digest}])
    assert r.status_code == 422
    # 摘要格式非法。
    for bad in ("nope", "a" * 63, "g" * 64, "sha256:" + "b" * 63):
        r = _audit(client, sid, [{"start": 0, "end": 100, "sha256": bad}])
        assert r.status_code == 422, bad
    # 缺字段 / 非对象。
    r = _audit(client, sid, [{"start": 0, "end": 100}])
    assert r.status_code == 422
    r = client.post(f"/api/v1/sessions/{sid}/chunks/audit", json={"chunks": {}})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# 会话状态
# ---------------------------------------------------------------------------


def test_audit_unknown_session_is_404(client: TestClient) -> None:
    r = _audit(client, "deadbeef" * 4, [_entry(0, 10, b"0123456789")])
    assert r.status_code == 404


def test_audit_completed_session_is_409(client: TestClient) -> None:
    data = b"c" * 500
    sid = _create(client, data)
    _put(client, sid, 0, 500, data).raise_for_status()
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    r = _audit(client, sid, [_entry(0, 500, data)])
    assert r.status_code == 409


def test_audit_expired_session_is_410(client: TestClient, monkeypatch) -> None:
    data = b"e" * 300
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid = _create(client, data)
    _put(client, sid, 0, 300, data).raise_for_status()
    time.sleep(1.1)
    r = _audit(client, sid, [_entry(0, 300, data)])
    assert r.status_code == 410
    assert not storage.parts_dir(sid).exists()


def test_audit_aborted_session_is_404(client: TestClient) -> None:
    data = b"a" * 200
    sid = _create(client, data)
    _put(client, sid, 0, 200, data).raise_for_status()
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    r = _audit(client, sid, [_entry(0, 200, data)])
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 只读语义：不滑动 TTL、不改登记与已收字节、不留临时文件
# ---------------------------------------------------------------------------


def test_audit_does_not_slide_ttl_or_change_state(client: TestClient) -> None:
    data = bytes((i * 13 + 1) % 256 for i in range(2000))
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data[:1000]).raise_for_status()
    _put(client, sid, 1500, 2000, data[1500:2000]).raise_for_status()
    before = client.get(f"/api/v1/sessions/{sid}").json()

    r = _audit(
        client,
        sid,
        [_entry(0, 1000, data[:1000]), _entry(1500, 2000, data[1500:2000])],
    )
    assert r.status_code == 200, r.text

    after = client.get(f"/api/v1/sessions/{sid}").json()
    assert after["expires_at"] == before["expires_at"]
    assert after["last_access_at"] == before["last_access_at"]
    assert after["bytes_received"] == before["bytes_received"] == 1500
    assert after["missing_ranges"] == before["missing_ranges"]
    # 诊断不产生临时文件。
    assert not list(storage.tmp_dir().glob("*.part"))


# ---------------------------------------------------------------------------
# 并发：诊断全过程持有会话锁
# ---------------------------------------------------------------------------


def test_audit_holds_session_lock_complete_waits(client: TestClient, monkeypatch) -> None:
    """诊断核对磁盘期间（持锁）complete 只能排队；诊断放行后完成正常。"""
    data = bytes((i * 3 + 7) % 256 for i in range(2000))
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data[:1000]).raise_for_status()
    _put(client, sid, 1000, 2000, data[1000:2000]).raise_for_status()

    reached = threading.Event()
    released = threading.Event()
    real_hash = storage.hash_part_file
    used = {"hit": False}

    def paused_hash(session_id, start, end):
        if not used["hit"]:
            used["hit"] = True
            reached.set()
            released.wait(timeout=10)
        return real_hash(session_id, start, end)

    monkeypatch.setattr(storage, "hash_part_file", paused_hash)

    audit_outcome: dict[str, int] = {}

    def do_audit() -> None:
        r = _audit(
            client,
            sid,
            [_entry(0, 1000, data[:1000]), _entry(1000, 2000, data[1000:2000])],
        )
        audit_outcome["code"] = r.status_code

    t_audit = threading.Thread(target=do_audit, daemon=True)
    t_audit.start()
    assert reached.wait(timeout=10), "audit did not reach disk pass"

    complete_outcome: dict[str, int] = {}

    def do_complete() -> None:
        r = client.post(f"/api/v1/sessions/{sid}/complete")
        complete_outcome["code"] = r.status_code

    t_complete = threading.Thread(target=do_complete, daemon=True)
    t_complete.start()
    time.sleep(0.3)
    # 诊断持有会话锁：complete 拿不到锁，不可能在核对中途完成。
    assert "code" not in complete_outcome
    released.set()
    t_audit.join(timeout=10)
    t_complete.join(timeout=10)
    assert not t_audit.is_alive() and not t_complete.is_alive()

    assert audit_outcome["code"] == 200
    assert complete_outcome["code"] == 200
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data


def test_audit_waits_for_repair_commit_and_sees_new_content(
    client: TestClient, monkeypatch
) -> None:
    """修复在事务内替换分片时（持锁）诊断只能等待；放行后读到新内容，
    绝不会把替换中的瞬时状态误报为缺失或损坏。"""
    data = b"o" * 1000
    sid = _create(client, data)
    _put(client, sid, 0, 1000, data).raise_for_status()
    new_content = b"N" * 1000

    commit_entered = threading.Event()
    commit_release = threading.Event()
    real_immediate = db.immediate
    used = {"hit": False}

    @contextmanager
    def signaling_immediate():
        # 修复提交是第一个进入 BEGIN IMMEDIATE 的写事务：此时它已持有会话锁。
        if not used["hit"]:
            used["hit"] = True
            commit_entered.set()
            commit_release.wait(timeout=10)
        with real_immediate() as tx:
            yield tx

    monkeypatch.setattr(db, "immediate", signaling_immediate)

    repair_outcome: dict[str, int] = {}

    def do_repair() -> None:
        r = client.put(
            f"/api/v1/sessions/{sid}/chunks/repair?start=0&end=1000",
            content=new_content,
        )
        repair_outcome["code"] = r.status_code

    t_repair = threading.Thread(target=do_repair, daemon=True)
    t_repair.start()
    assert commit_entered.wait(timeout=10), "repair did not reach commit"

    audit_outcome: dict[str, object] = {}

    def do_audit() -> None:
        r = _audit(client, sid, [_entry(0, 1000, new_content)])
        audit_outcome["code"] = r.status_code
        audit_outcome["body"] = r.json()

    t_audit = threading.Thread(target=do_audit, daemon=True)
    t_audit.start()
    time.sleep(0.3)
    # 修复持锁提交期间，诊断拿不到锁，不可能读到半替换状态。
    assert "code" not in audit_outcome
    commit_release.set()
    t_repair.join(timeout=10)
    t_audit.join(timeout=10)
    assert not t_repair.is_alive() and not t_audit.is_alive()

    assert repair_outcome["code"] == 200
    assert audit_outcome["code"] == 200
    result = audit_outcome["body"]["results"][0]
    # 诊断在修复提交之后执行：读到的是新内容，按新摘要判定为 ok。
    assert result["status"] == "ok"
    assert result["actual_sha256"] == hashlib.sha256(new_content).hexdigest()
    assert storage.part_path(sid, 0, 1000).read_bytes() == new_content
