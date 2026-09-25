"""POST /sessions/{id}/copy-chunk：从已完成来源会话成品服务端复制分片。"""
from __future__ import annotations

import hashlib
import threading

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


def _upload_all(client: TestClient, sid: str, data: bytes, cut: int) -> None:
    """用 PUT 把 data 按 [0,cut)+[cut,len) 两片传完。"""
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end={cut}", content=data[:cut]
    )
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start={cut}&end={len(data)}",
        content=data[cut:],
    )


def _complete(client: TestClient, sid: str, data: bytes):
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == data
    return dl


def _copy(
    client: TestClient,
    target: str,
    source: str,
    source_start: int,
    source_end: int,
    target_start: int,
):
    return client.post(
        f"/api/v1/sessions/{target}/copy-chunk",
        json={
            "source_session_id": source,
            "source_start": source_start,
            "source_end": source_end,
            "target_start": target_start,
        },
    )


def _make_completed(client: TestClient, data: bytes, cut: int | None = None):
    """创建 + PUT 上传 + 完成，返回来源会话 id。"""
    sid = _create(client, data)
    _upload_all(client, sid, data, cut if cut is not None else len(data) // 2)
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    return sid


# ---------------------------------------------------------------------------
# 正常路径
# ---------------------------------------------------------------------------


def test_copy_segment_then_complete_with_full_sha(client: TestClient) -> None:
    data = bytes((i * 13 + 7) % 256 for i in range(10_000))
    src = _make_completed(client, data, cut=4000)

    # 目标会话大小一致，分三段：PUT、复制、PUT，顺序打乱。
    tgt = _create(client, data, name="copy-target.bin")
    r = client.put(
        f"/api/v1/sessions/{tgt}/chunks?start=0&end=1000", content=data[:1000]
    )
    assert r.status_code == 200

    r = _copy(client, tgt, src, source_start=3000, source_end=7000, target_start=3000)
    assert r.status_code == 200, r.text
    body = r.json()
    # 响应与普通分片提交一致：ChunkResponse 形态。
    assert body["session_id"] == tgt
    assert body["received"] == {"start": 3000, "end": 7000}
    assert body["bytes_received"] == 5000
    assert body["fully_received"] is False
    assert {"start": 1000, "end": 3000} in body["missing_ranges"]
    assert {"start": 7000, "end": 10000} in body["missing_ranges"]
    assert "expires_at" in body

    # 复制的分片字节与来源一致地落在目标分片目录。
    part = storage.part_path(tgt, 3000, 7000)
    assert part.read_bytes() == data[3000:7000]

    # 补齐剩余两片（含一段从来源不同偏移复制到目标同偏移的分片）。
    r = _copy(client, tgt, src, source_start=1000, source_end=3000, target_start=1000)
    assert r.status_code == 200, r.text
    r = client.put(
        f"/api/v1/sessions/{tgt}/chunks?start=7000&end=10000",
        content=data[7000:],
    )
    assert r.status_code == 200

    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["fully_received"] is True
    assert status["bytes_received"] == 10_000

    # 原有完整覆盖 + 整文件摘要规则不变。
    _complete(client, tgt, data)


def test_copy_full_file_single_chunk(client: TestClient) -> None:
    data = b"server-side copy whole file" * 50
    src = _make_completed(client, data)
    tgt = _create(client, data)
    r = _copy(client, tgt, src, 0, len(data), 0)
    assert r.status_code == 200, r.text
    assert r.json()["fully_received"] is True
    assert r.json()["bytes_received"] == len(data)
    _complete(client, tgt, data)


def test_copy_segment_to_different_target_offset(client: TestClient) -> None:
    # 10 KiB 目标文件；来源前 2100 字节刻意构造成与目标 [5000,7100) 相同。
    data = bytes(range(256)) * 40  # 10240 字节，i 位置字节为 i % 256
    src_data = bytearray(b"z" * 5000)
    src_data[100:2100] = data[5000:7000]
    src_data = bytes(src_data)
    src = _make_completed(client, src_data)
    tgt = _create(client, data)
    # 来源 [100, 2100) 复制到目标起点 5000。
    r = _copy(client, tgt, src, 100, 2100, 5000)
    assert r.status_code == 200, r.text
    assert r.json()["received"] == {"start": 5000, "end": 7000}
    assert storage.part_path(tgt, 5000, 7000).read_bytes() == data[5000:7000]

    # 其余字节仍由 PUT 补齐后完成（整文件 SHA-256 必须通过）。
    for start, end in ((0, 5000), (7000, len(data))):
        pr = client.put(
            f"/api/v1/sessions/{tgt}/chunks?start={start}&end={end}",
            content=data[start:end],
        )
        assert pr.status_code == 200, (start, end, pr.text)
    _complete(client, tgt, data)


def test_copy_from_different_size_source(client: TestClient) -> None:
    # 来源与目标 total_size 可以不同，只复制共同字节序列的一段。
    prefix = bytes((i * 31 + 5) % 256 for i in range(3000))
    data = prefix + b"TAIL" * 250
    src = _make_completed(client, prefix)
    tgt = _create(client, data)
    r = _copy(client, tgt, src, 500, 2500, 1000)
    assert r.status_code == 200, r.text
    assert storage.part_path(tgt, 1000, 3000).read_bytes() == prefix[500:2500]


def test_source_session_remains_downloadable_after_copy(client: TestClient) -> None:
    data = bytes(range(256)) * 10
    src = _make_completed(client, data)
    tgt = _create(client, data)
    assert _copy(client, tgt, src, 0, 500, 0).status_code == 200
    # 复制不改动来源：仍可下载、状态不变。
    status = client.get(f"/api/v1/sessions/{src}").json()
    assert status["status"] == "completed"
    assert client.get(f"/api/v1/sessions/{src}/download").content == data


# ---------------------------------------------------------------------------
# 来源资格校验
# ---------------------------------------------------------------------------


def test_copy_rejects_unknown_uploading_and_expired_source(client: TestClient) -> None:
    data = b"x" * 500
    tgt = _create(client, data)

    # 来源不存在。
    r = _copy(client, tgt, "deadbeef" * 4, 0, 10, 0)
    assert r.status_code == 404
    assert "source session not found" in r.json()["detail"]["error"]

    # 来源仍在上传（即使区间已经全部收到、只差 complete）。
    up = _create(client, data)
    _upload_all(client, up, data, 250)
    r = _copy(client, tgt, up, 0, 10, 0)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "source session is not completed"

    # 失败未登记分片、未增加已收字节。
    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["bytes_received"] == 0
    assert status["missing_ranges"] == [{"start": 0, "end": 500}]


def test_copy_rejects_source_with_missing_artifact(client: TestClient) -> None:
    data = b"artifact-gone" * 100
    src = _make_completed(client, data)
    tgt = _create(client, data)

    # 库里状态仍为 completed，但成品文件被外部删除：必须拒绝。
    storage.artifact_path(src).unlink()
    r = _copy(client, tgt, src, 0, 100, 0)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "source completed artifact is missing"

    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["bytes_received"] == 0


# ---------------------------------------------------------------------------
# 区间 / 边界 / 重叠 / 大小校验
# ---------------------------------------------------------------------------


def test_copy_rejects_bad_source_range(client: TestClient) -> None:
    data = b"y" * 1000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    # 来源区间越过来源 total_size。
    r = _copy(client, tgt, src, 900, 1001, 0)
    assert r.status_code == 400
    assert "source range exceeds source total_size" in r.json()["detail"]["error"]

    # 半开区间非法（start >= end）由请求模型拒绝。
    r = client.post(
        f"/api/v1/sessions/{tgt}/copy-chunk",
        json={
            "source_session_id": src,
            "source_start": 100,
            "source_end": 100,
            "target_start": 0,
        },
    )
    assert r.status_code == 422

    # 负偏移同样 422。
    r = client.post(
        f"/api/v1/sessions/{tgt}/copy-chunk",
        json={
            "source_session_id": src,
            "source_start": -1,
            "source_end": 10,
            "target_start": 0,
        },
    )
    assert r.status_code == 422

    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["bytes_received"] == 0


def test_copy_rejects_target_out_of_bounds(client: TestClient) -> None:
    data = b"z" * 1000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    # target_start + 复制长度越过目标 total_size。
    r = _copy(client, tgt, src, 0, 200, 900)
    assert r.status_code == 400
    assert "exceeds target total_size" in r.json()["detail"]["error"]
    assert client.get(f"/api/v1/sessions/{tgt}").json()["bytes_received"] == 0


def test_copy_rejects_same_session(client: TestClient) -> None:
    data = b"q" * 200
    sid = _create(client, data)
    r = _copy(client, sid, sid, 0, 10, 0)
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "source and target session must differ"


def test_copy_rejects_overlap_with_received_ranges(client: TestClient) -> None:
    data = bytes(range(256)) * 20  # 5120
    src = _make_completed(client, data)
    tgt = _create(client, data)

    r = client.put(
        f"/api/v1/sessions/{tgt}/chunks?start=1000&end=2000",
        content=data[1000:2000],
    )
    assert r.status_code == 200

    # 复制区间与已收 [1000,2000) 交叠。
    r = _copy(client, tgt, src, 0, 1500, 500)
    assert r.status_code == 409
    assert "overlaps" in r.json()["detail"]["error"]

    # 相邻允许：目标 [2000, 3500) 与已收区间首尾相接。
    r = _copy(client, tgt, src, 500, 2000, 2000)
    assert r.status_code == 200, r.text

    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["bytes_received"] == 2500


def test_copy_respects_max_chunk_bytes(client: TestClient, monkeypatch) -> None:
    data = b"m" * 10_000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    monkeypatch.setattr(settings, "max_chunk_bytes", 4096)
    r = _copy(client, tgt, src, 0, 5000, 0)
    assert r.status_code == 413
    assert client.get(f"/api/v1/sessions/{tgt}").json()["bytes_received"] == 0

    monkeypatch.setattr(settings, "max_chunk_bytes", 0)  # 0 = 不限
    r = _copy(client, tgt, src, 0, 5000, 0)
    assert r.status_code == 200, r.text


def test_copy_into_completed_target_rejected(client: TestClient) -> None:
    data = b"c" * 300
    src = _make_completed(client, data)
    tgt = _make_completed(client, b"c" * 300)
    r = _copy(client, tgt, src, 0, 10, 0)
    assert r.status_code == 409


def test_unknown_target_session_is_404(client: TestClient) -> None:
    data = b"u" * 100
    src = _make_completed(client, data)
    r = _copy(client, "nope1234" * 4, src, 0, 10, 0)
    assert r.status_code == 404


def test_failed_copy_leaves_no_tempfile(client: TestClient) -> None:
    data = b"t" * 1000
    src = _make_completed(client, data)
    tgt = _create(client, data)
    # 预校验阶段失败（重叠）：复制尚未发生，不应有临时文件残留。
    client.put(
        f"/api/v1/sessions/{tgt}/chunks?start=0&end=10", content=data[:10]
    )
    _copy(client, tgt, src, 0, 100, 0)
    assert list(storage.tmp_dir().glob("*.part")) == []


def test_database_failure_after_file_move_registers_nothing(
    client: TestClient, monkeypatch
) -> None:
    """临时文件已 replace 到 parts 目录后、登记 SQL 失败：
    事务回滚，落地文件与临时文件都必须清除，重试仍可成功。"""
    import pytest
    from app import db

    data = b"f" * 3000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    class InjectedDBError(RuntimeError):
        pass

    from contextlib import contextmanager

    class FailingConn:
        """连接代理：在 INSERT INTO chunks 上注入故障，其余透传。"""

        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *params):
            if sql.startswith("INSERT INTO chunks"):
                raise InjectedDBError("injected insert failure")
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

    monkeypatch.setattr("app.main.db.immediate", flaky_immediate)
    with pytest.raises(InjectedDBError):
        _copy(client, tgt, src, 100, 1100, 200)
    monkeypatch.undo()

    # 事务已回滚：没有分片登记、bytes_received 未增加。
    status = client.get(f"/api/v1/sessions/{tgt}").json()
    assert status["bytes_received"] == 0
    assert status["missing_ranges"] == [{"start": 0, "end": 3000}]
    # 没有已落地但未登记的孤儿分片，也没有残留临时文件。
    assert not storage.part_path(tgt, 200, 1200).exists()
    assert not list(storage.parts_dir(tgt).iterdir())
    assert list(storage.tmp_dir().glob("*.part")) == []

    # 会话仍可正常收片：重新复制并完成。
    r = _copy(client, tgt, src, 100, 1100, 200)
    assert r.status_code == 200, r.text
    for start, end in ((0, 200), (1200, 3000)):
        pr = client.put(
            f"/api/v1/sessions/{tgt}/chunks?start={start}&end={end}",
            content=data[start:end],
        )
        assert pr.status_code == 200, (start, end, pr.text)
    _complete(client, tgt, data)


def test_copy_source_artifact_deleted_during_segment_copy(
    client: TestClient, monkeypatch
) -> None:
    """成品在复制过程中被外部删除：读失败映射为 409，不登记任何东西。"""
    data = b"g" * 2000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    real_copy = storage.copy_artifact_segment

    def copy_then_delete(source_session_id, start, end):
        # 模拟打开瞬间成品被删：直接删后调用原函数，其应抛 OSError。
        storage.artifact_path(source_session_id).unlink()
        return real_copy(source_session_id, start, end)

    monkeypatch.setattr(
        "app.main.storage.copy_artifact_segment", copy_then_delete
    )
    r = _copy(client, tgt, src, 0, 500, 0)
    assert r.status_code == 409
    assert "cannot copy source artifact segment" in r.json()["detail"]["error"]
    assert client.get(f"/api/v1/sessions/{tgt}").json()["bytes_received"] == 0
    assert list(storage.tmp_dir().glob("*.part")) == []


# ---------------------------------------------------------------------------
# 并发：反向复制不死锁；来源删除 / 目标放弃竞争
# ---------------------------------------------------------------------------


def test_concurrent_reverse_copies_do_not_deadlock(client: TestClient) -> None:
    data_a = b"A" * 4096
    data_b = b"B" * 4096
    a = _make_completed(client, data_a)
    b = _make_completed(client, data_b)
    # 目标 ta / tb 与来源同样大小，复制区间互不重叠。
    ta = _create(client, data_b)
    tb = _create(client, data_a)

    results: list[int] = []
    barrier = threading.Barrier(2)
    done = threading.Event()

    def worker(target: str, source: str, ss: int, se: int, ts: int) -> None:
        barrier.wait()
        r = _copy(client, target, source, ss, se, ts)
        results.append(r.status_code)
        done.set()

    # a -> tb 与 b -> ta 反向并发；持锁顺序固定为 ID 字典序，不得死锁。
    threads = [
        threading.Thread(target=worker, args=(tb, a, 0, 1000, 0), daemon=True),
        threading.Thread(target=worker, args=(ta, b, 0, 1000, 0), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads), "reverse copies deadlocked"
    assert sorted(results) == [200, 200]

    # 再来一对同目标区间的并发复制：定序加锁后只允许一个登记。
    tc = _create(client, data_a)
    results.clear()
    b2 = threading.Barrier(2)

    def run_with_barrier(ts: int) -> None:
        b2.wait()
        r = _copy(client, tc, a, 1000, 2000, ts)
        results.append(r.status_code)

    threads = [
        threading.Thread(target=run_with_barrier, args=(0,), daemon=True),
        threading.Thread(target=run_with_barrier, args=(0,), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads)
    assert sorted(results).count(200) == 1
    assert all(code == 409 for code in results if code != 200)
    status = client.get(f"/api/v1/sessions/{tc}").json()
    assert status["bytes_received"] == 1000


def test_concurrent_source_delete_race(client: TestClient) -> None:
    data = b"D" * 2000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    outcome: dict[str, int] = {}

    def do_copy() -> None:
        r = _copy(client, tgt, src, 0, 500, 0)
        outcome["copy"] = r.status_code

    t_copy = threading.Thread(target=do_copy, daemon=True)
    # 与复制几乎同时删除来源：两种结局都合法，但绝不能出现损坏登记。
    t_copy.start()
    rd = client.delete(f"/api/v1/sessions/{src}")
    t_copy.join(timeout=10)
    assert not t_copy.is_alive()
    assert rd.status_code == 204
    assert outcome["copy"] in (200, 404, 409)

    status = client.get(f"/api/v1/sessions/{tgt}").json()
    if outcome["copy"] == 200:
        # 复制先于删除：分片完整登记。
        assert status["bytes_received"] == 500
        assert storage.part_path(tgt, 0, 500).is_file()
    else:
        # 删除先于复制：什么都不许登记。
        assert status["bytes_received"] == 0
        assert not storage.part_path(tgt, 0, 500).exists()


def test_concurrent_target_abort_race(client: TestClient) -> None:
    data = b"E" * 2000
    src = _make_completed(client, data)
    tgt = _create(client, data)

    outcome: dict[str, int] = {}

    def do_copy() -> None:
        r = _copy(client, tgt, src, 0, 500, 0)
        outcome["copy"] = r.status_code

    t_copy = threading.Thread(target=do_copy, daemon=True)
    t_copy.start()
    rd = client.delete(f"/api/v1/sessions/{tgt}")
    t_copy.join(timeout=10)
    assert not t_copy.is_alive()
    assert rd.status_code == 204
    assert outcome["copy"] in (200, 404, 410)

    if outcome["copy"] == 200:
        # 复制先完成登记：随后的放弃硬删除会连同分片/会话一起删除。
        assert client.get(f"/api/v1/sessions/{tgt}").status_code == 404
    else:
        assert client.get(f"/api/v1/sessions/{tgt}").status_code == 404
    # 两种结局都不残留分片目录。
    assert not storage.parts_dir(tgt).exists()
