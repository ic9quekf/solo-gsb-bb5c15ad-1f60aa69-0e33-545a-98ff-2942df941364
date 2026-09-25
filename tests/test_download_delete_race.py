"""成品下载与删除会话并发竞态的回归测试。

修复前的竞态：凭票下载先扣次，再按路径重新 stat 取长度并延迟到发送时才
打开文件；会话下载同样按路径取长度后延迟打开。删除会话可在“扣次/定长”
与“打开文件”之间移走成品，导致下载失败却已经扣掉票据次数。

修复后的两条不变式（本文件强制）：
  1. 下载先取得成品：会话锁内打开句柄并据此确定状态码/Range/Content-Length，
     随后即使 DELETE 排队完成并移走目录项，已打开 inode 仍按声明长度送完；
     凭票下载仅在此时才扣次。
  2. 删除先完成：下载在锁内按现有“无成品”语义拒绝（会话 404；票据 404），
     416 与成品消失发生在扣次之前时一律不扣次。

句柄在传输结束与客户端断开时都必须释放（fd 不泄漏）。
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import os
import threading
import time
from collections.abc import Callable

from fastapi.testclient import TestClient

from app import db, ranges, storage


def _completed_session(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    """上传完整文件并完成，返回 session_id。"""
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
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200
    return sid


class _PinGate:
    """在 storage.open_artifact 周围插一对事件，制造确定性时序。

    下载线程在会话锁内打开句柄（pinned 置位）后阻塞在 release；
    DELETE 线程此时已在会话锁外排队。置 release 后下载放开锁、装配响应，
    排队的 DELETE 随即取得锁移走成品——而下载仍从句柄流式发送。
    """

    def __init__(self, monkeypatch) -> None:
        self.pinned = threading.Event()
        self.release = threading.Event()
        original_open = storage.open_artifact

        def gated_open(session_id: str):
            handle = original_open(session_id)
            self.pinned.set()
            assert self.release.wait(10), "download never released from pin gate"
            return handle

        monkeypatch.setattr(storage, "open_artifact", gated_open)

    def pin_then_race_delete(
        self, client: TestClient, sid: str, download: Callable
    ) -> dict:
        """跑下载线程；句柄固定后让 DELETE 在锁上排队，再放开发送。

        返回 {"download": 下载响应, "delete_status": 删除状态码}。
        """
        outcome: dict = {}

        def download_thread() -> None:
            outcome["download"] = download()

        def delete_thread() -> None:
            outcome["delete_status"] = client.delete(
                f"/api/v1/sessions/{sid}"
            ).status_code

        tdl = threading.Thread(target=download_thread)
        tdl.start()
        assert self.pinned.wait(10), "download never pinned the artifact"
        tdel = threading.Thread(target=delete_thread)
        tdel.start()
        # 给 DELETE 一点时间走到会话锁上排队（随后下载一放锁它就赢）。
        time.sleep(0.2)
        self.release.set()
        tdl.join(timeout=10)
        tdel.join(timeout=10)
        assert not tdl.is_alive() and not tdel.is_alive()
        return outcome


def _remaining(ticket_id: str) -> int:
    row = db.get_conn().execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()
    return 0 if row is None else row["remaining_downloads"]


# ---------------------------------------------------------------------------
# 下载先取得成品：随后删除也按声明长度送完
# ---------------------------------------------------------------------------


def test_session_download_pinned_then_delete_streams_full(
    client: TestClient, monkeypatch
) -> None:
    data = os.urandom(3 * 1024 * 1024 + 7)  # 跨多个 1 MiB 分块
    sid = _completed_session(client, data, name="big.bin")
    gate = _PinGate(monkeypatch)

    outcome = gate.pin_then_race_delete(
        client, sid, lambda: client.get(f"/api/v1/sessions/{sid}/download")
    )

    assert outcome["delete_status"] == 204
    assert not storage.artifact_path(sid).exists()
    r = outcome["download"]
    assert r.status_code == 200
    assert r.headers["content-length"] == str(len(data))
    assert r.content == data  # 目录项已删，仍按声明长度送完同一 inode
    # 删除完成后新的下载请求按无成品语义拒绝。
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404


def test_ticket_download_pinned_then_delete_streams_range(
    client: TestClient, monkeypatch
) -> None:
    data = os.urandom(3 * 1024 * 1024 + 7)
    sid = _completed_session(client, data, name="big.bin")
    issued = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 2},
    ).json()
    ticket = issued["ticket"]
    gate = _PinGate(monkeypatch)

    outcome = gate.pin_then_race_delete(
        client,
        sid,
        lambda: client.get(
            f"/api/v1/tickets/{ticket}/download",
            headers={"Range": "bytes=1048576-"},
        ),
    )

    assert outcome["delete_status"] == 204
    assert not storage.artifact_path(sid).exists()
    r = outcome["download"]
    # 删除在流式发送期间完成：206 + 声明长度 + 完整区间字节，次数不回退。
    assert r.status_code == 206
    assert r.headers["content-length"] == str(len(data) - 1048576)
    assert r.headers["content-range"].startswith(
        f"bytes 1048576-{len(data) - 1}/"
    )
    assert r.content == data[1048576:]


def test_ticket_full_download_pinned_then_delete_succeeds(
    client: TestClient, monkeypatch
) -> None:
    data = b"z" * (2 * 1024 * 1024 + 13)
    sid = _completed_session(client, data, name="big.bin")
    issued = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 1},
    ).json()
    ticket = issued["ticket"]
    gate = _PinGate(monkeypatch)

    outcome = gate.pin_then_race_delete(
        client, sid, lambda: client.get(f"/api/v1/tickets/{ticket}/download")
    )

    assert outcome["delete_status"] == 204
    r = outcome["download"]
    assert r.status_code == 200
    assert r.content == data


# ---------------------------------------------------------------------------
# 删除先完成：按现有无成品语义拒绝
# ---------------------------------------------------------------------------


def test_delete_first_session_download_404(client: TestClient) -> None:
    sid = _completed_session(client, b"gone")
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "no completed artifact for this session"
    # 删除幂等后依旧 404。
    client.delete(f"/api/v1/sessions/{sid}")
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404


def test_delete_first_ticket_download_404(client: TestClient) -> None:
    sid = _completed_session(client, b"gone")
    issued = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 3},
    ).json()
    ticket = issued["ticket"]
    client.delete(f"/api/v1/sessions/{sid}")
    # 票据行随会话级联删除：等同票据不存在，404；没有“扣次”可言。
    r = client.get(f"/api/v1/tickets/{ticket}/download")
    assert r.status_code == 404


class _VanishingPath:
    """会话行检查通过后、stat/open 成品时表现为文件已消失的路径替身。"""

    def __init__(self, real) -> None:
        self._real = real

    def stat(self):
        raise FileNotFoundError()

    def open(self, *a, **k):
        raise FileNotFoundError()

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_artifact_vanishes_after_row_check_404_without_decrement(
    client: TestClient, monkeypatch
) -> None:
    """会话行仍在但成品在锁内预检 stat 时消失：404 且不扣次。"""
    sid = _completed_session(client, b"vanish")
    issued = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 2},
    ).json()
    ticket, ticket_id = issued["ticket"], issued["ticket_id"]

    real_artifact = storage.artifact_path(sid)
    monkeypatch.setattr(
        storage, "artifact_path", lambda sid_: _VanishingPath(real_artifact)
    )

    r = client.get(f"/api/v1/tickets/{ticket}/download")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "no completed artifact for this session"
    assert _remaining(ticket_id) == 2  # 成品消失不扣次


def test_session_download_artifact_vanishes_404(client: TestClient, monkeypatch) -> None:
    sid = _completed_session(client, b"vanish")
    real_artifact = storage.artifact_path(sid)
    monkeypatch.setattr(
        storage, "artifact_path", lambda sid_: _VanishingPath(real_artifact)
    )
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 416 在扣次之前且不打开文件；句柄在传输结束/客户端断开时释放
# ---------------------------------------------------------------------------


def test_ticket_416_does_not_open_or_decrement(client: TestClient, monkeypatch) -> None:
    sid = _completed_session(client, b"x" * 100)
    issued = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": 2},
    ).json()
    ticket, ticket_id = issued["ticket"], issued["ticket_id"]

    opened = threading.Event()
    original_open = storage.open_artifact

    def must_not_open(session_id: str):
        opened.set()
        return original_open(session_id)

    monkeypatch.setattr(storage, "open_artifact", must_not_open)

    r = client.get(
        f"/api/v1/tickets/{ticket}/download", headers={"Range": "bytes=9999-"}
    )
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */100"
    assert not opened.is_set()  # 416 只 stat，绝不打开成品
    assert _remaining(ticket_id) == 2  # 不扣次，随后仍可下载满额


def test_handle_closed_after_full_transfer(client: TestClient) -> None:
    sid = _completed_session(client, b"q" * 128)
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 200 and r.content == b"q" * 128
    # 响应结束后成品仍可正常打开（前一句柄已释放，无残留锁定/泄漏）。
    with storage.artifact_path(sid).open("rb") as fh:
        assert fh.read() == b"q" * 128


def test_generator_closes_handle_on_cancellation() -> None:
    """客户端断开（异步生成器 aclose）必须关闭已固定句柄。"""

    async def scenario() -> None:
        handle = io.BytesIO(b"y" * (512 * 1024))
        gen = ranges._stream_pinned_segment(handle, 0, 512 * 1024)
        first = await gen.__anext__()
        assert first
        assert not handle.closed
        await gen.aclose()  # 等价于传输中途客户端断开
        assert handle.closed

    asyncio.run(scenario())


def test_generator_closes_handle_on_range_cancellation() -> None:
    async def scenario() -> None:
        handle = io.BytesIO(b"z" * (512 * 1024))
        gen = ranges._stream_pinned_segment(handle, 10, 512 * 1024)
        await gen.__anext__()
        await gen.aclose()
        assert handle.closed

    asyncio.run(scenario())


def test_pinned_handle_survives_unlink_posix(tmp_path) -> None:
    """POSIX 保证：已打开文件 unlink 后句柄仍可按原长度读完（修复的底层依据）。"""
    path = tmp_path / "artifact"
    payload = b"pinned-bytes" * 100
    path.write_bytes(payload)
    fh = path.open("rb")
    size = os.fstat(fh.fileno()).st_size
    path.unlink()
    assert not path.exists()
    assert fh.read() == payload  # 目录项消失，inode 与数据仍在
    assert size == len(payload)
    fh.close()
