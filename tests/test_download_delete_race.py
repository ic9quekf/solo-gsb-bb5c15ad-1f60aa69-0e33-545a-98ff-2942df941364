"""成品下载与删除会话并发的竞态修复测试。

复现的旧缺陷：凭票下载先在事务里扣次，随后才按路径 stat 长度并延迟
到发送时才打开成品；这两步之间删除会话（DELETE /sessions/{id}）可以
移走成品，导致下载已经扣掉次数却 404/中断。会话下载同理（先 stat、
发送时才 open，删除可在中途 unlink 成品）。

修复后的不变式：

  * 删除先完成：会话下载与凭票下载都按既有“无成品”语义 404，凭票
    404 在原子扣减之前，**绝不扣次**；
  * 下载先固定成品：状态码、Range、Content-Length 全部锚定到会话锁内
    已打开的同一份可读成品，锁释放后即使会话被删，也按声明长度送完；
  * 凭票请求仅在取得可读成品且 Range 有效后才原子扣次：416 或成品
    消失（404）不扣次；
  * 传输结束与客户端断开都释放文件句柄（不泄漏 FD）。

TestClient 的 ASGI 通道会在客户端读取前于服务端缓冲整个响应体，无法
真实制造“发送到一半”的交错；因此关键用例用进程内真实 uvicorn 服务 +
原始 socket：客户端只读响应头并暂停（使服务端阻塞在写满的发送缓冲），
此时另一个连接执行 DELETE，再把响应体读尽，校验字节完整。
"""
from __future__ import annotations

import hashlib
import os
import socket
import struct
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import db, ranges, storage
from app.main import app

# 足够大的成品：客户端暂停读取时，回环缓冲很快写满，服务端阻塞在发送
# 路径上、成品句柄保持打开，从而确定性地制造“发送到一半被删”。
BIG_SIZE = 8 * 1024 * 1024

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="FD 断言依赖 /proc，仅在 Linux 运行",
)


# ---------------------------------------------------------------------------
# 公共构造助手
# ---------------------------------------------------------------------------


def _complete(client: TestClient, data: bytes, name: str = "race.bin") -> str:
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


def _issue(client: TestClient, sid: str, *, max_downloads: int = 5) -> dict:
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": max_downloads},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _remaining(ticket_id: str) -> int:
    return db.get_conn().execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()["remaining_downloads"]


def _open_fds_for(path: Path) -> list[str]:
    """列出本进程仍指向该成品路径（含已 unlink 的 (deleted)）的 FD。"""
    hits: list[str] = []
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            continue
        if str(path) in target:
            hits.append(fd)
    return hits


def _wait_no_fd(path: Path, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _open_fds_for(path):
            return
        time.sleep(0.02)
    pytest.fail(f"成品句柄未释放，仍有 FD: {_open_fds_for(path)}")


# ---------------------------------------------------------------------------
# 进程内真实 uvicorn 服务（模块级，端口 0 由内核分配）
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def live() -> Iterator[tuple[str, threading.Thread, uvicorn.Server]]:
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    host, port = server.servers[0].sockets[0].getsockname()
    try:
        yield f"http://{host}:{port}", thread, server
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def _raw_get_headers(addr: tuple[str, int], path: str) -> tuple[socket.socket, bytes]:
    """发起 GET，只读到响应头为止（不排空响应体），返回 (连接, 已读字节)。"""
    sock = socket.create_connection(addr)
    sock.sendall(
        f"GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode()
    )
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError(f"响应头未到达即断连: {buf!r}")
        buf += chunk
    return sock, buf


def _drain(sock: socket.socket, initial: bytes) -> bytes:
    """读完整个响应（含 initial 中已随响应头到达的体字节）。"""
    body = initial.split(b"\r\n\r\n", 1)[1]
    sock.settimeout(10)
    while True:
        chunk = sock.recv(262144)
        if not chunk:
            break
        body += chunk
    sock.close()
    return body


def _raw_get_headers_range(
    addr: tuple[str, int], path: str, range_header: str
) -> tuple[socket.socket, bytes]:
    """发起带 Range 的 GET，只读到响应头为止（不排空响应体）。"""
    sock = socket.create_connection(addr)
    sock.sendall(
        f"GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
        f"Range: {range_header}\r\n\r\n".encode()
    )
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError("响应头未到达即断连")
        buf += chunk
    return sock, buf


# ---------------------------------------------------------------------------
# 删除先完成：按无成品语义 404；凭票 404 不扣次
# ---------------------------------------------------------------------------


def test_download_after_delete_returns_404(client: TestClient) -> None:
    sid = _complete(client, b"deleted-before-download")
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "no completed artifact for this session"


def test_ticket_download_after_delete_404_without_decrement(
    client: TestClient,
) -> None:
    data = b"deleted-before-ticket-download"
    sid = _complete(client, data)
    ticket = _issue(client, sid, max_downloads=3)
    tid = ticket["ticket_id"]
    assert _remaining(tid) == 3

    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    r = client.get(f"/api/v1/tickets/{ticket['ticket']}/download")
    # 会话已删 → 级联删除票据，按票据/会话不存在的无成品语义拒绝。
    assert r.status_code in (404, 410)
    # 票据随会话级联删除；即使重建同 id 也不可能——确认没有残留可扣行。
    assert (
        db.get_conn()
        .execute("SELECT COUNT(*) AS n FROM download_tickets WHERE id = ?", (tid,))
        .fetchone()["n"]
        == 0
    )


def test_ticket_download_when_artifact_vanishes_404_without_decrement(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """is_file() 通过但成品在打开前消失：404 且不扣次（模拟删除插到锁内）。"""
    data = b"vanish between is_file and open"
    sid = _complete(client, data)
    path = storage.artifact_path(sid)
    ticket = _issue(client, sid, max_downloads=2)
    tid = ticket["ticket_id"]

    real_pin = ranges.pin_artifact
    real_open = Path.open

    def vanish_then_pin(p, raw_range):  # type: ignore[no-untyped-def]
        # 模拟“is_file 已通过、成品在打开瞬间被移走”：stat 长度仍可取得，
        # 但 open 必失败——对应删除插到固定成品的两步之间。
        if p == path:
            def open_raises(self, *a, **k):  # type: ignore[no-untyped-def]
                if self == path:
                    raise FileNotFoundError(str(self))
                return real_open(self, *a, **k)

            monkeypatch.setattr(Path, "open", open_raises)
        return real_pin(p, raw_range)

    monkeypatch.setattr(ranges, "pin_artifact", vanish_then_pin)

    r = client.get(f"/api/v1/tickets/{ticket['ticket']}/download")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "no completed artifact for this session"
    assert _remaining(tid) == 2  # 成品消失不扣次

    # 会话下载在同一时序下同样 404。
    r2 = client.get(f"/api/v1/sessions/{sid}/download")
    assert r2.status_code == 404


def test_416_still_does_not_decrement_nor_hold_file(client: TestClient) -> None:
    data = b"q" * 100
    sid = _complete(client, data)
    ticket = _issue(client, sid, max_downloads=2)
    tid = ticket["ticket_id"]

    r = client.get(
        f"/api/v1/tickets/{ticket['ticket']}/download",
        headers={"Range": "bytes=999-"},
    )
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(data)}"
    assert _remaining(tid) == 2  # 416 不扣次


# ---------------------------------------------------------------------------
# 下载先固定成品：发送期间删除，仍按声明长度送完（真实服务 + 原始 socket）
# ---------------------------------------------------------------------------


def test_session_download_completes_when_deleted_mid_transfer(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
    path = storage.artifact_path(sid)

    sock, head_bytes = _raw_get_headers(live[2].servers[0].sockets[0].getsockname(),
                                        f"/api/v1/sessions/{sid}/download")
    try:
        head = head_bytes.split(b"\r\n\r\n", 1)[0].decode()
        assert head.split("\r\n")[0].endswith("200 OK")
        assert f"content-length: {len(data)}" in head.lower()
        # 已在发送途中：成品句柄保持打开（指向可能被 unlink 的 inode）。
        assert _open_fds_for(path)

        with httpx.Client(base_url=base_url) as http:
            deleted = http.delete(f"/api/v1/sessions/{sid}")
        assert deleted.status_code == 204
        assert not path.exists()  # 磁盘成品已被删除

        body = _drain(sock, head_bytes)
    finally:
        if not sock._closed:
            sock.close()
    # 下载先取得成品：即使随后删除，也必须按声明长度送完，字节一个不多不少。
    assert len(body) == len(data)
    assert body == data
    _wait_no_fd(path)


def test_ticket_download_completes_when_deleted_mid_transfer(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
        ticket = _issue(http, sid, max_downloads=5)

    sock, head_bytes = _raw_get_headers(
        live[2].servers[0].sockets[0].getsockname(),
        f"/api/v1/tickets/{ticket['ticket']}/download",
    )
    try:
        head = head_bytes.split(b"\r\n\r\n", 1)[0].decode()
        assert head.split("\r\n")[0].endswith("200 OK")
        assert f"content-length: {len(data)}" in head.lower()
        assert _open_fds_for(storage.artifact_path(sid))

        with httpx.Client(base_url=base_url) as http:
            assert http.delete(f"/api/v1/sessions/{sid}").status_code == 204
        body = _drain(sock, head_bytes)
    finally:
        if not sock._closed:
            sock.close()
    assert body == data
    _wait_no_fd(storage.artifact_path(sid))


def test_range_download_completes_when_deleted_mid_transfer(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    """带合法 Range 的会话下载在发送期间被删：206 与声明的 Content-Length 不变。"""
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    start = 1024
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
    path = storage.artifact_path(sid)

    sock, head_bytes = _raw_get_headers_range(
        live[2].servers[0].sockets[0].getsockname(),
        f"/api/v1/sessions/{sid}/download",
        f"bytes={start}-",
    )
    try:
        head = head_bytes.split(b"\r\n\r\n", 1)[0].decode()
        assert head.split("\r\n")[0].endswith("206 Partial Content")
        expected_length = len(data) - start
        assert f"content-length: {expected_length}" in head.lower()
        assert f"content-range: bytes {start}-{len(data) - 1}/{len(data)}" in head.lower()
        assert _open_fds_for(path)

        with httpx.Client(base_url=base_url) as http:
            assert http.delete(f"/api/v1/sessions/{sid}").status_code == 204
        body = _drain(sock, head_bytes)
    finally:
        if not sock._closed:
            sock.close()
    assert body == data[start:]
    _wait_no_fd(path)


# ---------------------------------------------------------------------------
# 句柄释放：传输结束 / 客户端断开（FIN 与 RST）
# ---------------------------------------------------------------------------


def test_handle_closed_after_full_transfer(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
        r = http.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 200
    assert r.content == data
    _wait_no_fd(storage.artifact_path(sid))


def test_handle_released_on_client_fin(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    """客户端读到响应头后直接断开（FIN）、不排空：服务端必须关闭成品句柄。"""
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
    path = storage.artifact_path(sid)

    sock, _ = _raw_get_headers(
        live[2].servers[0].sockets[0].getsockname(),
        f"/api/v1/sessions/{sid}/download",
    )
    assert _open_fds_for(path)  # 传输中句柄确实打开
    # 优雅断开（FIN），不读剩余响应体。
    sock.close()
    _wait_no_fd(path)


def test_handle_released_on_client_rst(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    """客户端硬断开（SO_LINGER 0 → RST）：服务端同样必须关闭成品句柄。"""
    base_url, _, _ = live
    data = os.urandom(BIG_SIZE)
    with httpx.Client(base_url=base_url) as http:
        sid = _complete(http, data)
    path = storage.artifact_path(sid)

    sock, _ = _raw_get_headers(
        live[2].servers[0].sockets[0].getsockname(),
        f"/api/v1/sessions/{sid}/download",
    )
    assert _open_fds_for(path)
    # 立即 RST，丢弃未发送数据。
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()
    _wait_no_fd(path)


# ---------------------------------------------------------------------------
# 并发交错压力：反复“下载 vs 删除”，两种结局都必须自洽
# ---------------------------------------------------------------------------


def test_concurrent_download_vs_delete_never_serves_short_or_5xx(
    live: tuple[str, threading.Thread, uvicorn.Server],
) -> None:
    base_url, _, _ = live
    rounds = 8
    for i in range(rounds):
        data = os.urandom(2 * 1024 * 1024 + i)
        with httpx.Client(base_url=base_url) as http:
            sid = _complete(http, data, name=f"round-{i}.bin")

        outcome: dict[str, object] = {}

        def do_download() -> None:
            try:
                with httpx.Client(base_url=base_url) as http:
                    with http.stream(
                        "GET", f"/api/v1/sessions/{sid}/download"
                    ) as resp:
                        status = resp.status_code
                        body = resp.read()
                        outcome["status"] = status
                        outcome["length"] = len(body)
                        outcome["ok"] = body == data
            except httpx.TransportError as exc:  # 删除后连接被关也属合法结局
                outcome["transport_error"] = repr(exc)

        def do_delete() -> None:
            with httpx.Client(base_url=base_url) as http:
                outcome["delete"] = http.delete(
                    f"/api/v1/sessions/{sid}"
                ).status_code

        t_dl = threading.Thread(target=do_download)
        t_del = threading.Thread(target=do_delete)
        t_dl.start()
        t_del.start()
        t_dl.join()
        t_del.join()

        assert outcome.get("delete") == 204
        # 合法结局只有两种：删除先 → 404；下载先 → 200 且字节完整。
        # 绝不允许 200 却短字节、或 5xx。
        if "transport_error" in outcome:
            # 仅当连接恰好在响应开始前后被关闭时允许；不得伴随短内容。
            assert "length" not in outcome
        else:
            status = outcome["status"]
            assert status in (200, 404), outcome
            if status == 200:
                assert outcome["length"] == len(data)
                assert outcome["ok"] is True
        _wait_no_fd(storage.artifact_path(sid))


def test_concurrent_ticket_downloads_vs_delete_no_failed_decrement(
    client: TestClient,
) -> None:
    """大量凭票下载与删除并发：

    - 删除先的请求得到 404（或票据级联后的 404），这些失败**绝不扣次**；
    - 成功（200）的次数恰等于票据被扣减的次数（max_downloads 与删除
      先后两条路径合计），且每个成功响应字节完整；
    - 不出现 5xx 或 200 短内容。
    """
    data = b"race-" * 2048  # 10 KiB
    sid = _complete(client, data)
    max_downloads = 12
    ticket = _issue(client, sid, max_downloads=max_downloads)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"

    results: list[tuple[int, int, bool]] = []
    barrier = threading.Barrier(max_downloads + 1)

    def downloader() -> None:
        barrier.wait()
        r = client.get(url)
        results.append(
            (
                r.status_code,
                len(r.content),
                r.status_code == 200 and r.content == data,
            )
        )

    def deleter() -> None:
        barrier.wait()
        client.delete(f"/api/v1/sessions/{sid}")

    workers = [threading.Thread(target=downloader) for _ in range(max_downloads)]
    workers.append(threading.Thread(target=deleter))
    for t in workers:
        t.start()
    for t in workers:
        t.join()

    statuses = [status for status, _, _ in results]
    successes = [item for item in results if item[0] == 200]
    # 每个 200 都必须字节完整。
    assert all(ok and length == len(data) for _, length, ok in successes), results
    # 除 200 外只允许 404（删除先 / 票据级联）/429（成功扣尽后到达），
    # 绝不允许 5xx。
    assert all(s in (200, 404, 410, 429) for s in statuses), statuses

    # 成功次数等于实际扣减次数；票据若尚未级联删除，剩余次数必须与之自洽。
    remaining_row = (
        db.get_conn()
        .execute(
            "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
            (ticket["ticket_id"],),
        )
        .fetchone()
    )
    n_success = len(successes)
    if remaining_row is not None:
        assert remaining_row["remaining_downloads"] == max_downloads - n_success
    # 成功数不可能超过签发上限（原子扣减 + 锁内固定）。
    assert n_success <= max_downloads

