"""成品断点下载测试：单 Range 的三种形态、416 语义、票据仅有效范围扣次。

覆盖两条下载路径：
  * 会话下载 GET /sessions/{id}/download（不计数）；
  * 凭票下载 GET /tickets/{ticket}/download（合法范围原子扣减一次，
    范围非法或成品隔离时不扣次）。
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import db, ranges, storage


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


def _issue(client: TestClient, sid: str, *, max_downloads: int = 3):
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"expires_in_minutes": 60, "max_downloads": max_downloads},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _remaining(client: TestClient, ticket_id: str) -> int:
    return db.get_conn().execute(
        "SELECT remaining_downloads FROM download_tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()["remaining_downloads"]


# ---------------------------------------------------------------------------
# Range 头规范化（纯函数单元测试，不接触 HTTP/文件）
# ---------------------------------------------------------------------------


def test_parse_range_none_when_header_absent() -> None:
    assert ranges.parse_range_header(None, 1000) is None


@pytest.mark.parametrize(
    "header,total,expected",
    [
        # 闭区间
        ("bytes=0-99", 1000, (0, 100)),
        ("bytes=100-199", 1000, (100, 200)),
        ("bytes=00-0099", 1000, (0, 100)),  # 前导 0 合法
        # 开放结尾
        ("bytes=100-", 1000, (100, 1000)),
        ("bytes=0-", 1000, (0, 1000)),
        ("bytes=999-", 1000, (999, 1000)),
        # 后缀形式
        ("bytes=-100", 1000, (900, 1000)),
        ("bytes=-1", 1000, (999, 1000)),
        ("bytes=-1000", 1000, (0, 1000)),  # 后缀不短于文件 → 整个文件
        ("bytes=-5000", 1000, (0, 1000)),
        # 闭区间结尾越界 → 收敛到文件末尾（仍为 206）
        ("bytes=900-9999", 1000, (900, 1000)),
        ("bytes=999-9999", 1000, (999, 1000)),
        # 空白（OWS）剥除
        ("  bytes=10-19  ", 1000, (10, 20)),
    ],
)
def test_parse_range_single_forms(
    header: str, total: int, expected: tuple[int, int]
) -> None:
    parsed = ranges.parse_range_header(header, total)
    assert parsed is not None
    assert (parsed.start, parsed.end) == expected
    assert parsed.length == expected[1] - expected[0]


@pytest.mark.parametrize(
    "header,total",
    [
        # 多范围（逗号）
        ("bytes=0-99,200-299", 1000),
        ("bytes=0-99, 100-199", 1000),
        # 越界
        ("bytes=1000-1099", 1000),  # 起点恰等于长度
        ("bytes=5000-", 1000),  # 开放结尾起点越界
        ("bytes=200-100", 1000),  # 闭区间倒置
        # 语法非法
        ("bytes=abc-", 1000),
        ("bytes=-abc", 1000),
        ("bytes=10-9-", 1000),
        ("bytes=10--9", 1000),
        ("items=0-99", 1000),  # 非 bytes 单位
        ("bytes 0-99", 1000),
        ("bytes=", 1000),
        ("bytes=-", 1000),
        ("bytes=-0", 1000),  # 后缀零长度无法满足
        # 空成品上的任何区间
        ("bytes=0-0", 0),
        ("bytes=0-", 0),
        ("bytes=-1", 0),
    ],
)
def test_parse_range_unsatisfiable_raises_416(header: str, total: int) -> None:
    with pytest.raises(HTTPException) as excinfo:
        ranges.parse_range_header(header, total)
    assert excinfo.value.status_code == 416
    # 416 必须携带 Content-Range: bytes */<总长度>
    assert excinfo.value.headers == {"Content-Range": f"bytes */{total}"}


# ---------------------------------------------------------------------------
# 会话下载：完整文件与 206
# ---------------------------------------------------------------------------


def test_session_download_full_without_range(client: TestClient) -> None:
    data = bytes(range(256)) * 8  # 2048 字节
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers["content-length"] == str(len(data))
    assert "content-range" not in r.headers  # 200 不带 Content-Range
    assert "attachment" in r.headers["content-disposition"]


def test_session_download_closed_range_206(client: TestClient) -> None:
    data = bytes(range(256)) * 8
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=100-199"})
    assert r.status_code == 206
    assert r.content == data[100:200]
    assert r.headers["content-range"] == f"bytes 100-199/{len(data)}"
    assert r.headers["content-length"] == "100"
    assert r.headers["accept-ranges"] == "bytes"


def test_session_download_open_ended_range_206(client: TestClient) -> None:
    data = b"0123456789abcdef" * 4
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=50-"})
    assert r.status_code == 206
    assert r.content == data[50:]
    assert r.headers["content-range"] == f"bytes 50-{len(data) - 1}/{len(data)}"
    assert r.headers["content-length"] == str(len(data) - 50)


def test_session_download_suffix_range_206(client: TestClient) -> None:
    data = b"".join(bytes([i % 256]) for i in range(300))
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=-64"})
    assert r.status_code == 206
    assert r.content == data[-64:]
    assert r.headers["content-range"] == f"bytes 236-299/{len(data)}"
    assert r.headers["content-length"] == "64"


def test_session_download_suffix_larger_than_file(client: TestClient) -> None:
    data = b"abcdefgh"
    sid = _completed_session(client, data)
    # 后缀长度不小于成品长度时，按 RFC 等价于整个文件（仍返回 206）。
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=-100"})
    assert r.status_code == 206
    assert r.content == data
    assert r.headers["content-range"] == f"bytes 0-7/{len(data)}"


def test_session_download_clamped_end(client: TestClient) -> None:
    data = b"abcdefghij"
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=8-999"})
    assert r.status_code == 206
    assert r.content == data[8:]
    assert r.headers["content-range"] == f"bytes 8-9/{len(data)}"
    assert r.headers["content-length"] == "2"


def test_session_download_large_range_streams_all_bytes(client: TestClient) -> None:
    # 大于单个 1 MiB 分块，验证流式循环而非一次读完。
    data = os.urandom(3 * 1024 * 1024 + 123)
    sid = _completed_session(client, data, name="large.bin")
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=1048576-"})
    assert r.status_code == 206
    assert r.content == data[1048576:]
    assert int(r.headers["content-length"]) == len(data) - 1048576


# ---------------------------------------------------------------------------
# 会话下载：416（多范围/越界），不读取文件内容
# ---------------------------------------------------------------------------


def _assert_416(r, total: int) -> None:
    assert r.status_code == 416
    assert r.json()["detail"]["error"] == "range not satisfiable"
    assert r.headers["content-range"] == f"bytes */{total}"


@pytest.mark.parametrize(
    "header",
    [
        "bytes=0-99,200-299",  # 多范围
        "bytes=9999-",  # 开放结尾越界
        "bytes=9999-10099",  # 闭区间越界
        "bytes=garbage",  # 语法非法
    ],
)
def test_session_download_bad_range_416(
    client: TestClient, header: str
) -> None:
    data = b"x" * 100
    sid = _completed_session(client, data)
    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": header})
    _assert_416(r, len(data))
    # 416 绝不返回任何成品字节：响应体只是统一的 JSON 错误描述。
    assert b"x" * 100 not in r.content


@pytest.mark.skipif(os.geteuid() == 0, reason="root 绕过文件权限位，无法验证不读内容")
def test_416_does_not_open_file_contents(client: TestClient, tmp_path: Path) -> None:
    """越界/多范围在取长度后即拒绝：文件内容不可读也必须照常 416。"""
    data = b"secret bytes"
    sid = _completed_session(client, data)
    path = storage.artifact_path(sid)
    os.chmod(path, 0o000)  # 内容不可读（stat 仍可取长度）
    try:
        r = client.get(
            f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=999-"}
        )
        assert r.status_code == 416
        assert r.headers["content-range"] == f"bytes */{len(data)}"
    finally:
        os.chmod(path, 0o644)


def test_empty_file_full_download_ok_but_range_416(client: TestClient) -> None:
    sid = _completed_session(client, b"", name="empty.bin")
    full = client.get(f"/api/v1/sessions/{sid}/download")
    assert full.status_code == 200
    assert full.content == b""
    assert full.headers["content-length"] == "0"

    r = client.get(f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=-1"})
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */0"


def test_session_download_range_after_quarantine_423(client: TestClient) -> None:
    data = b"quarantine me"
    sid = _completed_session(client, data)
    storage.artifact_path(sid).unlink()
    check = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert check.status_code == 200
    assert check.json()["quarantined"] is True
    # 隔离优先于 Range：合法 Range 同样被拒，且不读文件。
    r = client.get(
        f"/api/v1/sessions/{sid}/download", headers={"Range": "bytes=0-3"}
    )
    assert r.status_code == 423
    assert r.json()["detail"]["error"] == "session quarantined"


# ---------------------------------------------------------------------------
# 凭票下载：有效范围原子扣减一次；非法范围/隔离不扣次
# ---------------------------------------------------------------------------


def test_ticket_full_and_ranges_each_decrement_once(client: TestClient) -> None:
    data = bytes(range(256)) * 16  # 4096
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=5)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"
    tid = ticket["ticket_id"]
    assert _remaining(client, tid) == 5

    r = client.get(url, headers={"Range": "bytes=100-199"})
    assert r.status_code == 206
    assert r.content == data[100:200]
    assert r.headers["content-range"] == f"bytes 100-199/{len(data)}"
    assert r.headers["accept-ranges"] == "bytes"
    assert _remaining(client, tid) == 4  # 合法范围扣减一次

    r = client.get(url, headers={"Range": "bytes=4000-"})  # 开放结尾
    assert r.status_code == 206
    assert r.content == data[4000:]
    assert _remaining(client, tid) == 3

    r = client.get(url, headers={"Range": "bytes=-96"})  # 后缀
    assert r.status_code == 206
    assert r.content == data[-96:]
    assert _remaining(client, tid) == 2

    r = client.get(url)  # 完整下载
    assert r.status_code == 200
    assert r.content == data
    assert _remaining(client, tid) == 1

    r = client.get(url)
    assert r.status_code == 200
    assert _remaining(client, tid) == 0
    assert client.get(url).status_code == 429


@pytest.mark.parametrize(
    "header",
    [
        "bytes=0-99,100-199",  # 多范围
        "bytes=9999-",  # 开放结尾越界
        "bytes=9999-10000",  # 闭区间起点越界
        "bytes=not-a-range",  # 语法非法
        "bytes=-0",
    ],
)
def test_ticket_invalid_range_416_without_decrement(
    client: TestClient, header: str
) -> None:
    data = b"t" * 256
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=2)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"
    tid = ticket["ticket_id"]

    r = client.get(url, headers={"Range": header})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(data)}"
    # 关键不变式：范围非法绝不扣次，随后仍可正常下载两次。
    assert _remaining(client, tid) == 2

    ok = client.get(url, headers={"Range": "bytes=0-9"})
    assert ok.status_code == 206
    assert ok.content == b"t" * 10
    assert _remaining(client, tid) == 1

    # 第二次 416 仍不扣次（此时只剩 1 次，验证不会把仅剩次数吃掉）。
    again = client.get(url, headers={"Range": "bytes=999-"})
    assert again.status_code == 416
    assert _remaining(client, tid) == 1
    last = client.get(url)
    assert last.status_code == 200
    assert _remaining(client, tid) == 0
    assert client.get(url).status_code == 429


def test_ticket_range_on_quarantined_session_423_without_decrement(
    client: TestClient,
) -> None:
    data = b"do not serve me"
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=3)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"
    tid = ticket["ticket_id"]

    storage.artifact_path(sid).unlink()
    check = client.post(f"/api/v1/sessions/{sid}/integrity-check")
    assert check.json()["quarantined"] is True

    # 即使携带合法 Range，隔离拒绝也优先，且在扣减之前。
    for header in (None, "bytes=0-3", "bytes=0-", "bytes=-5", "bytes=9999-"):
        headers = {"Range": header} if header is not None else {}
        r = client.get(url, headers=headers)
        assert r.status_code == 423, (header, r.status_code)
        assert r.json()["detail"]["error"] == "session quarantined"
        assert _remaining(client, tid) == 3  # 一次都不扣


def test_ticket_empty_file_range_416_without_decrement(client: TestClient) -> None:
    sid = _completed_session(client, b"", name="empty.bin")
    ticket = _issue(client, sid, max_downloads=2)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"
    tid = ticket["ticket_id"]

    r = client.get(url, headers={"Range": "bytes=-1"})
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */0"
    assert _remaining(client, tid) == 2

    ok = client.get(url)
    assert ok.status_code == 200
    assert ok.content == b""
    assert _remaining(client, tid) == 1


def test_ticket_revoked_range_request_403_without_decrement(client: TestClient) -> None:
    data = b"revoked"
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=2)
    url = f"/api/v1/tickets/{ticket['ticket']}/download"
    client.post(f"/api/v1/tickets/{ticket['ticket']}/revoke")

    # 票据状态校验先于 Range 解析：合法/非法 Range 都不会让撤销票据扣次。
    for header in ("bytes=0-1", "bytes=9999-"):
        r = client.get(url, headers={"Range": header})
        assert r.status_code == 403
        assert _remaining(client, ticket["ticket_id"]) == 2


def test_ticket_416_response_has_no_artifact_bytes(client: TestClient) -> None:
    data = b"ABCD" * 64
    sid = _completed_session(client, data)
    ticket = _issue(client, sid, max_downloads=1)
    r = client.get(
        f"/api/v1/tickets/{ticket['ticket']}/download",
        headers={"Range": "bytes=0-99,300-399"},
    )
    assert r.status_code == 416
    assert r.json()["detail"]["error"] == "range not satisfiable"
    assert b"ABCD" not in r.content  # 只有 JSON 错误描述，没有任何成品字节
    assert _remaining(client, ticket["ticket_id"]) == 1
