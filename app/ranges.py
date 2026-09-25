"""成品文件断点下载：单个 HTTP Range 的解析、规范化与流式响应装配。

会话下载（GET /sessions/{id}/download）与凭票下载
（GET /tickets/{ticket}/download）共用本模块，保证两条路径的
Range 语义、响应头与拒绝方式完全一致：

  * 不带 Range：返回完整文件（200 + Accept-Ranges: bytes）。
  * 带且仅带一个合法 bytes 区间：按成品实际长度规范化后只流式输出对应
    字节，返回 206、Content-Range、Content-Length 与 Accept-Ranges。
    支持三种写法（RFC 7233）：
      - 闭区间        bytes=100-199      字节偏移 100..199（半开 [100,200)）
      - 开放结尾      bytes=100-         字节 100 到文件末尾
      - 后缀形式      bytes=-100         文件最后 100 字节
    闭区间起点落在文件内而结尾越界时，按 RFC 收敛到文件末尾。
  * 多范围（逗号多个区间）或无法满足的范围（如起点越过文件末尾）：
    返回 416 Range Not Satisfiable 与 ``Content-Range: bytes */总长度``，
    **只 stat 取长度，绝不打开/读取文件内容**。

Range 规范化与长度判定全部在打开文件之前完成；凭票下载因此可以在
票据扣减事务内调用，范围非法时抛错回滚、绝不扣减次数。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

# 仅接受“单个” bytes 区间：多范围（值中出现逗号）一律无法满足。
# 两种形态分开匹配，避免闭区间规则误吞后缀形式；空白在调用方先行剥除。
_CLOSED_RANGE = re.compile(r"^bytes=([0-9]+)-([0-9]+)$")
_OPEN_ENDED_RANGE = re.compile(r"^bytes=([0-9]+)-$")
_SUFFIX_RANGE = re.compile(r"^bytes=-([0-9]+)$")

# 与存储层其他流式操作一致的分块大小：弱机也不会把成品整段读进内存。
_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class ByteRange:
    """规范化后的字节区间（半开 [start, end)），恒满足 0 <= start < end <= size。"""

    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start


def range_not_satisfiable(total_size: int) -> HTTPException:
    """构造 416 响应：必须携带 Content-Range: bytes */<总长度>，且不读文件。"""
    return HTTPException(
        status_code=416,
        detail={"error": "range not satisfiable"},
        headers={"Content-Range": f"bytes */{total_size}"},
    )


def parse_range_header(raw: str | None, total_size: int) -> ByteRange | None:
    """按成品实际长度规范化 Range 头。

    返回 None 表示请求未携带 Range（调用方应返回完整文件）；
    携带 Range 但为多范围、语法非法或无法满足时，抛 416 且不打开文件。
    """
    if raw is None:
        return None

    # OWS（可选空白）剥除；多范围（含逗号）本服务不支持，直接 416。
    value = raw.strip()
    if "," in value:
        raise range_not_satisfiable(total_size)

    if m := _SUFFIX_RANGE.match(value):
        # 后缀形式：最后 N 字节；N 不小于文件长度时等价于整个文件。
        suffix = int(m.group(1))
        if suffix == 0 or total_size == 0:
            raise range_not_satisfiable(total_size)
        start = max(0, total_size - suffix)
        return ByteRange(start, total_size)

    if m := _OPEN_ENDED_RANGE.match(value):
        # 开放结尾：bytes=start- → 到文件末尾。
        start = int(m.group(1))
        if start >= total_size:
            raise range_not_satisfiable(total_size)
        return ByteRange(start, total_size)

    m = _CLOSED_RANGE.match(value)
    if m is None:
        raise range_not_satisfiable(total_size)

    # 闭区间：bytes=start-end，end 为最后一个字节的偏移（含）。
    start, last = int(m.group(1)), int(m.group(2))
    if last < start or start >= total_size:
        # 区间倒置，或起点越过文件末尾（含空文件的任何区间请求）：无法满足。
        raise range_not_satisfiable(total_size)
    # 结尾越界按 RFC 收敛到文件末尾。
    return ByteRange(start, min(last + 1, total_size))


def _content_disposition(filename: str) -> str:
    """构造与原 FileResponse 同形的附件文件名头（ASCII 名直出，UTF-8 名走 filename*）。"""
    if filename.isascii():
        return f'attachment; filename="{filename}"'
    return f"attachment; filename*=utf-8''{quote(filename)}"


def _iter_file_segment(path: Path, start: int, end: int):
    """流式产出 [start, end) 字节；范围解析已保证区间合法，全程不把文件读进内存。"""
    remaining = end - start
    with path.open("rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(_CHUNK_SIZE, remaining))
            if not block:
                # 成品在响应期间被并发截断：已发送部分无法收回，停止即可，
                # 绝不越界读取（区间在打开文件前已按当时长度裁剪）。
                break
            yield block
            remaining -= len(block)


def build_download_response(path: Path, filename: str, raw_range: str | None):
    """装配会话下载/凭票下载共用的成品响应。

    长度只取自 ``Path.stat()``：范围解析与 416 判定均在打开文件之前，
    多范围/越界请求不会读取任何文件内容。
    """
    total_size = path.stat().st_size
    byte_range = parse_range_header(raw_range, total_size)
    disposition = _content_disposition(filename)

    if byte_range is None:
        return StreamingResponse(
            _iter_file_segment(path, 0, total_size),
            status_code=200,
            media_type="application/octet-stream",
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(total_size),
                "Content-Disposition": disposition,
            },
        )

    return StreamingResponse(
        _iter_file_segment(path, byte_range.start, byte_range.end),
        status_code=206,
        media_type="application/octet-stream",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(byte_range.length),
            "Content-Range": (
                f"bytes {byte_range.start}-{byte_range.end - 1}/{total_size}"
            ),
            "Content-Disposition": disposition,
        },
    )
