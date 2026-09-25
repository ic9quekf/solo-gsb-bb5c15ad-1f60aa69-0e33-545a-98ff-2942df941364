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
    返回 416 Range Not Satisfiable 与 ``Content-Range: bytes */总长度``。

并发安全（与 DELETE /sessions/{id} 的竞态）：调用方必须先持有会话锁，
在锁内完成会话/隔离判定、按路径预检 416（只 stat 取长度、绝不打开文件
读取内容），随后**打开成品句柄并以句柄 fstat 的长度**规范化 Range 与
Content-Length。状态码、Range 与 Content-Length 一旦在
:func:`build_pinned_response` 中确定，就与这份已打开的成品绑定；
随后释放会话锁、由句柄流式发送，即使删除会话移走目录项，POSIX 已打开
inode 依旧可读，服务端必然按声明长度送完。删除先拿到锁时，预检即按
“无成品”语义拒绝（404/416 按总长度），不会进入句柄阶段。

句柄生命周期与流式响应相同：正常送完与客户端断开都会进入生成器
``finally``，在 shielded 区间经同一把锁（等待在途读取结束）后于工作线程
关闭；StreamingResponse 的 ``BackgroundTask`` 再兜底关闭一次（幂等）。
"""
from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import BinaryIO
from urllib.parse import quote

from anyio import CancelScope
from anyio.to_thread import run_sync
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

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


async def _stream_pinned_segment(handle: BinaryIO, start: int, end: int):
    """从**已打开**句柄异步流式产出 [start, end) 字节，结束（含断开）必关句柄。

    所有句柄操作（seek/read/close）都经同一把 :class:`anyio.Lock` 串行后
    丢到工作线程：读期间客户端断开会取消排队等待，``finally`` 在
    shielded 区间获取同一把锁后再关闭句柄——任何在途读取先结束、close
    随后执行，不存在 read/close 竞争同一 fd，也不存在 fd 延迟到 GC 才释放。
    读路径不再经过文件路径，成品随后被删除会话移走也不影响本句柄。
    """
    lock = asyncio.Lock()

    async def read_block(length: int) -> bytes | None:
        # 锁与 run_sync 都在 shield 内等待：取消只发生在两次读取之间的
        # yield 点（StreamingResponse 向客户端发送块时），取消一个正在
        # 等锁的读协程不会使锁泄漏（asyncio.Lock 对取消获取安全）。
        with CancelScope(shield=True):
            async with lock:
                return await run_sync(handle.read, length)

    with CancelScope(shield=True):
        await run_sync(handle.seek, start)

    remaining = end - start
    try:
        while remaining > 0:
            block = await read_block(min(_CHUNK_SIZE, remaining))
            if not block:
                # 已发布成品不可变，正常情况下不会发生；绝不越界读取或静默补零。
                raise IOError("pinned artifact ended before declared Content-Length")
            yield block
            remaining -= len(block)
    finally:
        # 传输结束与客户端断开都到这里：shield + 同一把锁，等待在途读取
        # （若有）完成后再关句柄；句柄在工作线程关闭，绝不阻塞事件循环。
        with CancelScope(shield=True):
            async with lock:
                await run_sync(handle.close)


def build_pinned_response(
    handle: BinaryIO,
    total_size: int,
    filename: str,
    raw_range: str | None,
) -> StreamingResponse:
    """从**已打开且固定**的成品句柄装配下载响应。

    句柄由调用方在会话锁内打开，``total_size`` 必须是该句柄 fstat 得到的
    长度：状态码（200/206）、Content-Range、Content-Length 全部取自这同
    一份成品，装配完成后调用方即可释放会话锁流式发送。句柄所有权随响应
    转移，由流式生成器在结束时（正常送完/客户端断开）关闭；本函数在发送
    前抛错（目前仅可能是 416）时自行关闭句柄，不泄漏。
    """
    # 句柄已固定，Range 再按句柄长度归一化一次：与 Content-Length 同源，
    # 消除“预检 stat 与句柄 fstat 之间”的任何差异窗口。
    try:
        byte_range = parse_range_header(raw_range, total_size)
    except BaseException:
        handle.close()
        raise
    disposition = _content_disposition(filename)

    if byte_range is None:
        return StreamingResponse(
            _stream_pinned_segment(handle, 0, total_size),
            status_code=200,
            media_type="application/octet-stream",
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(total_size),
                "Content-Disposition": disposition,
            },
            background=BackgroundTask(handle.close),
        )

    return StreamingResponse(
        _stream_pinned_segment(handle, byte_range.start, byte_range.end),
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
        background=BackgroundTask(handle.close),
    )
