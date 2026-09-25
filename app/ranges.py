"""成品文件断点下载：单个 HTTP Range 的解析、规范化与流式响应装配。

会话下载（GET /sessions/{id}/download）与凭票下载
（GET /tickets/{ticket}/download）共用本模块，保证两条路径的
Range 语义、响应头、拒绝方式以及与删除会话的并发口径完全一致：

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

与删除会话的竞态由调用方与 DELETE 共用同一会话锁、再配合本模块的
“固定成品（pin）”两步消除：

  1. :func:`pin_artifact` 先 ``stat`` 取长度并完成 416 判定（不打开
     文件，越界/多范围不获得句柄），随后才打开成品并用打开对象自身的
     长度重新规范化 Range、固定 Content-Length；调用方全程持有会话锁，
     删除会话拿不到锁，因此拿到的句柄必然对应一份完整、可读的成品。
  2. 锁随后释放、响应从这份已打开句柄流式发送：POSIX 下文件即使被
     删除（unlink），已打开的文件描述符仍可读到完整 inode 内容，
     “下载先取得成品”时即使会话随后被删，也必然按声明长度送完。

句柄的释放由流式迭代器自身的 ``finally`` 保证：正常送完与客户端断开
（Starlette 取消迭代）都会关闭它，与删除是否移走文件无关。这里必须
使用**原生 async 迭代器**而非同步迭代器——同步生成器在 anyio 线程池
中被取消时其 ``finally`` 不会运行（线程池取消不关闭同步生成器），会
泄漏已打开的文件描述符；异步生成器在任务取消时会被 aclose，``finally``
必然执行。文件自身的 seek/read 仅微秒级，直接在事件循环中调用即可，
不需要丢到线程池。
"""
from __future__ import annotations

import io
import re
from collections.abc import AsyncIterator
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


@dataclass
class PinnedArtifact:
    """一份在会话锁内“固定”下来的可读成品。

    ``handle`` 是已打开的成品文件对象；``total_size`` 与 ``byte_range``
    都取自该句柄自身的长度/Range 规范化，与磁盘路径此后是否被删除
    无关——响应必须且只能从这份对象流式发送。``byte_range`` 为 None
    表示完整下载（200），否则为 206 的半开区间。
    """

    handle: io.BufferedReader
    total_size: int
    byte_range: ByteRange | None

    @property
    def content_length(self) -> int:
        return self.total_size if self.byte_range is None else self.byte_range.length

    def close(self) -> None:
        """放弃这份成品（未能构造响应时）：关闭句柄。幂等。"""
        self.handle.close()


def pin_artifact(path: Path, raw_range: str | None) -> PinnedArtifact:
    """在会话锁内固定本次下载要发送的成品与区间。

    必须由调用方在持有该会话锁时调用（锁使删除会话在此期间无法移走
    成品）。分两步：

      1. ``stat`` 取长度并解析 Range：多范围/越界（416）在此拒绝，
         **不打开文件、不读取内容**（与既有语义一致）；
      2. 打开成品，随即以**打开句柄自身**的长度重新规范化 Range，把
         状态码、Range 与 Content-Length 全部锚定到这同一份可读对象。
         文件在两步之间消失（``is_file`` 已通过却无法打开）时抛
         FileNotFoundError，由调用方按“无成品”拒绝（凭票下载此时尚在
         扣减事务内，回滚即不扣次）。

    成功返回后句柄所有权移交调用方：必须用它构造响应（响应结束自动
    关闭），或在放弃时调用 :meth:`PinnedArtifact.close`。
    """
    # 第一步：仅凭路径长度做 416 判定，绝不打开/读取成品内容。
    total_size = path.stat().st_size
    parse_range_header(raw_range, total_size)

    # 第二步：打开成品，之后一切以这份已打开对象为准。
    handle = path.open("rb")
    try:
        open_size = handle.seek(0, io.SEEK_END)  # 以打开对象自身长度为准
        handle.seek(0)
        if open_size != total_size:
            # 持锁期间成品不可变（唯一的替换者 complete 与 delete 都要
            # 同一把锁）；长度不一致只可能来自外部干预，按消失处理，
            # 绝不按过时的 stat 长度声明 Content-Length。
            raise FileNotFoundError(
                f"artifact size changed while pinning: {total_size} -> {open_size}"
            )
        byte_range = parse_range_header(raw_range, open_size)
    except BaseException:
        handle.close()
        raise
    return PinnedArtifact(handle=handle, total_size=open_size, byte_range=byte_range)


async def _iter_pinned_segment(pinned: PinnedArtifact) -> AsyncIterator[bytes]:
    """从已固定的成品句柄流式产出区间字节，结束（含客户端断开）必关句柄。

    原生 async 生成器：
      * 正常送完时自然走到 ``finally``；
      * 客户端断开时 Starlette 取消响应任务，异步生成器被 ``aclose()``，
        ``finally`` 同样执行——已打开句柄必然释放，不会随并发删除泄漏
        文件描述符。
    成品的 seek/read 只在本地文件上执行、耗时微秒级，直接在事件循环
    中调用即可；不经过 anyio 线程池（同步迭代器在其中被取消时不会
    执行生成器 ``finally``）。
    """
    handle = pinned.handle
    start = 0 if pinned.byte_range is None else pinned.byte_range.start
    remaining = pinned.content_length
    try:
        handle.seek(start)
        while remaining > 0:
            block = handle.read(min(_CHUNK_SIZE, remaining))
            if not block:
                # 句柄已在会话锁内固定到完整 inode，正常不会提前 EOF；
                # 即便极端情况下内容被外部截断，已声明的响应头无法收回，
                # 停止即可，绝不越界读取。
                break
            yield block
            remaining -= len(block)
    finally:
        # 正常送完与客户端断开（迭代被取消、生成器 aclose）都在此释放
        # 句柄——文件是否已被删除会话 unlink 与此无关。
        handle.close()


def build_pinned_response(pinned: PinnedArtifact, filename: str) -> StreamingResponse:
    """从已固定的可读成品装配会话下载/凭票下载共用的流式响应。

    状态码、Content-Length、Content-Range 全部取自 ``pinned`` 中那份
    已打开对象，因此响应一旦开始，删除会话移走磁盘文件也不影响发送。
    """
    disposition = _content_disposition(filename)

    if pinned.byte_range is None:
        return StreamingResponse(
            _iter_pinned_segment(pinned),
            status_code=200,
            media_type="application/octet-stream",
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(pinned.content_length),
                "Content-Disposition": disposition,
            },
        )

    byte_range = pinned.byte_range
    return StreamingResponse(
        _iter_pinned_segment(pinned),
        status_code=206,
        media_type="application/octet-stream",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(byte_range.length),
            "Content-Range": f"bytes {byte_range.start}-{byte_range.end - 1}/"
            f"{pinned.total_size}",
            "Content-Disposition": disposition,
        },
    )
