"""创建上传会话的幂等键支持。

客户端在 ``POST /api/v1/sessions`` 上可携带 ``Idempotency-Key`` 头
（1～128 个可见 ASCII 字符，0x21–0x7E）。服务端**不**保存键明文，
只保存键的 SHA-256 与规范化请求指纹（同样是 SHA-256）：

- 相同键 + 相同请求（首次响应丢失后的重试、并发重试）→ 返回首次创建的会话；
- 相同键 + 不同请求（filename/total_size/sha256/chunk_size 任一不同）→ 409，
  且不新建会话。

键记录与会话在同一个 BEGIN IMMEDIATE 事务中写入，会话删除时经外键级联删除。
"""
from __future__ import annotations

import hashlib
import json
import re

# 可见 ASCII：0x21(!) 到 0x7E(~)，不含空格、制表符等空白与控制字符。
_KEY_RE = re.compile(r"[!-~]{1,128}")

KEY_REQUIREMENT = "1-128 visible ASCII characters (0x21-0x7E)"


def is_valid_key(raw: str) -> bool:
    """键格式校验：1～128 个可见 ASCII 字符。"""
    return bool(_KEY_RE.fullmatch(raw))


def key_hash(key: str) -> str:
    """键的 SHA-256（十六进制）；明文键绝不落库。"""
    return hashlib.sha256(key.encode("ascii")).hexdigest()


def request_fingerprint(
    *, filename: str, total_size: int, sha256: str, chunk_size: int
) -> str:
    """规范化请求指纹：对四个语义字段做确定性序列化后取 SHA-256。

    入参必须是校验/规范化后的值（filename 已去空白、sha256 已小写化并去掉
    ``sha256:`` 前缀），因此语义相同的请求得到同一指纹。
    """
    canonical = json.dumps(
        {
            "filename": filename,
            "total_size": total_size,
            "sha256": sha256,
            "chunk_size": chunk_size,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
