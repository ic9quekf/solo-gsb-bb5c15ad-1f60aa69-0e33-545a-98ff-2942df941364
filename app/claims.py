"""分片认领（chunk claim）支持。

上传中会话可通过 ``POST /api/v1/sessions/{id}/chunk-claims`` 认领一个
**尚未接收**的半开区间 ``[start, end)``，服务端返回一次性随机令牌
（默认 5 分钟有效，``CHUNK_CLAIM_TTL_SECONDS`` 可调）：

- 同一时刻相交的有效（未过期、未消费）认领最多一个；过期或被消费后
  同一区间可重新认领；
- ``PUT /chunks`` 携带 ``X-Chunk-Claim: <claim_id>.<token>`` 头提交
  被认领区间：令牌匹配且区间与认领**完全一致**才接受，成功登记即在
  同一事务中消费（删除）认领；失败或断流不消费；
- 与下载票据一致：服务端只保存令牌明文的 SHA-256，明文仅在认领
  响应中返回这一次，无法从库中反推。
"""
from __future__ import annotations

import hashlib
import re
import secrets

# 认领凭证格式：<32 位小写十六进制 claim id>.<URL-safe 随机串>，
# 与下载票据的 <ticket_id>.<token> 形态一致。
_CREDENTIAL_RE = re.compile(r"([0-9a-f]{32})\.([A-Za-z0-9_\-]{8,128})")

CREDENTIAL_REQUIREMENT = (
    "<claim_id>.<token> (claim_id: 32 lowercase hex chars, "
    "token: URL-safe random string returned by the claim response)"
)


def new_token() -> str:
    """生成约 256 bit 熵的随机令牌明文（URL-safe）。"""
    return secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    """令牌明文的 SHA-256（十六进制）；明文绝不落库。"""
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def parse_credential(raw: str) -> tuple[str, str]:
    """把 X-Chunk-Claim 头解析为 (claim_id, token)；格式非法抛 ValueError。"""
    match = _CREDENTIAL_RE.fullmatch(raw.strip())
    if match is None:
        raise ValueError("invalid chunk claim credential")
    return match.group(1), match.group(2)
