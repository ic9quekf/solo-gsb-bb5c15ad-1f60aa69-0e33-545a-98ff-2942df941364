"""请求/响应模型。"""
from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator, model_validator

from . import settings

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def normalize_sha256(value: str) -> str:
    """规整 SHA-256：去空白、转小写、剥离可选 sha256: 前缀，返回 64 位 hex。

    整文件摘要字段与 X-Chunk-SHA256 请求头共用；格式非法抛 ValueError。
    """
    value = value.strip().lower()
    if value.startswith("sha256:"):
        value = value[len("sha256:"):]
    if not _SHA256_HEX.match(value):
        raise ValueError("sha256 must be 64 lowercase hex characters")
    return value


class CreateSessionRequest(BaseModel):
    filename: str = Field(..., description="原始文件名")
    total_size: int = Field(..., ge=0, description="文件总字节数")
    sha256: str = Field(..., description="整文件 SHA-256，64 位小写十六进制（可带 sha256: 前缀）")
    chunk_size: int = Field(
        default=0, ge=0, description="建议分片大小（字节），仅作提示；0 表示无建议"
    )

    @field_validator("filename")
    @classmethod
    def _validate_filename(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("filename must not be empty")
        if len(value) > settings.max_filename_length:
            raise ValueError(f"filename too long (max {settings.max_filename_length})")
        return value

    @field_validator("total_size")
    @classmethod
    def _validate_size(cls, value: int) -> int:
        if settings.max_upload_bytes and value > settings.max_upload_bytes:
            raise ValueError(
                f"total_size exceeds server limit {settings.max_upload_bytes} bytes"
            )
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        return normalize_sha256(value)


class MissingRange(BaseModel):
    start: int
    end: int


class SessionResponse(BaseModel):
    session_id: str
    filename: str
    total_size: int
    sha256_expected: str
    chunk_size: int
    status: str
    bytes_received: int
    complete: bool
    fully_received: bool
    missing_ranges: list[MissingRange]
    created_at: str
    expires_at: str
    last_access_at: str
    completed_at: str | None = None
    download_url: str | None = None


class ChunkResponse(BaseModel):
    session_id: str
    received: MissingRange
    bytes_received: int
    missing_ranges: list[MissingRange]
    fully_received: bool
    expires_at: str


# ---------------------------------------------------------------------------
# 分片进度增量同步（progress snapshot + chunk events）
# ---------------------------------------------------------------------------


class ProgressSnapshotResponse(BaseModel):
    """进度快照：已收区间与事件游标取自同一个数据库读事务（同一一致性快照）。

    多端重连时先取本快照，把 ``received_ranges`` 作为基线、``event_cursor``
    作为游标，再轮询 GET .../events?cursor=... 追增量，不会漏掉两次请求之间
    其他客户端成功提交的区间。
    """

    session_id: str
    status: str = Field(
        ..., description="会话当前状态（快照时点）：uploading / completed / expired"
    )
    total_size: int
    bytes_received: int = Field(..., description="快照时点已登记字节数")
    received_ranges: list[MissingRange] = Field(
        ...,
        description=(
            "当前已收区间（合并相邻区间后按起点升序）；旧会话以此作为事件基线"
        ),
    )
    missing_ranges: list[MissingRange] = Field(
        ..., description="相对 total_size 仍缺失的区间"
    )
    event_cursor: int = Field(
        ...,
        ge=0,
        description=(
            "本会话事件游标（=已生成的最大事件序号，无事件时为 0）；"
            "之后以 ?cursor=<该值> 拉取游标之后的事件"
        ),
    )
    expires_at: str | None = Field(
        default=None, description="会话过期时间；非 uploading 状态可能为 null"
    )


class ChunkEvent(BaseModel):
    """一条分片接收事件：序号在同一会话内从 1 开始严格连续、单调递增。"""

    seq: int = Field(..., ge=1, description="会话内单调递增的事件序号（游标值）")
    start: int = Field(..., description="成功登记区间的起始偏移（含）")
    end: int = Field(..., description="成功登记区间的结束偏移（不含）")
    source: str = Field(
        ...,
        description=(
            "接收来源：upload=普通 PUT 分片；copy=服务端 copy-chunk。"
            "失败、重叠冲突与原位修复不产生事件"
        ),
    )
    created_at: str = Field(..., description="事件写入时间（UTC，ISO-8601 带 Z）")


class ChunkEventListResponse(BaseModel):
    """按游标分页的事件页：按序返回 cursor 之后成功接收的区间。"""

    session_id: str
    cursor: int = Field(..., ge=0, description="本次请求使用的游标（回显）")
    events: list[ChunkEvent] = Field(
        ..., description="游标之后的事件，按 seq 升序，最多 limit 条"
    )
    next_cursor: int | None = Field(
        default=None,
        description=(
            "下一页游标（=本页最后一条事件的 seq）；本页为空时为 null。"
            "无论 has_more 是否为 false，客户端都可用它继续轮询新提交"
        ),
    )
    has_more: bool = Field(
        ..., description="游标之后是否还有超过本页大小的事件未返回"
    )


# ---------------------------------------------------------------------------
# 分片认领（chunk-claims）
# ---------------------------------------------------------------------------


class ChunkClaimRequest(BaseModel):
    """认领一个尚未接收的半开区间 [start, end)。"""

    start: int = Field(..., ge=0, description="起始偏移（含）")
    end: int = Field(..., ge=0, description="结束偏移（不含），半开区间 [start, end)")

    @model_validator(mode="after")
    def _validate_range(self) -> "ChunkClaimRequest":
        if self.end <= self.start:
            raise ValueError("invalid range: require start < end")
        return self


class ChunkClaimResponse(BaseModel):
    claim_id: str = Field(..., description="认领 ID（令牌凭证的前半段）")
    session_id: str
    start: int
    end: int
    created_at: str
    expires_at: str
    claim: str = Field(
        ...,
        description=(
            "认领令牌 <claim_id>.<token>，明文仅本次返回；提交该区间分片时"
            "放入 X-Chunk-Claim 请求头，请像密码一样保管"
        ),
    )


class CopyChunkRequest(BaseModel):
    """服务端复制分片请求：从已完成来源会话的成品中复制一段。"""

    source_session_id: str = Field(
        ..., min_length=1, max_length=128, description="已完成的来源会话 ID"
    )
    source_start: int = Field(..., ge=0, description="来源起始偏移（含）")
    source_end: int = Field(..., ge=0, description="来源结束偏移（不含），半开区间")
    target_start: int = Field(
        ..., ge=0, description="目标会话中的起始偏移（含），长度沿用来源区间"
    )

    @model_validator(mode="after")
    def _validate_range(self) -> "CopyChunkRequest":
        if self.source_end <= self.source_start:
            raise ValueError("invalid source range: require source_start < source_end")
        return self


# ---------------------------------------------------------------------------
# 分片完整性诊断（chunks/audit）
# ---------------------------------------------------------------------------


class ChunkAuditEntry(BaseModel):
    """诊断清单中的一条：区间必须与某已登记分片完全一致。"""

    start: int = Field(..., ge=0, description="起始偏移（含）")
    end: int = Field(..., ge=0, description="结束偏移（不含），半开区间")
    sha256: str = Field(
        ...,
        description="客户端持有的该区间内容摘要，64 位小写十六进制（可带 sha256: 前缀）",
    )

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        return normalize_sha256(value)

    @model_validator(mode="after")
    def _validate_range(self) -> "ChunkAuditEntry":
        if self.end <= self.start:
            raise ValueError("invalid range: require start < end")
        return self


class ChunkAuditRequest(BaseModel):
    """诊断清单：1～32 条且区间不可重复；与已登记分片的精确对应在服务端校验。"""

    chunks: list[ChunkAuditEntry] = Field(
        ..., min_length=1, max_length=32, description="待核对的分片清单，最多 32 条"
    )

    @model_validator(mode="after")
    def _validate_unique_ranges(self) -> "ChunkAuditRequest":
        seen: set[tuple[int, int]] = set()
        for entry in self.chunks:
            key = (entry.start, entry.end)
            if key in seen:
                raise ValueError(
                    f"duplicate chunk range {entry.start}-{entry.end} in audit manifest"
                )
            seen.add(key)
        return self


class ChunkAuditResult(BaseModel):
    """单条清单的核对结论；actual_sha256/size_on_disk 在文件缺失时为 null。"""

    start: int
    end: int
    status: str = Field(
        ..., description="ok / sha256_mismatch / length_mismatch / missing"
    )
    expected_sha256: str = Field(..., description="清单中客户端声明的摘要")
    actual_sha256: str | None = Field(
        default=None, description="磁盘分片的实际 SHA-256；文件缺失时为 null"
    )
    size_on_disk: int | None = Field(
        default=None, description="磁盘分片的实际字节数；文件缺失时为 null"
    )


class ChunkAuditResponse(BaseModel):
    session_id: str
    all_match: bool = Field(..., description="全部条目均为 ok 时为 true")
    results: list[ChunkAuditResult] = Field(
        ..., description="按起点升序逐条返回的核对结论"
    )


# ---------------------------------------------------------------------------
# 成品完整性巡检（integrity-check）
# ---------------------------------------------------------------------------


class IntegrityCheckResponse(BaseModel):
    """成品巡检结论；文件缺失/长度不符/摘要不符时已持久化隔离标记。"""

    session_id: str
    status: str = Field(
        ..., description="ok / missing / length_mismatch / sha256_mismatch"
    )
    quarantined: bool = Field(
        ..., description="巡检后会话是否处于隔离状态（下载与票据均被拒绝）"
    )
    declared_size: int = Field(..., description="会话声明的成品字节数（total_size）")
    size_on_disk: int | None = Field(
        default=None, description="成品文件的实际字节数；文件缺失时为 null"
    )
    expected_sha256: str = Field(..., description="会话声明的整文件 SHA-256")
    actual_sha256: str | None = Field(
        default=None, description="成品文件的实际 SHA-256；文件缺失时为 null"
    )
    quarantined_at: str | None = Field(
        default=None, description="隔离标记写入时间（UTC，ISO-8601 带 Z）；未隔离为 null"
    )
    checked_at: str = Field(..., description="本次巡检执行时间（UTC，ISO-8601 带 Z）")


# ---------------------------------------------------------------------------
# 受控下载票据
# ---------------------------------------------------------------------------


class CreateTicketRequest(BaseModel):
    expires_in_minutes: int = Field(
        ..., ge=1, le=1440, description="票据有效期（分钟），1～1440"
    )
    max_downloads: int = Field(
        ..., ge=1, le=100, description="下载次数上限，1～100"
    )


class TicketInfo(BaseModel):
    """票据的非敏感元数据（撤销接口返回）。"""

    ticket_id: str
    session_id: str
    created_at: str
    expires_at: str
    max_downloads: int
    remaining_downloads: int
    revoked: bool
    download_url: str


class TicketResponse(TicketInfo):
    # 明文随机票据，仅在签发响应中返回这一次；服务端只存它的哈希。
    ticket: str = Field(..., description="凭票下载令牌（明文仅本次返回，请妥善保管）")


# ---------------------------------------------------------------------------
# 运维只读汇总
# ---------------------------------------------------------------------------


class StorageStatusSummary(BaseModel):
    """单个状态的汇总行（四种状态固定各一行，计数为零也保留）。"""

    status: str = Field(
        ..., description="会话状态：uploading / completed / aborted / expired"
    )
    session_count: int = Field(..., ge=0, description="该状态的会话数量")
    declared_bytes: int = Field(
        ..., ge=0, description="声明字节数：所有会话 total_size 之和"
    )
    stored_bytes: int = Field(
        ...,
        ge=0,
        description=(
            "实际占用的逻辑字节数：uploading 取 bytes_received 之和，"
            "completed 取 total_size 之和，aborted / expired 恒为 0"
        ),
    )


class StorageSummaryResponse(BaseModel):
    statuses: list[StorageStatusSummary] = Field(
        ...,
        description="按 uploading、completed、aborted、expired 固定顺序排列的四行",
    )
    total: StorageStatusSummary = Field(
        ..., description="四行求和的总计（status 字段为 total）"
    )
