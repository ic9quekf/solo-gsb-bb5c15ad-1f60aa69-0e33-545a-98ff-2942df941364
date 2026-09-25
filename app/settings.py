"""运行期配置：所有配置项均来自环境变量，带合理默认值。

测试中可以直接对模块属性赋值（例如 monkeypatch.setattr(settings, "ttl_seconds", 1)），
代码在调用点动态读取，因此修改立即生效。
"""
from __future__ import annotations

import os
from pathlib import Path


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value


def reload() -> None:
    """从环境变量重新加载全部配置（主要供测试使用）。"""
    global data_dir, database_url, ttl_seconds, cleanup_interval_seconds
    global max_chunk_bytes, max_upload_bytes, max_filename_length
    global chunk_claim_ttl_seconds

    data_dir = Path(_env_str("DATA_DIR", "/data")).resolve()
    database_url = _env_str("DATABASE_URL", f"sqlite:///{data_dir / 'resumable.db'}")
    ttl_seconds = max(1, _env_int("SESSION_TTL_SECONDS", 86400))
    cleanup_interval_seconds = max(1, _env_int("CLEANUP_INTERVAL_SECONDS", 300))
    # 0 表示不限制单个分片大小。
    max_chunk_bytes = max(0, _env_int("MAX_CHUNK_BYTES", 64 * 1024 * 1024))
    # 0 表示不限制整文件大小。
    max_upload_bytes = max(0, _env_int("MAX_UPLOAD_BYTES", 100 * 1024 * 1024 * 1024))
    max_filename_length = max(1, _env_int("MAX_FILENAME_LENGTH", 255))
    # 分片认领令牌的有效期（秒），默认 5 分钟。
    chunk_claim_ttl_seconds = max(1, _env_int("CHUNK_CLAIM_TTL_SECONDS", 300))


data_dir: Path
database_url: str
ttl_seconds: int
cleanup_interval_seconds: int
max_chunk_bytes: int
max_upload_bytes: int
max_filename_length: int
chunk_claim_ttl_seconds: int

reload()
