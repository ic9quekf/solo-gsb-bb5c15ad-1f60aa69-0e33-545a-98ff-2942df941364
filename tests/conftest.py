"""测试夹具：在导入应用前把数据目录与数据库指向临时目录。"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

_TMP = Path(tempfile.mkdtemp(prefix="resumable-test-"))
os.environ["DATA_DIR"] = str(_TMP / "data")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP / 'data' / 'test.db'}"
os.environ["SESSION_TTL_SECONDS"] = "3600"
os.environ["CLEANUP_INTERVAL_SECONDS"] = "3600"
os.environ["MAX_CHUNK_BYTES"] = "0"
os.environ["MAX_UPLOAD_BYTES"] = "0"

import pytest
from fastapi.testclient import TestClient

from app import settings
from app.main import app

settings.reload()


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c
