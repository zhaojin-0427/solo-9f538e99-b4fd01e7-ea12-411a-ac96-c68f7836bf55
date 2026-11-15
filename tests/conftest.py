"""Test fixtures: isolated DATA_DIR per test session and an API client."""
from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_tmp_root = tempfile.mkdtemp(prefix="bsr-tests-")

import os  # noqa: E402

os.environ["DATA_DIR"] = str(Path(_tmp_root) / "data")

from fastapi.testclient import TestClient  # noqa: E402

from app import config, database as db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_data_dir():
    """Each test gets an empty data volume and a fresh registry DB."""
    data_dir = Path(tempfile.mkdtemp(prefix="bsr-case-", dir=_tmp_root))
    os.environ["DATA_DIR"] = str(data_dir)
    config.DATA_DIR = data_dir.resolve()
    config.PARQUET_DIR = (data_dir / "parquet").resolve()
    config.DB_PATH = (data_dir / "registry.db").resolve()
    config.ensure_dirs()
    db.init_db()
    yield
    # leave files on disk for post-mortem; temp dir is under the session tmp


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def make_parquet(rows: dict[str, list], schema: pa.Schema | None = None) -> bytes:
    """Build an in-memory Parquet file and return its bytes."""
    arrays = [pa.array(vals) for vals in rows.values()]
    table = pa.table(dict(zip(rows.keys(), arrays)), schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def upload(client, raw: bytes, version: int, name: str = "batch.parquet"):
    return client.post(
        "/api/v1/batches",
        data={"schema_version": str(version)},
        files={"file": (name, raw, "application/octet-stream")},
    )


INITIAL_FIELDS = [
    {"field_id": "cycle_id", "name": "cycle", "type": "int64", "nullable": False},
    {"field_id": "voltage", "name": "volt", "type": "float64", "nullable": True},
    {"field_id": "serial", "name": "sn", "type": "string", "nullable": True},
]


def init_schema(client, fields=None):
    return client.post(
        "/api/v1/schema/init", json={"fields": fields or INITIAL_FIELDS}
    )
