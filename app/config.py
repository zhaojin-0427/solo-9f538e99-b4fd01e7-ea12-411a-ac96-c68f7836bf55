"""Runtime configuration.

All state lives under DATA_DIR so the service survives container restarts:
  DATA_DIR/registry.db      SQLite schema registry + batch associations
  DATA_DIR/parquet/<batch_id>.parquet   immutable uploaded Parquet files
"""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data")).resolve()
PARQUET_DIR = DATA_DIR / "parquet"
DB_PATH = DATA_DIR / "registry.db"

# Hard cap for a single query page so a request cannot scan the whole volume.
DEFAULT_QUERY_LIMIT = 1000
MAX_QUERY_LIMIT = 10000

# Allowed logical field types.
ALLOWED_TYPES = ("int64", "float64", "string")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
