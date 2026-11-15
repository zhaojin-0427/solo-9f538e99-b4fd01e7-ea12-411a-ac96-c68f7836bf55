"""SQLite storage layer.

Schema
------
schema_versions       one row per committed structure version (immutable)
fields                field rows per version; field_id is the stable identity
batches               one row per successfully ingested Parquet batch
batch_columns         per-batch field_id -> physical column name snapshot
tightening_proposals  in-flight "make required" change proposals

Every request opens its own connection and uses BEGIN IMMEDIATE for writes or
a plain deferred transaction for reads, so a structure listing and its batch
list always come from the same committed snapshot. WAL keeps readers and the
single writer lock (see registry.py) from blocking each other.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config

_SCHEMA_SQL = [
    """
CREATE TABLE IF NOT EXISTS schema_versions (
    version        INTEGER PRIMARY KEY,
    created_at     TEXT NOT NULL,
    note           TEXT NOT NULL DEFAULT ''
)
""",
    """
CREATE TABLE IF NOT EXISTS fields (
    version        INTEGER NOT NULL REFERENCES schema_versions(version),
    field_id       TEXT NOT NULL,
    name           TEXT NOT NULL,
    type           TEXT NOT NULL CHECK (type IN ('int64','float64','string')),
    nullable       INTEGER NOT NULL CHECK (nullable IN (0,1)),
    PRIMARY KEY (version, field_id),
    UNIQUE (version, name)
)
""",
    """
CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    schema_version  INTEGER NOT NULL,
    filename        TEXT NOT NULL,
    stored_path     TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    uploaded_at     TEXT NOT NULL,
    columns_json    TEXT NOT NULL
)
""",
    """
CREATE TABLE IF NOT EXISTS tightening_proposals (
    proposal_id     TEXT PRIMARY KEY,
    field_id        TEXT NOT NULL,
    base_version    INTEGER NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('pending','committed','rejected')),
    snapshot_batch_json TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    decided_at      TEXT,
    note            TEXT NOT NULL DEFAULT ''
)
""",
    "CREATE INDEX IF NOT EXISTS idx_fields_version ON fields(version)",
    "CREATE INDEX IF NOT EXISTS idx_batches_version ON batches(schema_version)",
    "CREATE INDEX IF NOT EXISTS idx_proposals_state ON tightening_proposals(state)",
]


def connect() -> sqlite3.Connection:
    config.ensure_dirs()
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Separate execute() calls: executescript() would auto-commit the
        # surrounding transaction and defeat rollback semantics.
        for stmt in _SCHEMA_SQL:
            conn.execute(stmt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Serialised write transaction; rollback restores the prior state."""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    """Read transaction pinned to one committed snapshot (SQLite deferred)."""
    conn = connect()
    try:
        conn.execute("BEGIN")
        yield conn
        conn.rollback()  # release snapshot; nothing was written
    finally:
        conn.close()


def dumps_columns(columns: dict[str, str]) -> str:
    return json.dumps(columns, ensure_ascii=False, sort_keys=True)


def loads_columns(raw: str) -> dict[str, str]:
    return json.loads(raw)
