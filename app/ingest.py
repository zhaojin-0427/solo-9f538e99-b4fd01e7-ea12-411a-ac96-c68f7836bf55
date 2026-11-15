"""Batch ingestion orchestration.

Flow:
  1. stream the upload to an immutable file in the data volume
  2. validate it against the *declared* schema version, while required-ness
     comes from the *current* committed structure, so an old-structure
     upload cannot bypass a required rule that has since taken effect
  3. register batch metadata + the field_id -> column-name snapshot in one
     short write transaction

Any failure rolls the database back and removes the staged file, leaving the
previously available state untouched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, BinaryIO

from . import database as db
from . import parquet_store, registry
from .errors import ConflictError, NotFoundError, RegistryError
from .locks import writer_lock


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def ingest_batch(
    source: BinaryIO, filename: str, declared_version: int
) -> dict[str, Any]:
    # Stage first; nothing is visible to readers until the DB tx commits.
    batch_id, stored_path, size = parquet_store.store_parquet(source)
    try:
        with writer_lock:
            with db.write_tx() as conn:
                current = registry.current_version(conn)
                if current is None:
                    raise NotFoundError(
                        "schema has not been initialised yet"
                    )
                if declared_version < 1 or declared_version > current:
                    raise RegistryError(
                        f"declared schema_version {declared_version} does not "
                        f"exist; current version is {current}"
                    )

                version_fields = registry.get_fields(conn, declared_version)
                current_fields = registry.get_fields(conn, current)
                names_at_declared = {f["field_id"]: f["name"]
                                     for f in version_fields}
                # Validate against the *current* field set so a field added
                # (and later tightened) after the batch's declared version can
                # never slip past the required rule. Physical names for fields
                # that already existed in the declared version come from that
                # version, so old column names keep validating.
                effective_fields = []
                for f in current_fields:
                    effective_fields.append(
                        {**f, "name": names_at_declared.get(
                            f["field_id"], f["name"])}
                    )

                columns, row_count = parquet_store.validate_parquet_schema(
                    stored_path, effective_fields
                )
                # Keep only fields physically present in this batch; absent
                # nullable fields must not pollute the stored snapshot.
                columns = {
                    fid: name for fid, name in columns.items()
                    if fid in names_at_declared
                }

                # Defensive duplicate guard.
                exists = conn.execute(
                    "SELECT 1 FROM batches WHERE stored_path = ?", (stored_path,)
                ).fetchone()
                if exists:
                    raise ConflictError("batch already registered")

                conn.execute(
                    "INSERT INTO batches(batch_id, schema_version, filename, "
                    "stored_path, row_count, uploaded_at, columns_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        batch_id,
                        declared_version,
                        filename,
                        stored_path,
                        row_count,
                        _utcnow(),
                        db.dumps_columns(columns),
                    ),
                )
        return {
            "batch_id": batch_id,
            "schema_version": declared_version,
            "filename": filename,
            "row_count": row_count,
            "size_bytes": size,
            "columns": columns,
        }
    except Exception:
        # Registration failed: keep the previous state fully usable.
        parquet_store.discard_file(stored_path)
        raise
