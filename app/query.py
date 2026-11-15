"""Read-side queries.

Structure and batch listings come from one read transaction, i.e. one
committed SQLite snapshot, so they can never disagree. Row data is then read
from the immutable Parquet files and aligned by stable field ID.
"""
from __future__ import annotations

from typing import Any

from . import config, database as db
from . import parquet_store, registry
from .errors import RegistryError


def get_catalog() -> dict[str, Any]:
    """Current structure + batch list from a single committed snapshot."""
    with db.read_tx() as conn:
        version = registry.current_version(conn)
        if version is None:
            return {"schema": None, "fields": [], "batches": []}
        return {
            "schema": registry.get_version_meta(conn, version),
            "fields": registry.get_fields(conn, version),
            "batches": registry.list_batches(conn),
        }


def get_version_detail(version: int) -> dict[str, Any]:
    with db.read_tx() as conn:
        meta = registry.get_version_meta(conn, version)
        if meta is None:
            from .errors import NotFoundError

            raise NotFoundError(f"unknown schema version {version}")
        return {"schema": meta, "fields": registry.get_fields(conn, version)}


def query_rows(
    field_ids: list[str] | None,
    batch_ids: list[str] | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    if limit < 1 or limit > config.MAX_QUERY_LIMIT:
        raise RegistryError(
            f"limit must be between 1 and {config.MAX_QUERY_LIMIT}"
        )
    if offset < 0:
        raise RegistryError("offset must be >= 0")

    with db.read_tx() as conn:
        current = registry.require_schema(conn)
        current_fields = registry.get_fields(conn, current)
        known_ids = {f["field_id"] for f in current_fields}

        if field_ids:
            unknown = sorted(set(field_ids) - known_ids)
            if unknown:
                raise RegistryError(f"unknown field_id(s): {unknown}")
            selected = field_ids
        else:
            selected = sorted(known_ids)

        rows = conn.execute(
            "SELECT batch_id, schema_version, filename, stored_path, "
            "row_count, uploaded_at, columns_json FROM batches "
            "ORDER BY uploaded_at, batch_id"
        ).fetchall()
        batches = []
        for r in rows:
            item = dict(r)
            item["columns"] = db.loads_columns(item.pop("columns_json"))
            batches.append(item)

    if batch_ids is not None:
        wanted = set(batch_ids)
        unknown_batches = sorted(wanted - {b["batch_id"] for b in batches})
        if unknown_batches:
            raise RegistryError(f"unknown batch_id(s): {unknown_batches}")
        batches = [b for b in batches if b["batch_id"] in wanted]

    total_rows = sum(b["row_count"] for b in batches)

    # Offset/limit over the concatenated, batch-ordered row stream.
    skipped = 0
    remaining = limit
    data: list[dict[str, Any]] = []
    scanned_batches: list[str] = []
    for batch in batches:
        if remaining <= 0:
            break
        start = max(0, offset - skipped)
        if start >= batch["row_count"]:
            skipped += batch["row_count"]
            continue
        end = min(batch["row_count"], start + remaining)
        scanned_batches.append(batch["batch_id"])
        all_rows = parquet_store.read_aligned(batch, selected)
        page = all_rows[start:end]
        data.extend(page)
        remaining -= len(page)
        skipped += batch["row_count"]

    return {
        "schema_version": current,
        "field_ids": selected,
        "total_batches": len(batches),
        "total_rows": total_rows,
        "limit": limit,
        "offset": offset,
        "returned_rows": len(data),
        "batches_scanned": scanned_batches,
        "rows": data,
    }
