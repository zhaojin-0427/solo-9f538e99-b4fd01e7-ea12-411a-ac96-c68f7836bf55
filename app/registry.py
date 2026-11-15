"""Schema registry: stable field IDs, versions, and the allowed evolutions.

Only three structural changes are ever permitted:

  1. rename a field (field_id fixed, new physical column name)
  2. add a field (always nullable)
  3. tighten a field from nullable to required (nullable -> non-nullable)

Type changes, field deletion, and making a required field nullable are
rejected outright. All changes produce a new immutable *schema version*;
every batch keeps the snapshot of field_id -> column name it was uploaded
with, so renames never cause values to flow into the wrong field.

A process-global re-entrant lock serialises writers. Tightening is two-phase:
the proposal is committed under a short lock (uploads continue to work
against the old rules), then every batch — including batches uploaded while
the check runs — is scanned, and the decision is committed under another
short lock.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from . import database as db
from .errors import ConflictError, NotFoundError, RegistryError
from .locks import writer_lock as _write_lock
from .parquet_store import batch_has_nulls


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# read helpers (single-snapshot reads)
# --------------------------------------------------------------------------- #

def current_version(conn) -> int | None:
    row = conn.execute("SELECT MAX(version) AS v FROM schema_versions").fetchone()
    return row["v"] if row and row["v"] is not None else None


def require_schema(conn) -> int:
    version = current_version(conn)
    if version is None:
        raise NotFoundError("schema has not been initialised yet")
    return version


def get_fields(conn, version: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT field_id, name, type, nullable FROM fields "
        "WHERE version = ? ORDER BY field_id",
        (version,),
    ).fetchall()
    return [
        {
            "field_id": r["field_id"],
            "name": r["name"],
            "type": r["type"],
            "nullable": bool(r["nullable"]),
        }
        for r in rows
    ]


def get_version_meta(conn, version: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT version, created_at, note FROM schema_versions WHERE version = ?",
        (version,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def list_versions(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT version, created_at, note FROM schema_versions ORDER BY version"
    ).fetchall()
    return [dict(r) for r in rows]


def list_batches(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT batch_id, schema_version, filename, row_count, uploaded_at, "
        "columns_json FROM batches ORDER BY uploaded_at, batch_id"
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["columns"] = db.loads_columns(item.pop("columns_json"))
        out.append(item)
    return out


def get_proposal(conn, proposal_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM tightening_proposals WHERE proposal_id = ?",
        (proposal_id,),
    ).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------------------- #
# version construction
# --------------------------------------------------------------------------- #

def _write_new_version(conn, fields: list[dict[str, Any]], note: str) -> int:
    new_version = (current_version(conn) or 0) + 1
    conn.execute(
        "INSERT INTO schema_versions(version, created_at, note) VALUES (?, ?, ?)",
        (new_version, _utcnow(), note),
    )
    conn.executemany(
        "INSERT INTO fields(version, field_id, name, type, nullable) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (
                new_version,
                f["field_id"],
                f["name"],
                f["type"],
                1 if f["nullable"] else 0,
            )
            for f in fields
        ],
    )
    return new_version


def _validate_field_spec(spec: dict[str, Any]) -> dict[str, Any]:
    field_id = str(spec.get("field_id", "")).strip()
    name = str(spec.get("name", "")).strip()
    ftype = spec.get("type")
    if not field_id:
        raise RegistryError("field_id must be a non-empty string")
    if not name:
        raise RegistryError(f"field {field_id!r}: name must be a non-empty string")
    if ftype not in ("int64", "float64", "string"):
        raise RegistryError(
            f"field {field_id!r}: type must be one of int64/float64/string"
        )
    nullable = bool(spec.get("nullable", True))
    return {"field_id": field_id, "name": name, "type": ftype, "nullable": nullable}


# --------------------------------------------------------------------------- #
# public mutating operations
# --------------------------------------------------------------------------- #

def initialize_schema(fields: list[dict[str, Any]]) -> dict[str, Any]:
    cleaned = [_validate_field_spec(f) for f in fields]
    ids = [f["field_id"] for f in cleaned]
    names = [f["name"] for f in cleaned]
    if len(set(ids)) != len(ids):
        raise RegistryError("duplicate field_id in initial schema")
    if len(set(names)) != len(names):
        raise RegistryError("duplicate column name in initial schema")

    with _write_lock:
        with db.write_tx() as conn:
            if current_version(conn) is not None:
                raise ConflictError(
                    "schema already initialised; use the evolution endpoints"
                )
            version = _write_new_version(conn, cleaned, "initial schema")
        return {"version": version, "fields": cleaned}


def rename_field(field_id: str, new_name: str) -> dict[str, Any]:
    new_name = str(new_name).strip()
    if not new_name:
        raise RegistryError("new_name must be a non-empty string")

    with _write_lock:
        with db.write_tx() as conn:
            version = require_schema(conn)
            fields = get_fields(conn, version)
            by_id = {f["field_id"]: f for f in fields}
            if field_id not in by_id:
                raise NotFoundError(f"unknown field_id {field_id!r}")
            if any(f["name"] == new_name and f["field_id"] != field_id for f in fields):
                raise ConflictError(
                    f"column name {new_name!r} is already used by another field"
                )
            by_id[field_id]["name"] = new_name
            new_version = _write_new_version(
                conn, fields, f"rename {field_id} -> {new_name}"
            )
        return {"version": new_version, "fields": list(by_id.values())}


def add_field(spec: dict[str, Any]) -> dict[str, Any]:
    cleaned = _validate_field_spec(spec)
    # New columns are always nullable; required-ness must go through tightening.
    if not cleaned["nullable"]:
        raise RegistryError(
            f"new field {cleaned['field_id']!r} must be nullable; "
            "use the tightening endpoint once data is complete"
        )

    with _write_lock:
        with db.write_tx() as conn:
            version = require_schema(conn)
            fields = get_fields(conn, version)
            ids = {f["field_id"] for f in fields}
            names = {f["name"] for f in fields}
            if cleaned["field_id"] in ids:
                raise ConflictError(
                    f"field_id {cleaned['field_id']!r} already exists"
                )
            if cleaned["name"] in names:
                raise ConflictError(
                    f"column name {cleaned['name']!r} is already in use"
                )
            fields.append(cleaned)
            new_version = _write_new_version(
                conn, fields, f"add field {cleaned['field_id']}"
            )
        return {"version": new_version, "fields": fields}


# --------------------------------------------------------------------------- #
# tightening (nullable -> required), two-phase
# --------------------------------------------------------------------------- #

def _pending_proposal(conn) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM tightening_proposals WHERE state = 'pending'"
    ).fetchone()
    return dict(row) if row else None


def _mark_proposal_rejected(conn, proposal_id: str, note: str) -> None:
    conn.execute(
        "UPDATE tightening_proposals SET state='rejected', decided_at=?, "
        "note=? WHERE proposal_id=?",
        (_utcnow(), note, proposal_id),
    )


def create_tightening_proposal(field_id: str) -> dict[str, Any]:
    """Phase 1: register the intent under a short lock.

    No rule changes yet, so uploads keep completing while the check runs.
    """
    with _write_lock:
        with db.write_tx() as conn:
            version = require_schema(conn)
            field = next(
                (f for f in get_fields(conn, version) if f["field_id"] == field_id),
                None,
            )
            if field is None:
                raise NotFoundError(f"unknown field_id {field_id!r}")
            if not field["nullable"]:
                raise ConflictError(
                    f"field {field_id!r} is already required (non-nullable)"
                )
            if _pending_proposal(conn) is not None:
                raise ConflictError(
                    "another tightening proposal is already pending; "
                    "resolve it first"
                )
            batches = conn.execute(
                "SELECT batch_id FROM batches ORDER BY batch_id"
            ).fetchall()
            snapshot = [r["batch_id"] for r in batches]
            proposal_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO tightening_proposals("
                "proposal_id, field_id, base_version, state, "
                "snapshot_batch_json, created_at, note) "
                "VALUES (?, ?, ?, 'pending', ?, ?, ?)",
                (
                    proposal_id,
                    field_id,
                    version,
                    db.dumps_columns({"ids": snapshot}),
                    _utcnow(),
                    f"tighten {field_id} to required",
                ),
            )
        return {"proposal_id": proposal_id, "state": "pending",
                "field_id": field_id, "base_version": version,
                "batches_at_proposal": snapshot}


def decide_tightening(proposal_id: str) -> dict[str, Any]:
    """Phase 2: scan every batch and decide against the current committed state.

    The writer lock is deliberately *not* held during the Parquet scans, so
    uploads keep completing while the check runs. The final decision takes a
    short exclusive transaction and re-scans every batch that landed in the
    meantime, so no new batch can escape the adjudication.
    """
    # First read: locate the proposal without holding any write lock across
    # the (potentially long) Parquet scans.
    with db.read_tx() as conn:
        proposal = get_proposal(conn, proposal_id)
        if proposal is None:
            raise NotFoundError(f"unknown proposal {proposal_id!r}")
        if proposal["state"] != "pending":
            raise ConflictError(
                f"proposal already {proposal['state']}",
                {"state": proposal["state"]},
            )
        field_id = proposal["field_id"]
        base_version = proposal["base_version"]
        snapshot_ids = set(
            db.loads_columns(proposal["snapshot_batch_json"])["ids"]
        )
        current = current_version(conn)
        if current != base_version:
            # Mark the stale proposal rejected in a short committed
            # transaction so it cannot get stuck in 'pending' forever.
            with _write_lock:
                with db.write_tx() as wconn:
                    fresh = get_proposal(wconn, proposal_id)
                    if fresh is not None and fresh["state"] == "pending":
                        _mark_proposal_rejected(
                            wconn, proposal_id,
                            "schema changed while the proposal was pending",
                        )
            raise ConflictError(
                "schema changed while the proposal was pending; "
                "the stale proposal was rejected, open a new one"
            )
        field = next(
            (f for f in get_fields(conn, current) if f["field_id"] == field_id),
            None,
        )
        if field is None:  # defensive: deletion is not supported anyway
            raise NotFoundError(f"field {field_id!r} no longer exists")
        if not field["nullable"]:
            raise ConflictError(f"field {field_id!r} is already required")

    # Scan phase: no writer lock, uploads may complete concurrently.
    offenders: list[dict[str, str]] = []
    scanned: list[str] = []
    with db.read_tx() as conn:
        batch_rows = conn.execute(
            "SELECT batch_id, stored_path, columns_json FROM batches"
        ).fetchall()
    for row in batch_rows:
        bid = row["batch_id"]
        scanned.append(bid)
        columns = db.loads_columns(row["columns_json"])
        if field_id not in columns:
            # Batch predates the field -> missing column counts as null.
            offenders.append(
                {"batch_id": bid, "reason": "field missing from batch"}
            )
        elif batch_has_nulls(row["stored_path"], columns[field_id]):
            offenders.append(
                {"batch_id": bid, "reason": "null values present in column"}
            )

    scanned_set = set(scanned)

    # Decision phase: short exclusive transaction. The rejection marker is
    # committed before the error is raised, otherwise the write context
    # manager would roll the proposal straight back to 'pending'.
    rejection: dict[str, Any] | None = None
    commit_result: dict[str, Any] | None = None
    with _write_lock:
        with db.write_tx() as conn:
            fresh = get_proposal(conn, proposal_id)
            if fresh is None or fresh["state"] != "pending":
                raise ConflictError("proposal state changed concurrently")
            if current_version(conn) != base_version:
                _mark_proposal_rejected(
                    conn, proposal_id, "schema changed concurrently"
                )
                rejection = {
                    "message": "schema changed while the check ran; "
                               "proposal rejected",
                    "details": {"proposal_id": proposal_id},
                }
            else:
                # Re-scan batches that appeared after the unlocked scan, so a
                # batch landing in that window cannot bypass the required rule.
                late_rows = conn.execute(
                    "SELECT batch_id, stored_path, columns_json FROM batches "
                    "WHERE batch_id NOT IN (%s)" % ",".join("?" * len(scanned)),
                    sorted(scanned),
                ).fetchall() if scanned else conn.execute(
                    "SELECT batch_id, stored_path, columns_json FROM batches"
                ).fetchall()
                for row in late_rows:
                    bid = row["batch_id"]
                    columns = db.loads_columns(row["columns_json"])
                    if field_id not in columns:
                        offenders.append(
                            {"batch_id": bid,
                             "reason": "field missing from batch"}
                        )
                    elif batch_has_nulls(row["stored_path"], columns[field_id]):
                        offenders.append(
                            {"batch_id": bid,
                             "reason": "null values present in column"}
                        )

                # Final committed batch set defines what "during the check"
                # means: anything not present when the proposal was opened.
                final_ids = {
                    r["batch_id"]
                    for r in conn.execute("SELECT batch_id FROM batches")
                }
                new_batches = sorted(final_ids - snapshot_ids)

                if offenders:
                    _mark_proposal_rejected(conn, proposal_id, "")
                    rejection = {
                        "message": (
                            f"tightening rejected: {len(offenders)} batch(es) "
                            f"contain null or missing values for field "
                            f"{field_id!r}"
                        ),
                        "details": {
                            "proposal_id": proposal_id,
                            "field_id": field_id,
                            "offending_batches": offenders,
                            "batches_uploaded_during_check": new_batches,
                        },
                    }
                else:
                    fields = get_fields(conn, base_version)
                    for f in fields:
                        if f["field_id"] == field_id:
                            f["nullable"] = False
                    new_version = _write_new_version(
                        conn, fields, f"tighten {field_id} to required"
                    )
                    conn.execute(
                        "UPDATE tightening_proposals SET state='committed', "
                        "decided_at=? WHERE proposal_id=?",
                        (_utcnow(), proposal_id),
                    )
                    commit_result = {
                        "proposal_id": proposal_id,
                        "state": "committed",
                        "field_id": field_id,
                        "version": new_version,
                        "scanned_batches": sorted(scanned_set),
                        "batches_uploaded_during_check": new_batches,
                    }

    if rejection is not None:
        raise ConflictError(rejection["message"], rejection["details"])
    return commit_result


def list_proposals(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT proposal_id, field_id, base_version, state, created_at, "
        "decided_at, note FROM tightening_proposals ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in rows]
