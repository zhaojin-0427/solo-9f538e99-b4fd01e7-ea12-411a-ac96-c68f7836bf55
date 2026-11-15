"""Parquet validation, immutable storage and field-ID-aligned reads.

Uploaded files are written to a temporary name first and only atomically
renamed into the data volume once the registry transaction has committed.
Nothing ever rewrites an already stored file.
"""
from __future__ import annotations

import os
import tempfile
import uuid
from typing import Any, BinaryIO, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.types as patypes

from . import config
from .errors import RegistryError

# Logical type -> Arrow type predicate.
_TYPE_PREDICATE = {
    "int64": patypes.is_int64,
    "float64": patypes.is_float64,
    "string": lambda t: patypes.is_string(t) or patypes.is_large_string(t),
}

_ARROW_TO_LOGICAL = (
    (pa.int64(), "int64"),
    (pa.float64(), "float64"),
    (pa.string(), "string"),
    (pa.large_string(), "string"),
)


def arrow_type_to_logical(arrow_type: pa.DataType) -> str | None:
    for prototype, logical in _ARROW_TO_LOGICAL:
        if arrow_type == prototype:
            return logical
    return None


def _check_arrow_type(field_id: str, logical: str, arrow_type: pa.DataType) -> None:
    if not _TYPE_PREDICATE[logical](arrow_type):
        actual = arrow_type_to_logical(arrow_type) or str(arrow_type)
        raise RegistryError(
            f"column for field {field_id!r} has type {actual!r}, "
            f"expected {logical!r}"
        )


def validate_parquet_schema(
    parquet_file: str | BinaryIO, version_fields: list[dict[str, Any]]
) -> tuple[dict[str, str], int]:
    """Validate an uploaded Parquet file against one schema version.

    Returns (columns snapshot field_id -> physical column name, row_count).
    Rules:
      * every physical column must be registered in the declared version
      * Arrow type must match the registered logical type
      * a currently-required field missing from the file is rejected
      * nulls in a required column are rejected
      * nullable fields absent from the file are allowed (old batches)
    """
    try:
        schema = pq.read_schema(parquet_file)
        metadata = pq.read_metadata(
            parquet_file if isinstance(parquet_file, str) else parquet_file
        )
    except Exception as exc:  # noqa: BLE001 - corrupt/non-parquet upload
        raise RegistryError(f"cannot read Parquet file: {exc}") from exc

    physical_names = set(schema.names)
    registered_names = {f["name"]: f for f in version_fields}

    unknown = sorted(physical_names - set(registered_names))
    if unknown:
        raise RegistryError(
            f"file contains columns not registered in the declared schema "
            f"version: {unknown}"
        )

    columns: dict[str, str] = {}
    for field in version_fields:
        fid, name = field["field_id"], field["name"]
        if name not in physical_names:
            if not field["nullable"]:
                raise RegistryError(
                    f"required field {fid!r} (column {name!r}) is missing "
                    f"from the uploaded file"
                )
            continue  # nullable field absent: fine, treated as null
        arrow_field = schema.field(name)
        _check_arrow_type(fid, field["type"], arrow_field.type)
        columns[fid] = name

    # Null check for required columns that are present.
    for field in version_fields:
        if field["nullable"]:
            continue
        name = field["name"]
        if name not in physical_names:
            continue
        null_count = _column_null_count(parquet_file, name)
        if null_count:
            raise RegistryError(
                f"required field {field['field_id']!r} (column {name!r}) "
                f"contains {null_count} null value(s)"
            )

    return columns, metadata.num_rows


def _column_null_count(parquet_file: str | BinaryIO, column: str) -> int:
    pf = pq.ParquetFile(parquet_file)
    total = 0
    for batch in pf.iter_batches(columns=[column], batch_size=65536):
        total += batch.column(0).null_count
    return total


def batch_has_nulls(stored_path: str, physical_column: str) -> bool:
    return _column_null_count(stored_path, physical_column) > 0


def store_parquet(source: BinaryIO) -> tuple[str, str, int]:
    """Stream the upload into the data volume atomically.

    Returns (batch_id, stored_path, size_bytes).
    """
    config.ensure_dirs()
    batch_id = uuid.uuid4().hex
    final_path = config.PARQUET_DIR / f"{batch_id}.parquet"
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=f".{batch_id}.", suffix=".tmp", dir=config.PARQUET_DIR
    )
    size = 0
    try:
        with os.fdopen(tmp_fd, "wb") as out:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                size += len(chunk)
        if size == 0:
            raise RegistryError("uploaded file is empty")
        os.replace(tmp_path, final_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if os.path.exists(final_path):
            os.unlink(final_path)
        raise
    return batch_id, str(final_path), size


def discard_file(stored_path: str) -> None:
    try:
        if os.path.exists(stored_path):
            os.unlink(stored_path)
    except OSError:
        pass


def read_aligned(
    batch: dict[str, Any],
    field_ids: Iterable[str],
) -> list[dict[str, Any]]:
    """Read one batch, projecting physical columns onto stable field IDs.

    Missing columns (batch predates a field) are emitted as null; the batch's
    own field_id -> column-name snapshot guarantees a renamed or reused column
    name never feeds the wrong field.
    """
    field_ids = list(field_ids)
    columns_map: dict[str, str] = batch["columns"]
    wanted_physical = [
        columns_map[fid] for fid in field_ids if fid in columns_map
    ]

    table = pq.read_table(batch["stored_path"], columns=wanted_physical)
    # Reverse map: physical column name -> field id (names unique per batch).
    physical_to_id = {v: k for k, v in columns_map.items()}
    arrays = {}
    for fid in field_ids:
        if fid in columns_map:
            arrays[fid] = table.column(columns_map[fid]).to_pylist()
        else:
            arrays[fid] = [None] * table.num_rows

    rows = []
    for i in range(table.num_rows):
        row = {"batch_id": batch["batch_id"]}
        for fid in field_ids:
            row[fid] = arrays[fid][i]
        rows.append(row)
    return rows
