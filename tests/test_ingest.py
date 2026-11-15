"""Batch ingestion validation and field-ID-aligned storage."""
from __future__ import annotations

import pyarrow as pa

from tests.conftest import INITIAL_FIELDS, init_schema, make_parquet, upload


def _old_batch():
    return make_parquet(
        {
            "cycle": [1, 2],
            "volt": [3.7, 3.8],
            "sn": ["A", "B"],
        }
    )


def _new_named_batch():
    return make_parquet(
        {
            "cycle": [3],
            "voltage_v": [3.9],
            "sn": ["C"],
        }
    )


def test_upload_basic_and_catalog_snapshot(client):
    init_schema(client)
    r = upload(client, _old_batch(), 1)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["row_count"] == 2
    assert body["schema_version"] == 1
    assert body["columns"] == {
        "cycle_id": "cycle",
        "voltage": "volt",
        "serial": "sn",
    }

    cat = client.get("/api/v1/catalog").json()
    assert cat["schema"]["version"] == 1
    assert len(cat["batches"]) == 1
    # structure and batch list are served from one snapshot response
    assert cat["batches"][0]["columns"]["voltage"] == "volt"


def test_upload_unknown_version_rejected(client):
    init_schema(client)
    r = upload(client, _old_batch(), 99)
    assert r.status_code == 400
    assert "does not exist" in r.json()["error"]


def test_upload_unknown_column_rejected(client):
    init_schema(client)
    raw = make_parquet(
        {"cycle": [1], "volt": [1.0], "sn": ["x"], "mystery": [9]}
    )
    r = upload(client, raw, 1)
    assert r.status_code == 400
    assert "mystery" in r.json()["error"]


def test_type_mismatch_rejected(client):
    init_schema(client)
    raw = make_parquet({"cycle": [1.5], "volt": [1.0], "sn": ["x"]})
    r = upload(client, raw, 1)
    assert r.status_code == 400
    assert "expected 'int64'" in r.json()["error"]


def test_required_missing_and_null_rejected(client):
    init_schema(client)
    missing = make_parquet({"volt": [1.0], "sn": ["x"]})
    r = upload(client, missing, 1)
    assert r.status_code == 400
    assert "required" in r.json()["error"]

    nulls = make_parquet(
        {"cycle": pa.array([1, None], type=pa.int64()), "volt": [1.0, 2.0],
         "sn": ["x", "y"]}
    )
    r = upload(client, nulls, 1)
    assert r.status_code == 400
    assert "null value" in r.json()["error"]


def test_nullable_column_absent_is_allowed_and_filled_null(client):
    init_schema(client)
    raw = make_parquet({"cycle": [1, 2]})  # nullable volt/sn absent
    r = upload(client, raw, 1)
    assert r.status_code == 201, r.text

    q = client.get("/api/v1/query").json()
    assert q["total_rows"] == 2
    for row in q["rows"]:
        assert row["voltage"] is None
        assert row["serial"] is None
        assert row["cycle_id"] in (1, 2)


def test_rename_then_old_and_new_batches_align_by_field_id(client):
    init_schema(client)
    assert upload(client, _old_batch(), 1).status_code == 201

    r = client.post(
        "/api/v1/schema/rename",
        json={"field_id": "voltage", "new_name": "voltage_v"},
    )
    assert r.status_code == 201

    # old-structure batch still accepted after rename (declared version 1)
    r_old = upload(client, _old_batch(), 1)
    assert r_old.status_code == 201, r_old.text
    assert r_old.json()["columns"]["voltage"] == "volt"

    # new-structure batch
    r_new = upload(client, _new_named_batch(), 2)
    assert r_new.status_code == 201, r_new.text
    assert r_new.json()["columns"]["voltage"] == "voltage_v"

    q = client.get("/api/v1/query", params={"field_ids": "cycle_id,voltage"}).json()
    assert q["total_rows"] == 5
    values = sorted(row["voltage"] for row in q["rows"])
    assert values == [3.7, 3.7, 3.8, 3.8, 3.9]


def test_reused_old_column_name_does_not_cross_wire(client):
    """New field adopts an old physical name; values must not get mixed up."""
    init_schema(client)
    # batch under v1: 'volt' carries voltage
    assert upload(client, _old_batch(), 1).status_code == 201

    # rename voltage away from 'volt', then add a NEW field that reuses 'volt'
    client.post(
        "/api/v1/schema/rename",
        json={"field_id": "voltage", "new_name": "voltage_v"},
    )
    r = client.post(
        "/api/v1/schema/fields",
        json={"field_id": "impedance", "name": "volt", "type": "float64"},
    )
    assert r.status_code == 201, r.text

    raw = make_parquet(
        {
            "cycle": [9],
            "voltage_v": [4.0],
            "volt": [0.05],  # same physical name, now a different field_id
            "sn": ["Z"],
        }
    )
    assert upload(client, raw, 3).status_code == 201

    q = client.get(
        "/api/v1/query", params={"field_ids": "cycle_id,voltage,impedance"}
    ).json()
    by_cycle = {row["cycle_id"]: row for row in q["rows"]}
    # old batch: voltage populated under volt, impedance missing -> null
    assert by_cycle[1]["voltage"] == 3.7
    assert by_cycle[1]["impedance"] is None
    # new batch: no cross-wiring
    assert by_cycle[9]["voltage"] == 4.0
    assert by_cycle[9]["impedance"] == 0.05


def test_corrupt_upload_is_rejected_and_state_kept(client):
    init_schema(client)
    r = upload(client, b"not a parquet file at all", 1)
    assert r.status_code == 400
    cat = client.get("/api/v1/catalog").json()
    assert cat["batches"] == []
