"""Schema registry basics: init, versions, allowed and forbidden changes."""
from __future__ import annotations

from tests.conftest import INITIAL_FIELDS, init_schema


def test_init_and_read_current(client):
    r = init_schema(client)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["version"] == 1
    assert {f["field_id"] for f in body["fields"]} == {
        "cycle_id",
        "voltage",
        "serial",
    }

    cur = client.get("/api/v1/schema/current").json()
    assert cur["schema"]["version"] == 1
    by_id = {f["field_id"]: f for f in cur["fields"]}
    assert by_id["cycle_id"] == {
        "field_id": "cycle_id",
        "name": "cycle",
        "type": "int64",
        "nullable": False,
    }


def test_double_init_rejected(client):
    init_schema(client)
    r = init_schema(client)
    assert r.status_code == 409


def test_init_rejects_bad_type(client):
    bad = [dict(INITIAL_FIELDS[0], type="int32")]
    r = init_schema(client, bad)
    assert r.status_code == 422  # pydantic literal validation


def test_rename_creates_new_version_keeps_field_id(client):
    init_schema(client)
    r = client.post(
        "/api/v1/schema/rename",
        json={"field_id": "voltage", "new_name": "voltage_v"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["version"] == 2
    by_id = {f["field_id"]: f for f in body["fields"]}
    assert by_id["voltage"]["name"] == "voltage_v"
    assert by_id["voltage"]["type"] == "float64"
    assert by_id["cycle_id"]["name"] == "cycle"  # untouched

    versions = client.get("/api/v1/schema/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2]

    old = client.get("/api/v1/schema/versions/1").json()
    assert next(f for f in old["fields"] if f["field_id"] == "voltage")["name"] == "volt"


def test_rename_unknown_field_404(client):
    init_schema(client)
    r = client.post(
        "/api/v1/schema/rename", json={"field_id": "nope", "new_name": "x"}
    )
    assert r.status_code == 404


def test_rename_to_existing_name_rejected(client):
    init_schema(client)
    r = client.post(
        "/api/v1/schema/rename",
        json={"field_id": "voltage", "new_name": "cycle"},
    )
    assert r.status_code == 409


def test_add_nullable_field(client):
    init_schema(client)
    r = client.post(
        "/api/v1/schema/fields",
        json={
            "field_id": "temp",
            "name": "temperature",
            "type": "float64",
            "nullable": True,
        },
    )
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 2
    cur = client.get("/api/v1/schema/current").json()
    assert any(f["field_id"] == "temp" for f in cur["fields"])


def test_add_required_field_rejected(client):
    init_schema(client)
    r = client.post(
        "/api/v1/schema/fields",
        json={
            "field_id": "temp",
            "name": "temperature",
            "type": "float64",
            "nullable": False,
        },
    )
    assert r.status_code == 400
    assert "nullable" in r.json()["error"]


def test_add_duplicate_id_or_name_rejected(client):
    init_schema(client)
    r1 = client.post(
        "/api/v1/schema/fields",
        json={"field_id": "voltage", "name": "v2", "type": "float64"},
    )
    assert r1.status_code == 409
    r2 = client.post(
        "/api/v1/schema/fields",
        json={"field_id": "newf", "name": "volt", "type": "float64"},
    )
    assert r2.status_code == 409
