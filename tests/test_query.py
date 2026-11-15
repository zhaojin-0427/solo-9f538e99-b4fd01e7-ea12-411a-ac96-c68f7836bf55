"""Query side: field-ID alignment, projection, filtering, pagination."""
from __future__ import annotations

from tests.conftest import init_schema, make_parquet, upload


def _batch(cycles, name="cycle"):
    return make_parquet(
        {
            name: cycles,
            "volt": [float(c) for c in cycles],
            "sn": [f"s{c}" for c in cycles],
        }
    )


def test_query_pagination_and_filtering(client):
    init_schema(client)
    b1 = upload(client, _batch([1, 2, 3]), 1).json()["batch_id"]
    b2 = upload(client, _batch([4, 5]), 1).json()["batch_id"]

    q = client.get("/api/v1/query", params={"limit": 2, "offset": 0}).json()
    assert q["total_rows"] == 5
    assert q["returned_rows"] == 2
    assert [r["cycle_id"] for r in q["rows"]] == [1, 2]

    q2 = client.get("/api/v1/query", params={"limit": 2, "offset": 2}).json()
    assert [r["cycle_id"] for r in q2["rows"]] == [3, 4]

    q3 = client.get("/api/v1/query", params={"limit": 2, "offset": 4}).json()
    assert [r["cycle_id"] for r in q3["rows"]] == [5]

    qb = client.get(
        "/api/v1/query", params={"batch_ids": b2, "limit": 10}
    ).json()
    assert qb["total_batches"] == 1
    assert {r["batch_id"] for r in qb["rows"]} == {b2}
    assert b1 not in {r["batch_id"] for r in qb["rows"]}


def test_query_field_projection_and_unknown_ids(client):
    init_schema(client)
    upload(client, _batch([1]), 1)
    q = client.get(
        "/api/v1/query", params={"field_ids": "cycle_id"}
    ).json()
    assert set(q["rows"][0].keys()) == {"batch_id", "cycle_id"}
    assert q["field_ids"] == ["cycle_id"]

    r = client.get("/api/v1/query", params={"field_ids": "nope"})
    assert r.status_code == 400

    r2 = client.get("/api/v1/query", params={"batch_ids": "nope"})
    assert r2.status_code == 400

    r3 = client.get("/api/v1/query", params={"limit": 0})
    assert r3.status_code == 400


def test_query_backfills_null_for_batches_before_a_field(client):
    init_schema(client)
    upload(client, _batch([1]), 1)  # before 'temp' exists

    client.post(
        "/api/v1/schema/fields",
        json={"field_id": "temp", "name": "temperature", "type": "float64"},
    )
    raw = make_parquet(
        {
            "cycle": [2],
            "volt": [2.0],
            "sn": ["s2"],
            "temperature": [31.5],
        }
    )
    upload(client, raw, 2)

    q = client.get(
        "/api/v1/query", params={"field_ids": "cycle_id,temp"}
    ).json()
    by_cycle = {r["cycle_id"]: r for r in q["rows"]}
    assert by_cycle[1]["temp"] is None
    assert by_cycle[2]["temp"] == 31.5


def test_query_requires_initialised_schema(client):
    r = client.get("/api/v1/query")
    assert r.status_code == 404
