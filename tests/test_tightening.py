"""Tightening nullable -> required: proposals, adjudication, edge cases."""
from __future__ import annotations

import threading

import pyarrow as pa

from app import parquet_store, registry
from tests.conftest import init_schema, make_parquet, upload


def _batch(cycle, volt=None, sn=None):
    cols = {"cycle": pa.array(cycle, type=pa.int64())}
    if volt is not None:
        cols["volt"] = pa.array(volt, type=pa.float64())
    if sn is not None:
        cols["sn"] = pa.array(sn, type=pa.string())
    return make_parquet(cols)


def _propose(client, field_id):
    return client.post(
        "/api/v1/schema/tighten/proposals", json={"field_id": field_id}
    )


def test_tighten_rejected_names_offending_batches(client):
    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)  # complete
    bad = upload(client, _batch([2], [None], ["b"]), 1)  # null voltage
    assert bad.status_code == 201  # voltage still nullable

    p = _propose(client, "voltage")
    assert p.status_code == 202
    pid = p.json()["proposal_id"]

    d = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert d.status_code == 409
    details = d.json()["details"]
    offending = [o["batch_id"] for o in details["offending_batches"]]
    assert bad.json()["batch_id"] in offending
    # rule NOT in effect: schema still at v1, voltage still nullable
    cur = client.get("/api/v1/schema/current").json()
    f = next(x for x in cur["fields"] if x["field_id"] == "voltage")
    assert f["nullable"] is True

    listed = client.get("/api/v1/schema/tighten/proposals").json()
    assert listed["proposals"][0]["state"] == "rejected"


def test_tighten_succeeds_when_all_complete(client):
    init_schema(client)
    b = upload(client, _batch([1], [1.0], ["a"]), 1)
    assert b.status_code == 201

    pid = _propose(client, "voltage").json()["proposal_id"]
    d = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert d.status_code == 200, d.text
    body = d.json()
    assert body["state"] == "committed"
    assert body["version"] == 2

    cur = client.get("/api/v1/schema/current").json()
    f = next(x for x in cur["fields"] if x["field_id"] == "voltage")
    assert f["nullable"] is False


def test_tighten_missing_column_counts_as_offence(client):
    init_schema(client)
    # batch without the nullable volt column at all
    assert upload(client, _batch([1], sn=["a"]), 1).status_code == 201
    pid = _propose(client, "voltage").json()["proposal_id"]
    d = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert d.status_code == 409
    assert d.json()["details"]["offending_batches"][0]["reason"] == (
        "field missing from batch"
    )


def test_old_structure_upload_cannot_bypass_committed_required(client):
    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)
    pid = _propose(client, "voltage").json()["proposal_id"]
    client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")

    # after the rule is live, an old-structure file lacking the column fails
    r = upload(client, _batch([2], sn=["b"]), 1)
    assert r.status_code == 400
    assert "required" in r.json()["error"]

    # an old-structure file that still has the column, populated, is fine
    r2 = upload(client, _batch([3], [2.0], ["c"]), 1)
    assert r2.status_code == 201


def test_pending_proposal_blocks_second_one(client):
    init_schema(client)
    _propose(client, "voltage")
    r = _propose(client, "serial")
    assert r.status_code == 409


def test_decide_unknown_or_settled_proposal(client):
    init_schema(client)
    assert client.post(
        "/api/v1/schema/tighten/proposals/nope/decide"
    ).status_code == 404

    pid = _propose(client, "voltage").json()["proposal_id"]
    client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    # settling again -> conflict
    again = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert again.status_code == 409


def test_upload_during_check_is_part_of_adjudication(client, monkeypatch):
    """A batch uploaded while the scan runs must be adjudicated too.

    The worker threads call the service layer directly because FastAPI's
    TestClient (anyio portal) is bound to the creating thread; concurrency
    semantics under test live in the service layer anyway.
    """
    import io

    from app import ingest
    from app.errors import RegistryError

    class _Result:
        def __init__(self, exc=None, value=None):
            self.exc = exc
            self._value = value

        @property
        def status_code(self):
            return 409 if self.exc is not None else 200

        def json(self):
            if self.exc is not None:
                return {"details": self.exc.details}
            return self._value

    def _decide(pid):
        try:
            return _Result(value=registry.decide_tightening(pid))
        except RegistryError as exc:
            return _Result(exc=exc)

    def _ingest(raw: bytes):
        return ingest.ingest_batch(io.BytesIO(raw), "thread-batch.parquet", 1)

    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)
    pid = _propose(client, "voltage").json()["proposal_id"]

    blocker = threading.Event()
    released = threading.Event()
    original = parquet_store.batch_has_nulls

    def slow_has_nulls(path, column):
        # block the first scan of the single existing batch
        if not released.is_set():
            released.set()
            blocker.wait(timeout=10)
        return original(path, column)

    monkeypatch.setattr(registry, "batch_has_nulls", slow_has_nulls)

    decision_holder = {}

    t = threading.Thread(
        target=lambda: decision_holder.update(resp=_decide(pid))
    )
    t.start()
    assert released.wait(timeout=5)  # scan is in progress

    # upload concurrently: a complete batch, should not break the tightening
    new_batch_id = _ingest(_batch([7], [9.9], ["z"]))["batch_id"]

    blocker.set()
    t.join(timeout=15)
    assert not t.is_alive()
    resp = decision_holder["resp"]
    assert resp.status_code == 200
    assert new_batch_id in resp.json()["batches_uploaded_during_check"]

    # now the opposite: offending batch landing during the late re-scan
    monkeypatch.undo()
    upload(client, _batch([1], [1.0], ["a"]), 1)  # extra clean batch
    pid2 = _propose(client, "serial").json()["proposal_id"]

    scan_started = threading.Event()
    blocker2 = threading.Event()

    def slow2(path, column):
        scan_started.set()
        blocker2.wait(timeout=10)
        return original(path, column)

    monkeypatch.setattr(registry, "batch_has_nulls", slow2)
    holder2 = {}

    t2 = threading.Thread(
        target=lambda: holder2.update(resp=_decide(pid2))
    )
    t2.start()
    assert scan_started.wait(timeout=5)
    bad_id = _ingest(_batch([8], [1.0], [None]))["batch_id"]
    blocker2.set()
    t2.join(timeout=15)
    assert not t2.is_alive()
    resp2 = holder2["resp"]
    assert resp2.status_code == 409
    ids = [o["batch_id"] for o in resp2.json()["details"]["offending_batches"]]
    assert bad_id in ids


def test_rejected_proposal_state_is_committed_not_rolled_back(client):
    """Regression: the rejection marker must survive even though the API
    responds with an error (the write tx must commit before raising)."""
    init_schema(client)
    upload(client, _batch([2], [None], ["b"]), 1)
    pid = _propose(client, "voltage").json()["proposal_id"]
    d = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert d.status_code == 409

    proposals = client.get(
        "/api/v1/schema/tighten/proposals"
    ).json()["proposals"]
    mine = next(p for p in proposals if p["proposal_id"] == pid)
    assert mine["state"] == "rejected"
    assert mine["decided_at"] is not None
    # a rejected proposal cannot be decided twice
    again = client.post(
        f"/api/v1/schema/tighten/proposals/{pid}/decide"
    )
    assert again.status_code == 409


def test_field_added_then_tightened_blocks_old_structure_upload(client):
    """A field added after v1, once required, cannot be bypassed by an
    old-structure upload that simply lacks the column."""
    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)

    # add nullable field X (v2); old batches legitimately lack it
    r = client.post(
        "/api/v1/schema/fields",
        json={"field_id": "temp", "name": "temperature", "type": "float64"},
    )
    assert r.status_code == 201
    assert upload(client, _batch([2], [2.0], ["b"]), 1).status_code == 201

    # a v2 batch carrying X with data
    with_x = make_parquet({
        "cycle": pa.array([3], type=pa.int64()),
        "volt": pa.array([3.0], type=pa.float64()),
        "sn": pa.array(["c"], type=pa.string()),
        "temperature": pa.array([25.0], type=pa.float64()),
    })
    assert upload(client, with_x, 2).status_code == 201

    # tightening X fails while old batches lack it, and names them
    pid = _propose(client, "temp").json()["proposal_id"]
    d = client.post(f"/api/v1/schema/tighten/proposals/{pid}/decide")
    assert d.status_code == 409
    reasons = d.json()["details"]["offending_batches"]
    assert all(o["reason"] == "field missing from batch" for o in reasons)

    # establish the enforceable state on a field every batch has: serial
    pid2 = _propose(client, "serial").json()["proposal_id"]
    assert client.post(
        f"/api/v1/schema/tighten/proposals/{pid2}/decide"
    ).status_code == 200

    # old-structure upload (declared v1) missing the now-required serial fails
    missing = make_parquet({
        "cycle": pa.array([5], type=pa.int64()),
        "volt": pa.array([5.0], type=pa.float64()),
    })
    r = upload(client, missing, 1)
    assert r.status_code == 400
    assert "serial" in r.json()["error"]
    # and the stored batch set is unchanged after the rejection
    cat = client.get("/api/v1/catalog").json()
    assert len(cat["batches"]) == 3


def test_stale_pending_proposal_is_marked_rejected(client):
    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)
    pid = _propose(client, "voltage").json()["proposal_id"]

    # structure evolves while the proposal sits pending
    r = client.post(
        "/api/v1/schema/rename",
        json={"field_id": "serial", "new_name": "serial_no"},
    )
    assert r.status_code == 201

    d = client.post(
        f"/api/v1/schema/tighten/proposals/{pid}/decide"
    )
    assert d.status_code == 409
    listed = client.get("/api/v1/schema/tighten/proposals").json()
    mine = next(p for p in listed["proposals"] if p["proposal_id"] == pid)
    assert mine["state"] == "rejected"

    # a new proposal is allowed once the stale one is settled
    assert _propose(client, "voltage").status_code == 202


def test_pending_proposal_survives_restart(client, monkeypatch):
    """Pending proposals live in SQLite, so a process restart can finish them."""
    init_schema(client)
    upload(client, _batch([1], [1.0], ["a"]), 1)
    pid = _propose(client, "voltage").json()["proposal_id"]

    # simulate restart by dropping and recreating the TestClient against the
    # same DATA_DIR (registry state must come entirely from disk)
    from fastapi.testclient import TestClient

    from app.main import app as fastapi_app

    with TestClient(fastapi_app) as client2:
        r = client2.post(
            f"/api/v1/schema/tighten/proposals/{pid}/decide"
        )
        assert r.status_code == 200
        assert r.json()["state"] == "committed"
        cat = client2.get("/api/v1/catalog").json()
        assert cat["schema"]["version"] == 2
        assert len(cat["batches"]) == 1
