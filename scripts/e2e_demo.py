"""End-to-end scenario against a real uvicorn server over HTTP.

Run:  python3 scripts/e2e_demo.py [base_url]
Prepares Parquet fixtures in an isolated temp dir (never inside this repo's
import paths) and exercises every documented requirement.
"""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import urllib.request
import urllib.error

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
FIXTURE_DIR = tempfile.mkdtemp(prefix="bsr-fixtures-")


def call(method: str, path: str, body=None, files=None):
    url = BASE + path
    headers = {}
    data = None
    if files is not None:
        boundary = "----bsrboundary7MAv"
        lines = []
        for key, value in body.items():
            lines.append(f"--{boundary}".encode())
            lines.append(f'Content-Disposition: form-data; name="{key}"'.encode())
            lines.append(b"")
            lines.append(str(value).encode())
        for field, (filename, content) in files.items():
            lines.append(f"--{boundary}".encode())
            lines.append(
                f'Content-Disposition: form-data; name="{field}"; '
                f'filename="{filename}"'.encode()
            )
            lines.append(b"Content-Type: application/octet-stream")
            lines.append(b"")
            lines.append(content)
        lines.append(f"--{boundary}--".encode())
        lines.append(b"")
        data = b"\r\n".join(lines)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def write_parquet(name: str, columns: dict) -> str:
    arrays = {k: pa.array(v) for k, v in columns.items()}
    path = os.path.join(FIXTURE_DIR, name)
    pq.write_table(pa.table(arrays), path)
    return path


def main() -> int:
    failures = []

    def check(label, cond, detail=""):
        status = "PASS" if cond else "FAIL"
        print(f"[{status}] {label} {detail}")
        if not cond:
            failures.append(label)

    # 0. health
    s, b = call("GET", "/health")
    check("health endpoint", s == 200 and b["status"] == "ok", str(b))

    # 1. initial schema
    s, b = call("POST", "/api/v1/schema/init", {"fields": [
        {"field_id": "cycle_id", "name": "cycle", "type": "int64", "nullable": False},
        {"field_id": "voltage", "name": "volt", "type": "float64", "nullable": True},
        {"field_id": "serial", "name": "sn", "type": "string", "nullable": True},
    ]})
    check("initialise schema v1", s == 201 and b["version"] == 1)

    # 2. old batch (v1)
    p1 = write_parquet("old.parquet", {
        "cycle": [1, 2], "volt": [3.7, 3.8], "sn": ["A", "B"],
    })
    with open(p1, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 1},
                    {"file": ("old.parquet", fh.read())})
    check("upload v1 batch", s == 201 and b["row_count"] == 2, str(b))
    old_batch = b["batch_id"]

    # 3. rename voltage -> voltage_v (v2)
    s, b = call("POST", "/api/v1/schema/rename",
                {"field_id": "voltage", "new_name": "voltage_v"})
    check("rename produces v2", s == 201 and b["version"] == 2)

    # 4. new field reuses old physical name 'volt' (v3)
    s, b = call("POST", "/api/v1/schema/fields", {
        "field_id": "impedance", "name": "volt", "type": "float64",
        "nullable": True,
    })
    check("add field reusing old name -> v3", s == 201 and b["version"] == 3)

    # 5. new-structure batch with both voltage_v and reused 'volt'
    p2 = write_parquet("new.parquet", {
        "cycle": [9], "voltage_v": [4.0], "volt": [0.05], "sn": ["Z"],
    })
    with open(p2, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 3},
                    {"file": ("new.parquet", fh.read())})
    check("upload v3 batch", s == 201, str(b))

    # 6. old-structure upload after evolution still accepted (declared v1)
    with open(p1, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 1},
                    {"file": ("old_again.parquet", fh.read())})
    check("old-structure upload still readable post-rename",
          s == 201 and b["columns"]["voltage"] == "volt", str(b))

    # 7. query alignment: no cross-wiring
    s, b = call("GET", "/api/v1/query?field_ids=cycle_id,voltage,impedance")
    check("query aligned by field id", s == 200 and b["total_rows"] == 5)
    by_cycle = {r["cycle_id"]: r for r in b["rows"]}
    check("old batch: voltage kept, impedance backfilled null",
          by_cycle[1]["voltage"] == 3.7 and by_cycle[1]["impedance"] is None)
    check("new batch: no value cross-wiring",
          by_cycle[9]["voltage"] == 4.0 and by_cycle[9]["impedance"] == 0.05)

    # 8. tightening with an offending batch -> rejected, batch named
    p3 = write_parquet("nullvolt.parquet", {
        "cycle": pa.array([5], type=pa.int64()),
        "voltage_v": pa.array([None], type=pa.float64()),
        "sn": pa.array(["Q"], type=pa.string()),
    })
    with open(p3, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 3},
                    {"file": ("nullvolt.parquet", fh.read())})
    check("nullable null batch accepted pre-tightening", s == 201)
    null_batch = b["batch_id"]

    s, b = call("POST", "/api/v1/schema/tighten/proposals",
                {"field_id": "voltage"})
    check("proposal accepted", s == 202)
    pid = b["proposal_id"]
    s, b = call("POST", f"/api/v1/schema/tighten/proposals/{pid}/decide")
    check("tightening rejected with offending batch",
          s == 409 and null_batch in
          [o["batch_id"] for o in b["details"]["offending_batches"]], str(b))

    # proposal marked rejected (committed state), rule not in force
    s, b = call("GET", "/api/v1/schema/tighten/proposals")
    check("rejected proposal persisted",
          any(p["state"] == "rejected" for p in b["proposals"]))
    s, b = call("GET", "/api/v1/schema/current")
    check("schema unchanged after rejection", b["schema"]["version"] == 3)

    # 9. catalog: single snapshot response carries structure + batches
    s, b = call("GET", "/api/v1/catalog")
    check("catalog snapshot has structure and 4 batches",
          s == 200 and b["schema"]["version"] == 3 and len(b["batches"]) == 4)

    # 10. unknown column and type mismatch rejected
    bad1 = write_parquet("unknown.parquet", {
        "cycle": [1], "voltage_v": [1.0], "volt": [0.1], "sn": ["x"],
        "rogue": [1],
    })
    with open(bad1, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 3},
                    {"file": ("unknown.parquet", fh.read())})
    check("unknown column rejected", s == 400 and "rogue" in b["error"])

    bad2 = write_parquet("typemix.parquet", {
        "cycle": pa.array([1.5], type=pa.float64()),
        "voltage_v": [1.0], "volt": [0.1], "sn": ["x"],
    })
    with open(bad2, "rb") as fh:
        s, b = call("POST", "/api/v1/batches",
                    {"schema_version": 3},
                    {"file": ("typemix.parquet", fh.read())})
    check("type mismatch rejected", s == 400 and "int64" in b["error"])

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S): {failures}")
        return 1
    print("ALL E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
