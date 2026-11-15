"""FastAPI entrypoint.

Endpoints
---------
POST   /api/v1/schema/init                 initialise the structure
GET    /api/v1/schema/current              current structure
GET    /api/v1/schema/versions             all committed versions
GET    /api/v1/schema/versions/{version}   one historical version
POST   /api/v1/schema/rename               rename a column (new version)
POST   /api/v1/schema/fields               add a nullable column (new version)
POST   /api/v1/schema/tighten/proposals    open a tighten-to-required proposal
POST   /api/v1/schema/tighten/proposals/{id}/decide   adjudicate a proposal
GET    /api/v1/schema/tighten/proposals    list proposals
POST   /api/v1/batches                     upload a Parquet batch
GET    /api/v1/catalog                     structure + batch list, one snapshot
GET    /api/v1/batches                     list batches (same snapshot reads)
GET    /api/v1/query                       field-ID-aligned row query
GET    /health                             liveness probe
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse

from . import config, database as db, ingest, query, registry
from .errors import RegistryError
from .schemas import FieldSpec, InitSchemaRequest, RenameRequest, TightenRequest


@asynccontextmanager
async def lifespan(_: FastAPI):
    config.ensure_dirs()
    db.init_db()
    yield


app = FastAPI(
    title="Battery Cycle Test Schema Registry",
    version="1.0.0",
    description="Parquet ingestion with a stable field-ID schema registry.",
    lifespan=lifespan,
)


@app.exception_handler(RegistryError)
def _registry_error_handler(_: Request, exc: RegistryError) -> JSONResponse:
    payload: dict = {"error": exc.message}
    if exc.details:
        payload["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=payload)


# --------------------------------------------------------------------------- #
# schema management
# --------------------------------------------------------------------------- #

@app.post("/api/v1/schema/init", status_code=201)
def init_schema(body: InitSchemaRequest):
    return registry.initialize_schema(
        [f.model_dump() for f in body.fields]
    )


@app.get("/api/v1/schema/current")
def current_schema():
    with db.read_tx() as conn:
        version = registry.current_version(conn)
        if version is None:
            return JSONResponse(
                status_code=404,
                content={"error": "schema has not been initialised yet"},
            )
        return {
            "schema": registry.get_version_meta(conn, version),
            "fields": registry.get_fields(conn, version),
        }


@app.get("/api/v1/schema/versions")
def versions():
    with db.read_tx() as conn:
        return {"versions": registry.list_versions(conn)}


@app.get("/api/v1/schema/versions/{version}")
def version_detail(version: int):
    return query.get_version_detail(version)


@app.post("/api/v1/schema/rename", status_code=201)
def rename_field(body: RenameRequest):
    return registry.rename_field(body.field_id, body.new_name)


@app.post("/api/v1/schema/fields", status_code=201)
def add_field(body: FieldSpec):
    return registry.add_field(body.model_dump())


@app.post("/api/v1/schema/tighten/proposals", status_code=202)
def create_tighten(body: TightenRequest):
    return registry.create_tightening_proposal(body.field_id)


@app.post("/api/v1/schema/tighten/proposals/{proposal_id}/decide")
def decide_tighten(proposal_id: str):
    return registry.decide_tightening(proposal_id)


@app.get("/api/v1/schema/tighten/proposals")
def list_tighten():
    with db.read_tx() as conn:
        return {"proposals": registry.list_proposals(conn)}


# --------------------------------------------------------------------------- #
# batches & query
# --------------------------------------------------------------------------- #

@app.post("/api/v1/batches", status_code=201)
async def upload_batch(
    file: UploadFile = File(...),
    schema_version: int = Form(...),
):
    if not file.filename or not file.filename.lower().endswith(".parquet"):
        return JSONResponse(
            status_code=400,
            content={"error": "expected a .parquet upload"},
        )
    result = ingest.ingest_batch(file.file, file.filename, schema_version)
    return result


@app.get("/api/v1/catalog")
def catalog():
    return query.get_catalog()


@app.get("/api/v1/batches")
def batches():
    with db.read_tx() as conn:
        return {"batches": registry.list_batches(conn)}


@app.get("/api/v1/query")
def query_rows(
    field_ids: str | None = None,
    batch_ids: str | None = None,
    limit: int = config.DEFAULT_QUERY_LIMIT,
    offset: int = 0,
):
    fids = [s.strip() for s in field_ids.split(",")] if field_ids else None
    bids = [s.strip() for s in batch_ids.split(",")] if batch_ids else None
    return query.query_rows(fids, bids, limit, offset)


@app.get("/health")
def health():
    return {"status": "ok"}
