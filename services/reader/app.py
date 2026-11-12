"""
Reader service: the HTTP API around scanner.py.

Run:   python -m services.reader --port 8101 --shard 1/1
Docs:  open http://127.0.0.1:8101/docs  (FastAPI builds this page from the code below)

HOW FASTAPI WORKS (the short version):
  @app.get("/path") above a function = "call this function for GET /path".
  Function arguments become query parameters (?limit=50) or URL parts (/folders/{folder_id}).
  The `response_model` (a Pydantic class) describes the JSON that comes back; FastAPI
  checks it and shows it in the /docs page, so the API documents itself.
"""
import functools
import os
import socket
from datetime import datetime
from pathlib import Path

import services.common.django_setup  # noqa: F401  (must come before Django imports)

from django.conf import settings
from django.db import close_old_connections, connection
from django.db.models import Count, Sum
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core.models import IntakeFolder, IntakeImage
from pipeline.config import load_config

from .scanner import Shard, load_sources, scan_source

# ---------------------------------------------------------------- settings of this copy
CFG = load_config()["reader"]
SOURCES = load_sources(CFG, settings.PROJECT_ROOT)
SHARD = Shard.parse(os.environ.get("READER_SHARD", "1/1"))
INSTANCE = os.environ.get("READER_INSTANCE") or f"{socket.gethostname()}:{os.environ.get('READER_PORT', '?')}"
EXTENSIONS = {e.lower() for e in CFG["image_extensions"]}
ROOTS = [s.path for s in SOURCES]

app = FastAPI(
    title="Reader service",
    version="1.0",
    description="Finds new vehicle folders, reads their images and registers them for the pipeline. "
                "Run several copies with different --shard values to share the work.",
)


def with_db(func):
    """Django normally tidies database connections after each request; outside Django we do it here."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        close_old_connections()
        try:
            return func(*args, **kwargs)
        finally:
            close_old_connections()
    return wrapper


# ---------------------------------------------------------------- response shapes
class SourceOut(BaseModel):
    name: str
    path: str
    accuracy: str
    priority: int
    exists: bool


class HealthOut(BaseModel):
    status: str
    instance: str
    shard: str
    environment: str
    read_only: bool
    sources: list[SourceOut]


class ScanSourceOut(BaseModel):
    source: str
    seen: int
    other_shard: int
    already_known: int
    too_new: int
    registered: int
    images: int
    bytes: int
    errors: list[str]
    seconds: float


class ScanOut(BaseModel):
    instance: str
    shard: str
    registered: int
    results: list[ScanSourceOut]


class ImageOut(BaseModel):
    id: int
    file_name: str
    path: str
    size_bytes: int
    sha256: str
    is_duplicate: bool
    modified_at: datetime


class FolderOut(BaseModel):
    id: int
    name: str
    path: str
    source: str
    accuracy: str
    priority: int
    stage: str
    image_count: int
    duplicate_count: int
    total_bytes: int
    discovered_at: datetime
    discovered_by: str


class FolderDetailOut(FolderOut):
    images: list[ImageOut]


class StatsOut(BaseModel):
    total_folders: int
    total_images: int
    total_bytes: int
    by_stage: dict[str, int]
    by_accuracy: dict[str, int]
    waiting_by_source: dict[str, int]


def folder_out(f: IntakeFolder) -> dict:
    return {k: getattr(f, k) for k in FolderOut.model_fields}


# ---------------------------------------------------------------- endpoints
@app.get("/health", response_model=HealthOut, summary="Is this copy alive and connected?")
@with_db
def health():
    try:
        connection.ensure_connection()
    except Exception as exc:
        raise HTTPException(503, f"Database not reachable: {exc}")
    return {"status": "ok", "instance": INSTANCE, "shard": str(SHARD), "environment": settings.ANPR_ENV,
            "read_only": settings.READ_ONLY,
            "sources": [{"name": s.name, "path": str(s.path), "accuracy": s.accuracy, "priority": s.priority,
                         "exists": s.path.is_dir()} for s in SOURCES]}


@app.post("/scan", response_model=ScanOut, summary="Scan every source for new folders (this copy's shard only)")
@with_db
def scan():
    if settings.READ_ONLY:
        raise HTTPException(403, f"Read-only connection to {settings.ANPR_ENV}: scanning is disabled.")
    results = [scan_source(s, SHARD, INSTANCE, extensions=EXTENSIONS,
                           min_age_seconds=CFG["min_folder_age_seconds"], workers=CFG["read_workers"])
    for s in sorted(SOURCES, key=lambda s: s.priority)]
    return {"instance": INSTANCE, "shard": str(SHARD), "registered": sum(r.registered for r in results),
            "results": [vars(r) for r in results]}


@app.get("/folders", response_model=list[FolderOut], summary="Registered folders, in pipeline order")
@with_db
def list_folders(stage: str | None = Query(None, description="reading, needs_llm, needs_trip, manual, done, failed..."),
                 accuracy: str | None = Query(None, description="low or high"),
                 source: str | None = None,
                 limit: int = Query(50, ge=1, le=500),
                 offset: int = Query(0, ge=0)):
    qs = IntakeFolder.objects.all()
    if stage:
        qs = qs.filter(stage=stage)
    if accuracy:
        qs = qs.filter(accuracy=accuracy)
    if source:
        qs = qs.filter(source=source)
    # Pipeline order: lowest priority number first (low accuracy), then oldest first
    qs = qs.order_by("priority", "discovered_at", "id")[offset:offset + limit]
    return [folder_out(f) for f in qs]


@app.get("/folders/{folder_id}", response_model=FolderDetailOut, summary="One folder with its images")
@with_db
def get_folder(folder_id: int):
    folder = IntakeFolder.objects.filter(pk=folder_id).first()
    if folder is None:
        raise HTTPException(404, "No such folder")
    images = [{k: getattr(i, k) for k in ImageOut.model_fields} for i in folder.images.all()]
    return {**folder_out(folder), "images": images}


@app.get("/images/{image_id}/file", summary="The image file itself",
         responses={200: {"content": {"image/jpeg": {}}}})
@with_db
def get_image_file(image_id: int):
    image = IntakeImage.objects.filter(pk=image_id).first()
    if image is None:
        raise HTTPException(404, "No such image")
    path = Path(image.path).resolve()
    # Safety: only ever send files from inside a configured source folder
    if not any(path.is_relative_to(root) for root in ROOTS):
        raise HTTPException(403, "That file is outside the configured sources")
    if not path.is_file():
        raise HTTPException(404, "Image file is no longer on disk")
    return FileResponse(path)


@app.get("/stats", response_model=StatsOut, summary="Counts for monitoring")
@with_db
def stats():
    totals = IntakeFolder.objects.aggregate(folders=Count("id"), images=Sum("image_count"), size=Sum("total_bytes"))
    return {
        "total_folders": totals["folders"] or 0,
        "total_images": totals["images"] or 0,
        "total_bytes": totals["size"] or 0,
        "by_stage": dict(IntakeFolder.objects.values_list("stage").annotate(n=Count("id"))),
        "by_accuracy": dict(IntakeFolder.objects.values_list("accuracy").annotate(n=Count("id"))),
        "waiting_by_source": dict(IntakeFolder.objects.filter(stage=IntakeFolder.Stage.READING)
                                  .values_list("source").annotate(n=Count("id"))),
    }