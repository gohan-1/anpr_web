"""
Prefect pipeline: scan folders -> OCR API -> decide -> save -> route files.
Image quality is judged by the OCR API (its confidence), not locally.

Run:  python -m pipeline.flows                 (uses paths.input_dir from settings.yaml)
      python -m pipeline.flows --input ./data
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil

import sys
from pathlib import Path

import django

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "anpr_web.settings")
django.setup()

from django.utils import timezone  # noqa: E402
from prefect import flow, get_run_logger, task  # noqa: E402

from core.models import Folder, Image, Job  # noqa: E402
from pipeline import api_client  # noqa: E402
from pipeline.config import load_config  # noqa: E402
from pipeline.decision import ImageResult, Thresholds, decide, image_score  # noqa: E402

CFG = load_config()


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def is_readable(path: Path) -> bool:
    """Cheap corrupt-file check so broken files aren't sent to the API (and retried)."""
    import cv2
    import numpy as np
    data = np.fromfile(str(path), dtype=np.uint8)
    return data.size > 0 and cv2.imdecode(data, cv2.IMREAD_UNCHANGED) is not None


@task(retries=CFG["api"]["retries"], retry_delay_seconds=CFG["api"]["retry_delay_seconds"])
def call_api(path: Path) -> dict:
    return api_client.recognize(path, CFG["api"])


@task
def route_files(folder_path: Path, route: str) -> None:
    dest = Path(CFG["paths"]["output_dir"]) / route / folder_path.name
    shutil.copytree(folder_path, dest, dirs_exist_ok=True)   # copy, never move, until trusted


@flow(name="process-folder")
def process_folder(job_id: int, folder_path: Path) -> str:
    log = get_run_logger()
    thresholds = Thresholds.from_config(CFG["decision"])
    exts = {e.lower() for e in CFG["paths"]["image_extensions"]}
    files = sorted(p for p in folder_path.iterdir() if p.suffix.lower() in exts)

    folder = Folder.objects.create(job_id=job_id, path=str(folder_path), name=folder_path.name)

    # S10: duplicates by content hash
    seen, unique, dupes = set(), [], []
    for p in files:
        digest = sha256_of(p)
        (dupes if digest in seen else unique).append((p, digest))
        seen.add(digest)

    rows, results = [], []
    for p, digest in unique:
        if not is_readable(p):                           # S11 corrupt
            rows.append(Image(folder=folder, path=str(p), file_name=p.name, sha256=digest, is_corrupt=True))
            results.append(ImageResult(p.name, corrupt=True))
            continue
        try:
            api = call_api(p)                            # retried automatically
            r = ImageResult(p.name, plate=api["plate"], confidence=api["confidence"])
            raw, err = api["raw"], ""
        except Exception as exc:                         # S9 after retries exhausted
            r = ImageResult(p.name, error=str(exc))
            raw, err = {}, str(exc)
        results.append(r)
        rows.append(Image(
            folder=folder, path=str(p), file_name=p.name, sha256=digest,
            plate=r.plate or "", confidence=r.confidence,
            final_score=image_score(r, thresholds) if r.plate else None,
            api_response=raw, api_error=err,
        ))
    for p, digest in dupes:
        rows.append(Image(folder=folder, path=str(p), file_name=p.name, sha256=digest, is_duplicate=True))
        results.append(ImageResult(p.name, duplicate=True))

    Image.objects.bulk_create(rows)
    d = decide(results, thresholds)

    folder.route = d.route.value
    folder.scenario = d.scenario
    folder.reason = d.reason
    folder.final_plate = d.plate or ""
    folder.final_score = d.score
    folder.candidates = d.candidates
    if d.best_image_id:
        folder.best_image = folder.images.filter(file_name=d.best_image_id).first()
    folder.save()

    route_files(folder_path, d.route.value)
    log.info(f"{folder.name}: {d.route.value} [{d.scenario}] {d.plate or ''} - {d.reason}")
    return d.route.value


@flow(name="anpr-pipeline", log_prints=True)
def run_pipeline(input_dir: str | None = None) -> dict:
    input_path = Path(input_dir or CFG["paths"]["input_dir"]).resolve()
    job = Job.objects.create(input_path=str(input_path))
    summary: dict[str, int] = {}
    try:
        folders = sorted(p for p in input_path.iterdir() if p.is_dir())
        job.folder_count = len(folders)
        job.save()
        for fp in folders:
            route = process_folder(job.id, fp)
            summary[route] = summary.get(route, 0) + 1
        job.status = Job.Status.DONE
    except Exception as exc:
        job.status, job.error = Job.Status.ERROR, str(exc)
        raise
    finally:
        job.finished_at = timezone.now()
        job.save()
    print(f"Job #{job.id} finished: {summary}")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="folder containing one sub-folder per vehicle")
    run_pipeline(ap.parse_args().input)