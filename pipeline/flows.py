# """
# Prefect pipeline: find new folders -> hash -> quality -> API -> decide -> save -> route.

# Run once:            python -m pipeline.flows --input ./data
# Run as a bot:        python -m pipeline.flows --input ./data --watch
#                      (keeps running; checks for new folders every watch_interval_seconds)

# HOW THE PARALLEL PART WORKS
#   .submit() hands a task to a pool of worker threads and returns immediately
#   with a "future" (a ticket you can cash in later for the result). We submit
#   every image in a batch, so up to `max_parallel_images` run at the same time,
#   across ALL folders in the batch, not just one folder.
#   Database writes stay in the main thread, one after another, so SQLite is safe.
# """
# from __future__ import annotations

# import argparse
# import hashlib
# import os
# import shutil
# import sys
# import time
# from pathlib import Path

# import django

# ROOT = Path(__file__).resolve().parent.parent
# sys.path.insert(0, str(ROOT / "web"))
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "anpr_web.settings")
# django.setup()

# from django.utils import timezone  # noqa: E402
# from prefect import flow, get_run_logger, task  # noqa: E402
# from prefect.task_runners import ThreadPoolTaskRunner  # noqa: E402

# from core.models import Folder, Image, Job  # noqa: E402
# from pipeline import api_client  # noqa: E402
# from pipeline.config import load_config  # noqa: E402
# from pipeline.decision import ImageResult, Thresholds, decide, image_score  # noqa: E402
# from pipeline.quality import assess_quality  # noqa: E402
# from pipeline.ratelimit import RateLimiter  # noqa: E402

# CFG = load_config()
# PROC = CFG["processing"]
# _LIMITER: RateLimiter | None = None


# def limiter() -> RateLimiter:
#     """One rate limiter shared by all threads, created inside the running process.

#     WHY lazily: in bot mode Prefect sends the flow to a fresh subprocess. A lock
#     created at import time can't be sent along, so each process makes its own here.
#     """
#     global _LIMITER
#     if _LIMITER is None:
#         _LIMITER = RateLimiter(PROC["max_requests_per_second"])
#     return _LIMITER


# # ---------------------------------------------------------------- tasks (run in parallel)
# @task(task_run_name="hash {path.name}")
# def file_hash(path: Path) -> str:
#     h = hashlib.sha256()
#     with open(path, "rb") as f:
#         for chunk in iter(lambda: f.read(1 << 20), b""):
#             h.update(chunk)
#     return h.hexdigest()


# @task(task_run_name="quality {path.name}")
# def check_quality(path: Path) -> dict | None:
#     return assess_quality(path, CFG["quality"])


# @task(task_run_name="api {path.name}",
#       retries=CFG["api"]["retries"], retry_delay_seconds=CFG["api"]["retry_delay_seconds"])
# def call_api(path: Path, quality: dict | None) -> dict | None:
#     # Prefect waits for the quality task first, because we pass its future in as `quality`.
#     if quality is None:          # corrupt image: don't waste an API call
#         return None
#     limiter().wait()             # respect the API's requests-per-second limit
#     return api_client.recognize(path, CFG["api"])


# @task(task_run_name="copy {folder_path.name} to {route}")
# def route_files(folder_path: Path, route: str) -> None:
#     dest = Path(CFG["paths"]["output_dir"]) / route / folder_path.name
#     shutil.copytree(folder_path, dest, dirs_exist_ok=True)   # copy, never move, until trusted


# # ---------------------------------------------------------------- helpers (main thread)
# def find_new_folders(input_path: Path) -> list[Path]:
#     """Folders not yet in the database and not still being written to."""
#     min_age = PROC["min_folder_age_seconds"]
#     now = time.time()
#     candidates = [p for p in sorted(input_path.iterdir())
#                   if p.is_dir() and now - p.stat().st_mtime >= min_age]
#     done = set(Folder.objects.filter(path__in=[str(p) for p in candidates])
#                .values_list("path", flat=True))
#     return [p for p in candidates if str(p) not in done]


# def list_images(folder: Path) -> list[Path]:
#     exts = {e.lower() for e in CFG["paths"]["image_extensions"]}
#     return sorted(p for p in folder.iterdir() if p.suffix.lower() in exts)


# def result_or_error(future):
#     """Returns (value, error_text). A failed task gives back its exception instead of raising."""
#     value = future.result(raise_on_failure=False)
#     if isinstance(value, BaseException):
#         return None, f"{type(value).__name__}: {value}"
#     return value, ""


# def process_batch(job: Job, folders: list[Path], thresholds: Thresholds) -> dict[str, int]:
#     log = get_run_logger()
#     images = {f: list_images(f) for f in folders}

#     # Step 1: hash every image in the batch at once (to find duplicates)
#     hashes = {p: file_hash.submit(p) for imgs in images.values() for p in imgs}
#     unique, dupes = {}, {}
#     for f, imgs in images.items():
#         seen = set()
#         unique[f], dupes[f] = [], []
#         for p in imgs:
#             digest, _ = result_or_error(hashes[p])
#             (dupes[f] if digest in seen else unique[f]).append((p, digest))
#             seen.add(digest)

#     # Step 2: quality + API for every unique image in the batch, all in parallel
#     quality_f = {p: check_quality.submit(p) for f in folders for p, _ in unique[f]}
#     api_f = {p: call_api.submit(p, quality_f[p]) for p in quality_f}

#     # Step 3: as each folder's images finish, decide and save (main thread, one at a time)
#     summary: dict[str, int] = {}
#     copies = []
#     for f in folders:
#         folder = Folder.objects.create(job=job, path=str(f), name=f.name)
#         rows, results = [], []
#         for p, digest in unique[f]:
#             q, _ = result_or_error(quality_f[p])
#             if q is None:
#                 rows.append(Image(folder=folder, path=str(p), file_name=p.name, sha256=digest, is_corrupt=True))
#                 results.append(ImageResult(p.name, corrupt=True))
#                 continue
#             api, err = result_or_error(api_f[p])
#             if err:
#                 r = ImageResult(p.name, quality=q["quality"], error=err)
#             else:
#                 r = ImageResult(p.name, quality=q["quality"], plate=api["plate"], confidence=api["confidence"])
#             results.append(r)
#             rows.append(Image(
#                 folder=folder, path=str(p), file_name=p.name, sha256=digest,
#                 quality=q["quality"], quality_details=q, plate=r.plate or "", confidence=r.confidence,
#                 final_score=image_score(r, thresholds) if r.plate else None,
#                 api_response=(api or {}).get("raw", {}) if not err else {}, api_error=err))
#         for p, digest in dupes[f]:
#             rows.append(Image(folder=folder, path=str(p), file_name=p.name, sha256=digest, is_duplicate=True))
#             results.append(ImageResult(p.name, duplicate=True))
#         Image.objects.bulk_create(rows)

#         d = decide(results, thresholds)
#         folder.route, folder.scenario, folder.reason = d.route.value, d.scenario, d.reason
#         folder.final_plate, folder.final_score, folder.candidates = d.plate or "", d.score, d.candidates
#         if d.best_image_id:
#             folder.best_image = folder.images.filter(file_name=d.best_image_id).first()
#         folder.save()

#         copies.append(route_files.submit(f, d.route.value))
#         summary[d.route.value] = summary.get(d.route.value, 0) + 1
#         log.info(f"{f.name}: {d.route.value} [{d.scenario}] {d.plate or ''}")

#     for c in copies:
#         c.wait()
#     return summary


# # ---------------------------------------------------------------- the flow
# @flow(name="anpr-pipeline", log_prints=True,
#       task_runner=ThreadPoolTaskRunner(max_workers=PROC["max_parallel_images"]))
# def run_pipeline(input_dir: str | None = None) -> dict:
#     input_path = Path(input_dir or CFG["paths"]["input_dir"]).resolve()
#     folders = find_new_folders(input_path)
#     if not folders:
#         print("No new folders.")
#         return {}

#     thresholds = Thresholds.from_config(CFG["decision"])
#     job = Job.objects.create(input_path=str(input_path), folder_count=len(folders))
#     summary: dict[str, int] = {}
#     started = time.monotonic()
#     try:
#         size = PROC["chunk_size"]
#         for i in range(0, len(folders), size):
#             batch = folders[i:i + size]
#             for route, n in process_batch(job, batch, thresholds).items():
#                 summary[route] = summary.get(route, 0) + n
#             print(f"Batch done: {min(i + size, len(folders))}/{len(folders)} folders")
#         job.status = Job.Status.DONE
#     except Exception as exc:
#         job.status, job.error = Job.Status.ERROR, str(exc)
#         raise
#     finally:
#         job.finished_at = timezone.now()
#         job.save()
#     print(f"Job #{job.id}: {len(folders)} folders in {time.monotonic() - started:.1f}s -> {summary}")
#     return summary


# if __name__ == "__main__":
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--input", help="folder containing one sub-folder per vehicle")
#     ap.add_argument("--watch", action="store_true", help="bot mode: keep running and process new folders")
#     args = ap.parse_args()
#     if args.watch:
#         # serve() keeps this program running and starts run_pipeline on a timer.
#         # Each run only picks up NEW folders, so nothing is processed twice.
#         # We import the flow by its module name so Prefect can load it in a subprocess.
#         from pipeline.flows import run_pipeline as bot_flow
#         bot_flow.serve(name="anpr-bot", interval=PROC["watch_interval_seconds"],
#                            parameters={"input_dir": args.input})
#     else:
#         run_pipeline(args.input)
