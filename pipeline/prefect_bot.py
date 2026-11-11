from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger, tags, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run
from prefect.task_runners import ThreadPoolTaskRunner

from pipeline.processor import ImageAnalysis, list_images
from pipeline.processor import ocr_image as ocr_one

_BOTS: dict[str, tuple] = {}     # bot name -> (cfg, credentials, limiter), in this process only


def register_bot(name: str, cfg: dict, credentials, limiter) -> None:
    _BOTS[name] = (cfg, credentials, limiter)


@task(name="ocr-image", task_run_name="ocr {path.name}", cache_policy=NO_CACHE)
def ocr_image(path: Path, bot: str) -> ImageAnalysis:
    cfg, credentials, limiter = _BOTS[bot]
    item = ocr_one(path, cfg["api"], credentials, limiter)   # timed; backoff + rate limit
    if item.error:                           # recorded on the image, the folder still finishes
        get_run_logger().warning(f"{path.name}: {item.error}")
    return item


def _run_name() -> str:
    p = flow_run.parameters
    return f"{p['bot']}: {Path(p['folder']).name}"


@flow(name="anpr-bot-folder", flow_run_name=_run_name, persist_result=False,
      task_runner=ThreadPoolTaskRunner(max_workers=8))
def process_folder(folder: str, bot: str) -> list[ImageAnalysis]:
    log = get_run_logger()
    cfg = _BOTS[bot][0]
    images = list_images(Path(folder), cfg["paths"]["image_extensions"])
    futures = [ocr_image.submit(p, bot) for p in images]
    results = [f.result() for f in futures]
    read = sum(1 for r in results if r.plate)
    errors = sum(1 for r in results if r.error)
    log.info(f"{len(images)} image(s): {read} plate(s) read, {errors} API error(s)")
    return results


def analyse_folder_with_prefect(folder: Path, bot: str) -> list[ImageAnalysis]:
    """Same result as processor.analyse_folder, but recorded in Prefect."""
    with tags(f"bot:{bot}", "anpr-bot"):
        return process_folder(str(folder), bot)
