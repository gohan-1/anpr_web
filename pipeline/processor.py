"""
Analyses ONE folder: every image goes straight to the plate (OCR) API.
No local checks (no hashing, duplicates, readability or quality): the API decides.
No database here, so it can run in any thread or process (bots and Prefect both use it).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline import api_client


@dataclass
class ImageAnalysis:
    path: Path
    plate: str | None = None
    confidence: float | None = None
    raw: dict = field(default_factory=dict)
    error: str = ""


def list_images(folder: Path, extensions: list[str]) -> list[Path]:
    exts = {e.lower() for e in extensions}
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in exts)


def call_api_with_retries(path: Path, api_cfg: dict, credentials=None, limiter=None) -> dict:
    """Retries network/server errors a few times, waiting longer each time."""
    attempts = api_cfg["retries"] + 1
    for attempt in range(1, attempts + 1):
        if limiter:
            limiter.wait()
        try:
            return api_client.recognize(path, api_cfg, credentials)
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(api_cfg["retry_delay_seconds"] * attempt)


def analyse_folder(folder: Path, cfg: dict, credentials=None, limiter=None) -> list[ImageAnalysis]:
    """Every image of one folder, analysed. Raises only if the folder itself can't be read."""
    results = []
    for path in list_images(folder, cfg["paths"]["image_extensions"]):
        item = ImageAnalysis(path)
        try:
            api = call_api_with_retries(path, cfg["api"], credentials, limiter)
            item.plate, item.confidence, item.raw = api["plate"], api["confidence"], api["raw"]
        except Exception as exc:                             # API failed after retries
            item.error = f"{type(exc).__name__}: {exc}"
        results.append(item)
    return results
