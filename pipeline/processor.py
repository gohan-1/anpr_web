"""
Sends ONE image to the plate (OCR) API: timed, with retries and the bot's rate limit.
Used by the OCR stage (bots/handlers.py). No database here, so it runs in any thread.
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
    seconds: float | None = None   # wall time for this image: rate-limit wait + read + OCR call + retries
    attempts: int = 0              # OCR calls made (1 = answered first time)


def list_images(folder: Path, extensions: list[str]) -> list[Path]:
    exts = {e.lower() for e in extensions}
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in exts)


def call_api_with_retries(path: Path, api_cfg: dict, credentials=None, limiter=None,
                          item: ImageAnalysis | None = None) -> dict:
    """Retries network/server errors a few times, waiting longer each time.
    If `item` is given, item.attempts counts the calls made."""
    attempts = api_cfg["retries"] + 1
    for attempt in range(1, attempts + 1):
        if item is not None:
            item.attempts = attempt
        if limiter:
            limiter.wait()
        try:
            return api_client.recognize(path, api_cfg, credentials)
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(api_cfg["retry_delay_seconds"] * attempt)


def ocr_image(path: Path, api_cfg: dict, credentials=None, limiter=None) -> ImageAnalysis:
    """One image to the OCR API, timed. Never raises: an API failure is recorded in .error."""
    item = ImageAnalysis(path)
    started = time.perf_counter()
    try:
        api = call_api_with_retries(path, api_cfg, credentials, limiter, item)
        item.plate, item.confidence, item.raw = api["plate"], api["confidence"], api["raw"]
    except Exception as exc:                                 # API failed after retries
        item.error = f"{type(exc).__name__}: {exc}"
    item.seconds = time.perf_counter() - started
    return item
