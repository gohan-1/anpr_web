"""
Plate-recognition API client.

ADAPT `parse_response` to your API's real JSON shape. Everything else in the
project only ever sees {"plate", "confidence", "raw"}.
"""
from __future__ import annotations

import hashlib
import random
from pathlib import Path

import httpx


def parse_response(data: dict) -> tuple[str | None, float | None]:
    """Turn the API's JSON into (plate, confidence 0..1). Edit this for your API.

    Example shape assumed: {"results": [{"plate": "MH12AB1234", "score": 0.93}, ...]}
    """
    results = data.get("results") or []
    if not results:
        return None, None
    best = max(results, key=lambda r: r.get("score", 0))
    conf = best.get("score")
    if conf is not None and conf > 1:      # some APIs return 0-100
        conf = conf / 100
    return best.get("plate"), conf


def recognize(path: Path, cfg: dict) -> dict:
    """Send one image. Raises on network/HTTP errors so Prefect can retry."""
    if cfg.get("mock"):
        return _mock_response(path)

    headers = {"Authorization": f"Token {cfg['api_key']}"} if cfg.get("api_key") else {}
    with open(path, "rb") as f:
        resp = httpx.post(cfg["url"], files={"upload": f}, headers=headers, timeout=cfg["timeout_seconds"])
    resp.raise_for_status()
    data = resp.json()
    plate, conf = parse_response(data)
    return {"plate": plate, "confidence": conf, "raw": data}


def _mock_response(path: Path) -> dict:
    """Deterministic fake result per file, so local runs are repeatable."""
    seed = int(hashlib.md5(path.name.encode()).hexdigest(), 16)
    rng = random.Random(seed)
    if rng.random() < 0.1:
        data = {"results": []}
    else:
        plate = f"MH12AB{rng.randint(1000, 1003)}"
        data = {"results": [{"plate": plate, "score": round(rng.uniform(0.5, 0.99), 3)}]}
    plate, conf = parse_response(data)
    return {"plate": plate, "confidence": conf, "raw": data}
