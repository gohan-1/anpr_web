"""
What each stage actually DOES to one item.

A handler receives a small dict describing one item (never a database object, so it's
safe to run in threads or as a Prefect task) and returns an Outcome. The bot then saves
the outcome to the database. Handlers never touch the database themselves.

OCR is REAL: it sends the image to the OCR API (settings.yaml "api:"; mock: true gives
fake answers) with the bot's own account and rate limit. The other stages are still
PLACEHOLDERS that wait a little and pass the item on, until their services exist:
    preprocess -> preprocessing service    llm -> LLM service    trip -> trip service

`ctx` is the bot's private context (API account, rate limiter, settings). It is never
sent to Prefect or stored anywhere: it only lives in the bot's memory.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Outcome:
    id: int
    ok: bool
    to: str | None = None          # the line it goes to next (None = the stage's usual next line)
    fields: dict = field(default_factory=dict)   # extra columns to save, e.g. cleaned_path
    error: str = ""


@dataclass
class BotContext:
    """What a bot's handlers need besides the item: kept in memory only."""
    name: str
    api_cfg: dict = field(default_factory=dict)
    credentials: tuple | None = None        # (username, password) for the stage's outside service
    limiter: object = None                  # RateLimiter shared by the bot's threads


CONTEXTS: dict[str, BotContext] = {}        # bot name -> context, for Prefect tasks in this process


def payload_of(level: str, item) -> dict:
    """The plain data a handler needs about one item."""
    if level == "image":
        return {"id": item.id, "path": item.path, "cleaned_path": item.cleaned_path,
                "folder_id": item.folder_id, "file_name": item.file_name}
    return {"id": item.id, "name": item.name, "path": item.path}


def _pretend(cfg: dict) -> None:
    time.sleep(cfg.get("placeholder_seconds", 0))
    if random.random() < cfg.get("placeholder_fail_rate", 0):      # only used in tests
        raise RuntimeError("Simulated failure")


# ---------------------------------------------------------------- placeholders
def preprocess(item: dict, cfg: dict, ctx: BotContext | None = None) -> Outcome:
    _pretend(cfg)
    # step 3: send the image to the preprocessing service, store the cleaned copy's path
    return Outcome(item["id"], True, "cleaned", {"cleaned_path": item["path"]})


def ocr(item: dict, cfg: dict, ctx: BotContext | None = None) -> Outcome:
    """Sends the (cleaned) image to the OCR API, timed, with retries and the bot's rate limit.
    An API that still fails after its retries is a FAILED try: the item goes back to the OCR
    line, and after max_attempts it is marked failed (retry it from the Pipeline page)."""
    from pipeline.processor import ocr_image
    ctx = ctx or BotContext("test")
    if random.random() < cfg.get("placeholder_fail_rate", 0):     # only used in tests
        raise RuntimeError("Simulated failure")
    result = ocr_image(Path(item["cleaned_path"] or item["path"]), ctx.api_cfg, ctx.credentials, ctx.limiter)
    if result.error:
        return Outcome(item["id"], False, error=result.error)
    return Outcome(item["id"], True, "read", {
        "plate": result.plate or "", "confidence": result.confidence, "api_response": result.raw,
        "read_by": ctx.name, "duration_ms": round(result.seconds * 1000) if result.seconds is not None else None,
        "api_attempts": result.attempts or None})


def llm(item: dict, cfg: dict, ctx: BotContext | None = None) -> Outcome:
    _pretend(cfg)
    # step 5: ask the LLM service; for now 4 in 5 folders become confident, 1 goes to a person
    confident = item["id"] % 5 != 0
    return Outcome(item["id"], True, "needs_trip" if confident else "manual")


def trip(item: dict, cfg: dict, ctx: BotContext | None = None) -> Outcome:
    _pretend(cfg)
    # step 6: create the trip through the trip service
    return Outcome(item["id"], True, "done")


HANDLERS = {"preprocess": preprocess, "ocr": ocr, "llm": llm, "trip": trip}
