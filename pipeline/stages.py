"""
The assembly line, described in one place. Every part (bots, queue, API, UI) reads it.

Each stage has a WAITING status (its line), a WORKING status (a bot holds the item)
and where items go when finished. Image stages handle single images; folder stages
handle whole folders. The reader is special: it fills the first line from disk.

    reader ─► [new] ─► preprocess ─► [cleaned] ─► ocr ─► [read]
                                                   │ when all of a folder's images are read:
                                                   │ consensus decides the folder's route
              ┌────────────────────────────────────┴───────────────┐
              ▼                       ▼                            ▼
         [needs_trip]            [needs_llm] ─► llm ─► needs_trip or manual
              │                                                  [manual] → Review page
              ▼
            trip ─► [done]
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Stage:
    name: str           # bot stage name, also the key in settings.yaml "stages:"
    level: str          # "image" or "folder": which table its line lives in
    waits_in: str       # status of items waiting for this stage (its line)
    working: str        # status while a bot of this stage holds the item
    goes_to: str | None  # usual status after this stage (a handler may choose another)
    label: str


STAGES: dict[str, Stage] = {
    "preprocess": Stage("preprocess", "image", "new", "preprocessing", "cleaned", "Preprocessing"),
    "ocr": Stage("ocr", "image", "cleaned", "ocr_running", "read", "OCR"),
    "llm": Stage("llm", "folder", "needs_llm", "llm_running", None, "LLM"),
    "trip": Stage("trip", "folder", "needs_trip", "trip_running", "done", "Trip"),
}

# All stages bots can be assigned to, in assembly-line order (reader first).
BOT_STAGES = ["reader", "preprocess", "ocr", "llm", "trip"]
STAGE_LABELS = {"reader": "Reading", **{k: v.label for k, v in STAGES.items()}}

# Image statuses that mean "this image is finished, one way or another".
IMAGE_FINISHED = ("read", "skipped", "failed")


def next_line_of(stage: str) -> tuple[str, str] | None:
    """(level, status) of the line this stage feeds, used for backpressure."""
    s = STAGES[stage]
    return (s.level, s.goes_to) if s.goes_to in {x.waits_in for x in STAGES.values()} else None
