"""
Decision engine: pure logic, no files, no API, no database.

Input:  every image result for ONE folder.
Output: one Decision (route + scenario code + reason).

Scenario codes
  S1  single clear image, high confidence            -> accepted
  S2  blurry image(s) present, a clear one decided   -> accepted
  S3  several images agree on the same plate         -> accepted
  S4  different plates with high confidence          -> review
  S5  high confidence but only low-quality images    -> review
  S6  no plate detected in any image                 -> no_plate
  S7  plate found but score too low                  -> review
  S8  plate text fails format check                  -> review
  S9  API errors, nothing usable                     -> failed
  S10 duplicate images (recorded, not re-processed)  -> per-image flag
  S11 corrupt / unreadable images only               -> failed
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum


class Route(str, Enum):
    ACCEPTED = "accepted"
    REVIEW = "review"
    NO_PLATE = "no_plate"
    FAILED = "failed"


@dataclass
class ImageResult:
    image_id: str
    quality: float | None = None      # 0..1, None if not computed
    plate: str | None = None
    confidence: float | None = None   # 0..1 from the API
    error: str | None = None          # API error message, if any
    corrupt: bool = False
    duplicate: bool = False


@dataclass
class Thresholds:
    weight_confidence: float = 0.7
    weight_quality: float = 0.3
    accept_score: float = 0.85
    review_floor: float = 0.60
    high_confidence: float = 0.90
    conflict_confidence: float = 0.80
    min_quality: float = 0.40
    consensus_boost: float = 0.03
    max_consensus_boost: float = 0.09
    plate_regex: str = r"^[A-Z0-9]{4,10}$"

    @classmethod
    def from_config(cls, cfg: dict) -> "Thresholds":
        known = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in cfg.items() if k in known})


@dataclass
class Decision:
    route: Route
    scenario: str
    reason: str
    plate: str | None = None
    score: float | None = None
    best_image_id: str | None = None
    candidates: dict = field(default_factory=dict)  # plate -> {best_score, count}


def normalize_plate(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = re.sub(r"[^A-Z0-9]", "", text.upper())
    return cleaned or None


def image_score(r: ImageResult, t: Thresholds) -> float:
    quality = r.quality if r.quality is not None else 0.5  # unknown = neutral
    return round(t.weight_confidence * (r.confidence or 0.0) + t.weight_quality * quality, 4)


def decide(results: list[ImageResult], t: Thresholds | None = None) -> Decision:
    t = t or Thresholds()

    usable = [r for r in results if not r.corrupt and not r.duplicate]
    if not usable:
        return Decision(Route.FAILED, "S11", "No readable images in folder (all corrupt or empty)")

    answered = [r for r in usable if r.error is None]
    errored = [r for r in usable if r.error is not None]
    if not answered:
        return Decision(Route.FAILED, "S9", f"API failed for all {len(errored)} image(s)")

    detected = []
    for r in answered:
        plate = normalize_plate(r.plate)
        if plate and r.confidence:
            detected.append((plate, r))

    if not detected:
        if errored:
            return Decision(Route.FAILED, "S9",
                            f"No plate in {len(answered)} image(s), API failed on {len(errored)}; retry")
        return Decision(Route.NO_PLATE, "S6", f"No plate detected in any of {len(answered)} image(s)")

    # Group by plate text
    groups: dict[str, list[ImageResult]] = defaultdict(list)
    for plate, r in detected:
        groups[plate].append(r)

    candidates = {}
    for plate, rs in groups.items():
        best = max(rs, key=lambda r: image_score(r, t))
        boost = min(t.consensus_boost * (len(rs) - 1), t.max_consensus_boost)
        candidates[plate] = {
            "best_score": image_score(best, t),
            "final_score": round(min(1.0, image_score(best, t) + boost), 4),
            "count": len(rs),
            "best_image_id": best.image_id,
            "max_confidence": max(r.confidence for r in rs),
        }

    # S4: conflict - several different plates read with high confidence on decent images
    strong = [p for p, rs in groups.items()
              if any(r.confidence >= t.conflict_confidence and (r.quality or 0) >= t.min_quality for r in rs)]
    if len(strong) > 1:
        return Decision(Route.REVIEW, "S4", f"Conflicting plates with high confidence: {', '.join(sorted(strong))}",
                        candidates=candidates)

    winner = max(candidates, key=lambda p: candidates[p]["final_score"])
    info = candidates[winner]
    base = dict(plate=winner, score=info["final_score"], best_image_id=info["best_image_id"], candidates=candidates)

    # S8: format check
    if not re.match(t.plate_regex, winner):
        return Decision(Route.REVIEW, "S8", f"Plate '{winner}' does not match expected format", **base)

    # S5: high confidence but every supporting image is low quality
    supporting = groups[winner]
    if info["max_confidence"] >= t.high_confidence and all((r.quality or 0) < t.min_quality for r in supporting):
        return Decision(Route.REVIEW, "S5",
                        f"High API confidence ({info['max_confidence']:.2f}) but all images are low quality", **base)

    # S7: score too low to trust automatically
    if info["final_score"] < t.accept_score:
        level = "low" if info["final_score"] < t.review_floor else "medium"
        return Decision(Route.REVIEW, "S7", f"Score {info['final_score']:.3f} is {level} (auto-accept needs {t.accept_score:.2f}); needs human check", **base)

    # Accepted: work out which positive scenario applied
    blurry = [r for r in usable if r.quality is not None and r.quality < t.min_quality]
    note = f" (API failed on {len(errored)} image(s))" if errored else ""
    if info["count"] > 1:
        return Decision(Route.ACCEPTED, "S3", f"{info['count']} images agree on {winner}{note}", **base)
    if blurry:
        return Decision(Route.ACCEPTED, "S2",
                        f"{len(blurry)} low-quality image(s) ignored; clear image decided{note}", **base)
    return Decision(Route.ACCEPTED, "S1", f"Clear image, score {info['final_score']:.2f}{note}", **base)
