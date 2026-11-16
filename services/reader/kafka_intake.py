"""
Turns ONE Kafka folder message into a registered folder. No Kafka code in here: the
bot (bots/kafka_reader.py) receives the message and calls handle_message(); this file
only checks the message, reads the images from disk and registers them. That keeps it
easy to test without a Kafka server.

The message (one per vehicle folder):
    {
      "folder_id":   "car_001",                    required: the vehicle's id (also the Kafka key)
      "folder":      "low_accuracy/car_001",       required: RELATIVE to reader.storage_root
      "accuracy":    "low" | "high",               required: decides the priority
      "images":      ["cam_1.jpg", "cam_2.jpg"],   optional: without it, every image in the folder
      "camera":      "gate-2",                     optional
      "captured_at": "2026-11-16T09:15:02Z"        optional
    }

WHY a relative "folder": on the laptop the images live at <project>/data/..., in Docker at
/app/data/... The message stays the same; each environment adds its own storage_root.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from pathlib import Path

from .scanner import FolderInfo, ImageInfo, Source, _fingerprint, register


class BadMessage(Exception):
    """A message that can never succeed (wrong format, missing images...). It goes to
    the failed topic and is committed, so it can't block the partition."""


@dataclass
class IntakeResult:
    folder_id: str
    registered: bool          # False = it was already registered (a re-delivered message)
    images: int


def _required(data: dict, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise BadMessage(f'"{field}" is missing or empty')
    return value.strip()


def parse(data, root: Path) -> tuple[str, Path, str, list[str] | None]:
    """Checks the message and returns (folder_id, folder path, accuracy, image names or None)."""
    if not isinstance(data, dict):
        raise BadMessage("The message is not a JSON object")
    folder_id = _required(data, "folder_id")
    relative = _required(data, "folder")
    accuracy = _required(data, "accuracy").lower()
    if accuracy not in ("low", "high"):
        raise BadMessage(f'"accuracy" must be "low" or "high", not "{accuracy}"')
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):           # e.g. "../../etc": refuse
        raise BadMessage(f'"folder" points outside the image storage: {relative}')
    images = data.get("images")
    if images is not None:
        if not isinstance(images, list) or not all(isinstance(i, str) and i for i in images):
            raise BadMessage('"images" must be a list of file names')
        if any(Path(i).name != i for i in images):          # names only, no paths
            raise BadMessage('"images" must contain file names only, not paths')
    return folder_id, path, accuracy, images


def wait_for_files(path: Path, images: list[str] | None, extensions: set[str], seconds: float) -> list[Path]:
    """The message can arrive before the camera finished copying. Waits until the folder
    (and every listed image) is there, up to `seconds`. Returns the image paths."""
    deadline = time.monotonic() + seconds
    while True:
        if path.is_dir():
            if images is not None:
                wanted = [path / name for name in images]
                if all(p.is_file() for p in wanted):
                    return wanted
            else:
                found = sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() in extensions)
                if found:
                    return found
        if time.monotonic() >= deadline:
            missing = "the folder" if not path.is_dir() else "its images"
            raise BadMessage(f"After {seconds:.0f} s, {missing} still not found at {path}")
        time.sleep(1)


def read_images(path: Path, files: list[Path]) -> FolderInfo:
    info = FolderInfo(path=str(path), name=path.name,
                      modified_at=datetime.fromtimestamp(path.stat().st_mtime, dt_timezone.utc))
    seen = set()
    for f in files:
        stat = f.stat()
        digest = _fingerprint(str(f))
        info.images.append(ImageInfo(file_name=f.name, path=str(f), size_bytes=stat.st_size, sha256=digest,
                                     modified_at=datetime.fromtimestamp(stat.st_mtime, dt_timezone.utc),
                                     is_duplicate=digest in seen))
        seen.add(digest)
    return info


def handle_message(data, *, root: Path, extensions: set[str], priorities: dict[str, int],
                   instance: str, first_stage: str, wait_seconds: float) -> IntakeResult:
    """Checks, reads and registers one folder. Raises BadMessage for messages that can
    never succeed; any other exception (e.g. database down) means "try again later"."""
    folder_id, path, accuracy, images = parse(data, root)
    files = wait_for_files(path, images, extensions, wait_seconds)
    info = read_images(path, files)
    source = Source(name="kafka", path=root, accuracy=accuracy, priority=priorities[accuracy])
    registered = register(info, source, instance, first_stage)    # False = already registered
    return IntakeResult(folder_id, registered, len(info.images))


def priorities_from(reader_cfg: dict) -> dict[str, int]:
    """Priority per accuracy, taken from reader.sources (if several sources share an
    accuracy, the lowest number wins). Defaults: low = 1, high = 2."""
    found: dict[str, int] = {}
    for s in reader_cfg.get("sources", []):
        accuracy, priority = s.get("accuracy"), int(s.get("priority", 99))
        if accuracy in ("low", "high"):
            found[accuracy] = min(found.get(accuracy, priority), priority)
    return {"low": found.get("low", 1), "high": found.get("high", 2)}