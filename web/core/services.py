"""Reviewer actions: approve, correct or reject a folder's plate."""
import re
import shutil
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from pipeline.config import load_config
from pipeline.decision import normalize_plate

from .models import Folder, Route


class ReviewError(Exception):
    pass


def output_dir() -> Path:
    out = Path(load_config()["paths"]["output_dir"])
    return out if out.is_absolute() else settings.PROJECT_ROOT / out


def apply_review(folder: Folder, action: str, username: str, plate: str = "") -> str:
    """Updates the folder and moves its output copy. Returns a short confirmation message."""
    old_route = folder.route

    if action == "approve":
        if not folder.final_plate:
            raise ReviewError("There's no plate to approve. Type the correct plate instead.")
        folder.reviewed_plate = folder.final_plate
        folder.route = Route.ACCEPTED
        message = f"Approved {folder.final_plate} for {folder.name}."
    elif action == "correct":
        cleaned = normalize_plate(plate)
        if not cleaned:
            raise ReviewError("Type a plate number before saving.")
        pattern = load_config()["decision"]["plate_regex"]
        if not re.match(pattern, cleaned):
            raise ReviewError(f"{cleaned} doesn't match the plate format set in settings.yaml.")
        folder.reviewed_plate = cleaned
        folder.route = Route.ACCEPTED
        message = f"Saved {cleaned} for {folder.name}."
    elif action == "reject":
        folder.reviewed_plate = ""
        folder.route = Route.NO_PLATE
        message = f"Marked {folder.name} as having no readable plate."
    else:
        raise ReviewError("Unknown action.")

    folder.reviewed_by = username
    folder.reviewed_at = timezone.now()
    folder.save()
    _move_output(folder.name, old_route, folder.route)
    return message


def _move_output(name: str, old_route: str, new_route: str) -> None:
    if old_route == new_route:
        return
    src = output_dir() / old_route / name
    if not src.is_dir():
        return
    dst = output_dir() / new_route / name
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)
    shutil.rmtree(src)
