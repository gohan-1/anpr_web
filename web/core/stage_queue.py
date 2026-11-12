"""
The assembly-line queue, stored in PostgreSQL.

A "line" is simply every row with a given stage status. Bots only use the functions
below; when Redis or Kafka replaces PostgreSQL later, only THIS file changes.

    take()        a bot takes a batch from its line
    finish()      a finished item moves to its next line
    fail()        a failed item goes back to its line, or is given up after max attempts
    release()     a stopping bot hands back items it took but didn't start
    return_stuck() items held too long by a silent bot go back to their line
    record()      count finished/failed items per minute (the speed shown in the UI)
    line_stats()  how many items wait / are being worked on, per stage
    retry()       failed items go back to the line of the stage they failed in
    settings_of() a stage's settings: settings.yaml defaults + changes saved on the Pipeline page
"""
from datetime import timedelta

from django.db import IntegrityError, connection, transaction
from django.db.models import Count, F, Sum
from django.utils import timezone

from pipeline.stages import IMAGE_FINISHED, STAGES

from .models import IntakeFolder, IntakeImage, StageMinute, StageSettings

# Settings the Pipeline page may change, with their allowed range. Everything else in
# settings.yaml "stages:" can only be changed in the file.
EDITABLE_SETTINGS = {
    "batch_size": (1, 1000),
    "parallel": (1, 64),
    "max_attempts": (1, 20),
    "stuck_after_minutes": (1, 24 * 60),
    "pause_if_next_line_over": (0, 10_000_000),
    "scan_every_seconds": (5, 24 * 3600),
    "trip_if_confidence_at_least": (0.0, 1.0),     # a decimal: the only setting that isn't a whole number
}


def settings_of(stage: str) -> dict:
    """settings.yaml defaults for this stage, with any value saved on the Pipeline page on top.
    Bots call this before every batch, so a change applies without a restart."""
    from pipeline.config import load_config
    values = dict(load_config().get("stages", {}).get(stage, {}))
    saved = StageSettings.objects.filter(stage=stage).values_list("values", flat=True).first()
    values.update(saved or {})
    return values


def save_settings(stage: str, changes: dict, user: str = "") -> dict:
    """Validates and stores changes (None = back to the settings.yaml default). Returns the new settings."""
    errors = {}
    for key, value in changes.items():
        if key not in EDITABLE_SETTINGS:
            errors[key] = "This setting can't be changed here."
            continue
        if value is None:
            continue
        low, high = EDITABLE_SETTINGS[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high:
            errors[key] = f"Use a number from {low} to {high}."
    if errors:
        raise ValueError(errors)
    row, _ = StageSettings.objects.get_or_create(stage=stage)
    for key, value in changes.items():
        if value is None:
            row.values.pop(key, None)
        else:
            low, _ = EDITABLE_SETTINGS[key]
            row.values[key] = float(value) if isinstance(low, float) else int(value)
    row.updated_by = user
    row.save()
    return settings_of(stage)


def model_of(stage: str):
    return IntakeImage if STAGES[stage].level == "image" else IntakeFolder


# ---------------------------------------------------------------- taking work
def take(stage: str, bot: str, size: int) -> list:
    """Gives this bot up to `size` waiting items, lowest priority number (low accuracy) first.

    WHY IT'S SAFE WITH MANY BOTS: on PostgreSQL the rows are picked with
    FOR UPDATE SKIP LOCKED: rows another bot is taking at this very moment are locked,
    so this bot skips them and takes the next ones. No bot waits, none gets a duplicate.
    (SQLite has no SKIP LOCKED, but our SQLite settings run one writer at a time,
    which is also safe, just slower.)
    """
    s = STAGES[stage]
    Model = model_of(stage)
    with transaction.atomic():
        line = Model.objects.filter(stage=s.waits_in).order_by("priority", "id")
        if connection.features.has_select_for_update_skip_locked:
            line = line.select_for_update(skip_locked=True)
        ids = list(line.values_list("id", flat=True)[:size])
        if not ids:
            return []
        Model.objects.filter(id__in=ids, stage=s.waits_in).update(
            stage=s.working, taken_by=bot, taken_at=timezone.now(), attempts=F("attempts") + 1)
    return list(Model.objects.filter(id__in=ids, stage=s.working, taken_by=bot).order_by("priority", "id"))


# ---------------------------------------------------------------- results
def finish(stage: str, item, bot: str, to: str, **fields) -> bool:
    """Moves a finished item to the line `to`. Returns False if the bot no longer held it
    (it was returned as stuck and maybe taken by another bot), so nothing is overwritten."""
    s = STAGES[stage]
    extra = {"finished_at": timezone.now()} if s.level == "folder" and to in ("done", "manual") else {}
    # attempts restarts at 0: each stage gives an item its own max_attempts tries
    return bool(model_of(stage).objects.filter(id=item.id, stage=s.working, taken_by=bot).update(
        stage=to, taken_by="", taken_at=None, last_error="", attempts=0, **extra, **fields))


def fail(stage: str, item, bot: str, error: str, max_attempts: int) -> str:
    """Back to the line for another try, or 'failed' once it has had max_attempts."""
    s = STAGES[stage]
    Model = model_of(stage)
    attempts = Model.objects.filter(id=item.id).values_list("attempts", flat=True).first() or 0
    to = "failed" if attempts >= max_attempts else s.waits_in
    Model.objects.filter(id=item.id, stage=s.working, taken_by=bot).update(
        stage=to, taken_by="", taken_at=None, last_error=error[:2000], failed_in=stage if to == "failed" else "")
    return to


def release(stage: str, items, bot: str) -> int:
    """A stopping bot gives back items it took but never processed (the try isn't counted)."""
    s = STAGES[stage]
    return model_of(stage).objects.filter(id__in=[i.id for i in items], stage=s.working, taken_by=bot).update(
        stage=s.waits_in, taken_by="", taken_at=None, attempts=F("attempts") - 1)


def return_stuck(stage: str, minutes: float, max_attempts: int) -> int:
    """Items held longer than `minutes` (their bot died or hangs) go back to the line,
    or are failed if they've already used all their attempts."""
    s = STAGES[stage]
    Model = model_of(stage)
    stuck = Model.objects.filter(stage=s.working, taken_at__lt=timezone.now() - timedelta(minutes=minutes))
    failed = stuck.filter(attempts__gte=max_attempts).update(
        stage="failed", taken_by="", taken_at=None, last_error="Its bot stopped responding", failed_in=stage)
    back = stuck.filter(attempts__lt=max_attempts).update(stage=s.waits_in, taken_by="", taken_at=None)
    return failed + back


# ---------------------------------------------------------------- folders
def folders_ready(folder_ids) -> list[IntakeFolder]:
    """Folders still in 'reading' whose images are ALL finished (read, skipped or failed)."""
    busy = (IntakeImage.objects.filter(folder_id__in=folder_ids).exclude(stage__in=IMAGE_FINISHED)
            .values_list("folder_id", flat=True).distinct())
    return list(IntakeFolder.objects.filter(id__in=folder_ids, stage="reading").exclude(id__in=busy))


def route_folder(folder: IntakeFolder, to: str, reason: str) -> bool:
    """Moves a folder out of 'reading' exactly once. If two OCR bots finish the last two
    images of the same folder at the same moment, only one of them wins this update."""
    extra = {"finished_at": timezone.now()} if to in ("done", "manual", "failed") else {}
    return bool(IntakeFolder.objects.filter(id=folder.id, stage="reading").update(
        stage=to, route_reason=reason[:255], **extra))


# ---------------------------------------------------------------- monitoring
def record(stage: str, done: int, failed: int) -> None:
    """Adds this batch's counts to the current minute (for the speed in the UI)."""
    if not done and not failed:
        return
    minute = timezone.now().replace(second=0, microsecond=0)
    row = StageMinute.objects.filter(stage=stage, minute=minute)
    if row.update(done=F("done") + done, failed=F("failed") + failed):
        return
    try:
        StageMinute.objects.create(stage=stage, minute=minute, done=done, failed=failed)
    except IntegrityError:            # another bot created this minute's row a moment earlier
        row.update(done=F("done") + done, failed=F("failed") + failed)


def per_minute(stage: str, window: int = 5) -> float:
    since = timezone.now().replace(second=0, microsecond=0) - timedelta(minutes=window - 1)
    rows = StageMinute.objects.filter(stage=stage, minute__gte=since)
    total = rows.aggregate(n=Sum("done"))["n"] or 0
    first = rows.order_by("minute").values_list("minute", flat=True).first()
    if not total or first is None:
        return 0.0
    minutes = max((timezone.now() - first).total_seconds() / 60, 0.5)
    return round(total / minutes, 1)


def line_stats() -> dict:
    """Per stage: waiting, working, failed, and finished per minute. Plus folder totals."""
    images = dict(IntakeImage.objects.values_list("stage").annotate(n=Count("id")))
    folders = dict(IntakeFolder.objects.values_list("stage").annotate(n=Count("id")))
    out = {}
    for name, s in STAGES.items():
        counts = images if s.level == "image" else folders
        out[name] = {"waiting": counts.get(s.waits_in, 0), "working": counts.get(s.working, 0),
                     "per_minute": per_minute(name)}
    out["reader"] = {"waiting": 0, "working": 0, "per_minute": per_minute("reader")}
    return {"stages": out, "images": images, "folders": folders}


# ---------------------------------------------------------------- maintenance (Pipeline page)
REOPENABLE = ("needs_llm", "needs_trip", "manual", "failed")   # folder lines a retried image may reopen


def retry(level: str, ids=None) -> dict:
    """Sends failed items back to the line of the stage they failed in, with fresh attempts.

    level "image": failed images go back to preprocessing or OCR. Their folder is re-opened
    ("reading") so it is decided again once they're through, unless its trip is done or
    being made right now (those images are left alone).
    level "folder": failed folders go back to the LLM or trip line.
    ids: only these items; None = every failed item of that level.
    """
    lines = {name: s.waits_in for name, s in STAGES.items()}
    if level == "image":
        failed = IntakeImage.objects.filter(stage="failed").exclude(
            folder__stage__in=("done", "trip_running", "llm_running"))
    elif level == "folder":
        failed = IntakeFolder.objects.filter(stage="failed")
    else:
        raise ValueError("level must be 'image' or 'folder'")
    if ids is not None:
        failed = failed.filter(id__in=list(ids))
    moved = reopened = 0
    with transaction.atomic():
        folder_ids = set(failed.values_list("folder_id", flat=True)) if level == "image" else set()
        for stage, waits_in in lines.items():
            if STAGES[stage].level != level:
                continue
            moved += failed.filter(failed_in=stage).update(
                stage=waits_in, attempts=0, failed_in="", last_error="", taken_by="", taken_at=None)
        if level == "image":
            # images failed before failed_in existed: start them from the beginning
            moved += failed.filter(failed_in="").update(stage="new", attempts=0, last_error="")
            reopened = IntakeFolder.objects.filter(id__in=folder_ids, stage__in=REOPENABLE).update(
                stage="reading", route_reason="", finished_at=None)
    return {"moved": moved, "folders_reopened": reopened}


def failed_items(level: str, limit: int = 100) -> list[dict]:
    """The newest failed items with their error, for the Pipeline page."""
    if level == "image":
        rows = (IntakeImage.objects.filter(stage="failed").select_related("folder").order_by("-id")[:limit])
        return [{"id": i.id, "name": i.file_name, "folder": i.folder.name, "failed_in": i.failed_in,
                 "error": i.last_error, "attempts": i.attempts} for i in rows]
    rows = IntakeFolder.objects.filter(stage="failed").order_by("-id")[:limit]
    return [{"id": f.id, "name": f.name, "folder": f.name, "failed_in": f.failed_in,
             "error": f.last_error, "attempts": f.attempts} for f in rows]
