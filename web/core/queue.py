"""
The work queue, stored in the database (one WorkItem row per folder).

Bots only use the functions in this file: enqueue / claim / complete / fail.
When you move to Kafka later, you replace THIS file's insides and the bots
don't change. That's the point of keeping the queue behind a small interface.
"""
import os
import time
from datetime import timedelta
from pathlib import Path

from django.db import connection, transaction
from django.db.models import Count, F, Min, Q
from django.db.models.functions import TruncMinute
from django.utils import timezone

from .models import Bot, WorkItem

Status = WorkItem.Status


# ---------------------------------------------------------------- adding work
def resolve_dir(path: str) -> Path:
    """Relative folders are relative to the project root (main/), like output_dir."""
    from django.conf import settings
    p = Path(path).expanduser()
    return (p if p.is_absolute() else Path(settings.PROJECT_ROOT) / p).resolve()


def _has_images(folder: Path) -> bool:
    from pipeline.config import load_config
    exts = {e.lower() for e in load_config()["paths"]["image_extensions"]}
    with os.scandir(folder) as entries:
        return any(e.is_file() and Path(e.name).suffix.lower() in exts for e in entries)


def scan_all(default_dir: Path | None, min_age_seconds: int) -> dict[str, int]:
    """Queues new folders: each bot's own input folder for that bot, the default folder for the rest.
    Also keeps WAITING folders in line with each bot's current input folder: a folder that was
    already queued (shared, or for another bot) moves to the bot that now owns it, and a bot that
    changed folder gives back the waiting folders it no longer owns. Folders in progress or done
    are never touched. Returns {folder watched: how many new or moved}."""
    added = {}
    bot_dirs = {}
    for bot in Bot.objects.exclude(input_dir="").filter(delete_requested=False):
        d = resolve_dir(bot.input_dir)
        if d.is_dir():
            bot_dirs[str(d)] = bot

    owned: dict[int, set[str]] = {}          # bot id -> waiting folder paths it owns right now
    for d, bot in bot_dirs.items():
        if _has_images(Path(d)):
            # The bot was given ONE vehicle folder (images directly inside): process that folder itself.
            _, new = WorkItem.objects.get_or_create(path=d, defaults={"assigned_bot": bot})
            n = int(new)
            mine = {d}
        else:
            n = enqueue_folders(Path(d), min_age_seconds, assigned_bot=bot, skip=set(bot_dirs))
            prefix = d.rstrip(os.sep) + os.sep
            mine = {p for p in WorkItem.objects.filter(status=Status.PENDING, path__startswith=prefix)
                    .values_list("path", flat=True) if str(Path(p).parent) == d}
        moved = (WorkItem.objects.filter(status=Status.PENDING, path__in=mine)
                 .exclude(assigned_bot=bot).update(assigned_bot=bot))
        owned[bot.id] = mine
        added[f"{d} ({bot.name})"] = n + moved

    # Give back waiting folders a bot no longer owns (its input folder changed or was cleared).
    for item_bot_id in (WorkItem.objects.filter(status=Status.PENDING, assigned_bot__isnull=False)
                        .values_list("assigned_bot_id", flat=True).distinct()):
        (WorkItem.objects.filter(status=Status.PENDING, assigned_bot_id=item_bot_id)
         .exclude(path__in=owned.get(item_bot_id, set())).update(assigned_bot=None))

    if default_dir and default_dir.is_dir():
        added[str(default_dir)] = enqueue_folders(default_dir, min_age_seconds, skip=set(bot_dirs))
    return added


def enqueue_folders(input_dir: Path, min_age_seconds: int = 0, batch_size: int = 5000,
                    assigned_bot: Bot | None = None, skip: set[str] | None = None) -> int:
    """Adds every sub-folder of input_dir that isn't queued yet. Returns how many were new.

    assigned_bot: only that bot may process these folders (its own input folder).
    skip: paths that are NOT vehicle folders (e.g. other bots' input folders inside this one).

    Uses os.scandir (fast, even with millions of entries) and inserts in batches.
    ignore_conflicts=True skips folders already in the queue, thanks to the unique path.
    """
    now, added, batch = time.time(), 0, []
    before = WorkItem.objects.count()
    with os.scandir(input_dir) as entries:
        for entry in entries:
            if not entry.is_dir() or now - entry.stat().st_mtime < min_age_seconds:
                continue                     # not a folder, or still being written by the camera
            path = str(Path(entry.path).resolve())
            if skip and path in skip:
                continue
            batch.append(WorkItem(path=path, assigned_bot=assigned_bot))
            if len(batch) >= batch_size:
                WorkItem.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []
    if batch:
        WorkItem.objects.bulk_create(batch, ignore_conflicts=True)
    added = WorkItem.objects.count() - before
    return added


# ---------------------------------------------------------------- taking work
def claim(bot: Bot, limit: int) -> list[WorkItem]:
    """Gives this bot up to `limit` pending folders. Two bots can never get the same one.

    HOW: we pick candidate rows, then for EACH row run
        UPDATE ... SET status='in_progress' WHERE id=<row> AND status='pending'
    The database runs that as one atomic step. If another bot claimed the row a
    moment earlier, the WHERE no longer matches, 0 rows change, and we skip it.
    On PostgreSQL, SKIP LOCKED also lets many bots pick different rows without waiting.
    """
    if limit <= 0:
        return []
    claimed_ids = []
    with transaction.atomic():
        candidates = WorkItem.objects.filter(status=Status.PENDING).order_by("id")
        # A bot with its own input folder only takes folders assigned to it;
        # a bot without one takes the shared (unassigned) folders.
        candidates = candidates.filter(assigned_bot=bot) if bot.input_dir else candidates.filter(assigned_bot__isnull=True)
        if connection.features.has_select_for_update_skip_locked:
            candidates = candidates.select_for_update(skip_locked=True)
        for item_id in candidates.values_list("id", flat=True)[:limit]:
            won = (WorkItem.objects.filter(id=item_id, status=Status.PENDING).update(status=Status.IN_PROGRESS, bot=bot, bot_name=bot.name,claimed_at=timezone.now(), attempts=F("attempts") + 1))
            if won:
                claimed_ids.append(item_id)
    return list(WorkItem.objects.filter(id__in=claimed_ids))


def complete(item: WorkItem, folder) -> None:
    WorkItem.objects.filter(id=item.id).update(status=Status.DONE, folder=folder, finished_at=timezone.now(),last_error="")


def fail(item: WorkItem, error: str, max_attempts: int) -> str:
    """Puts the folder back in the queue, or marks it failed after too many attempts."""
    item.refresh_from_db(fields=["attempts"])
    status = Status.FAILED if item.attempts >= max_attempts else Status.PENDING
    WorkItem.objects.filter(id=item.id).update(
        status=status, last_error=error[:2000], bot=None,
        finished_at=timezone.now() if status == Status.FAILED else None)
    return status


def release(items) -> int:
    """A stopping bot hands back folders it claimed but never started."""
    ids = [i.id for i in items]
    return WorkItem.objects.filter(id__in=ids, status=Status.IN_PROGRESS).update(
        status=Status.PENDING, bot=None, attempts=F("attempts") - 1)


def requeue_stale(claim_timeout_seconds: int, max_attempts: int) -> int:
    """Folders stuck 'in progress' (their bot died) go back to the queue."""
    cutoff = timezone.now() - timedelta(seconds=claim_timeout_seconds)
    dead_bots = Q(bot__isnull=True) | ~Q(bot__state__in=[Bot.State.RUNNING, Bot.State.STOPPING])
    stuck = WorkItem.objects.filter(status=Status.IN_PROGRESS).filter(Q(claimed_at__lt=cutoff) | dead_bots)
    to_fail = stuck.filter(attempts__gte=max_attempts).update(
        status=Status.FAILED, bot=None, finished_at=timezone.now(),
        last_error="Bot stopped responding while processing this folder")
    to_retry = stuck.filter(attempts__lt=max_attempts).update(status=Status.PENDING, bot=None)
    return to_fail + to_retry


def retry_failed() -> int:
    return WorkItem.objects.filter(status=Status.FAILED).update(status=Status.PENDING, attempts=0,last_error="", finished_at=None)


# ---------------------------------------------------------------- monitoring
def folders_per_minute(items, window_minutes: int = 5) -> float:
    """Average speed over the last few minutes.

    We divide by the time actually covered: if the first folder finished 1 minute ago,
    dividing by 5 would make the bots look 5x slower than they are.
    """
    now = timezone.now()
    recent = items.filter(status=Status.DONE, finished_at__gte=now - timedelta(minutes=window_minutes))
    agg = recent.aggregate(n=Count("id"), first=Min("finished_at"))
    if not agg["n"]:
        return 0.0
    minutes = max((now - agg["first"]).total_seconds() / 60, 0.5)   # at least 30 s, avoids huge spikes
    return round(agg["n"] / minutes, 1)


def queue_stats(minutes: int = 30) -> dict:
    counts = dict(WorkItem.objects.values_list("status").annotate(n=Count("id")))
    now = timezone.now()
    per_minute = folders_per_minute(WorkItem.objects.all())
    pending = counts.get(Status.PENDING, 0) + counts.get(Status.IN_PROGRESS, 0)

    # Folders finished per minute, per bot, for the throughput chart
    timeline = {}
    rows = (WorkItem.objects.filter(status=Status.DONE, finished_at__gte=now - timedelta(minutes=minutes))
            .annotate(minute=TruncMinute("finished_at")).values("minute", "bot_name").annotate(n=Count("id")))
    for r in rows:
        key = timezone.localtime(r["minute"]).strftime("%H:%M")
        timeline.setdefault(key, {"minute": key})[r["bot_name"] or "unknown"] = r["n"]

    return {
        "counts": {s.value: counts.get(s.value, 0) for s in Status},
        "per_minute": round(per_minute, 1),
        "eta_minutes": round(pending / per_minute) if per_minute else None,
        "timeline": [timeline[k] for k in sorted(timeline)],
        "recent_failures": list(WorkItem.objects.filter(status=Status.FAILED).order_by("-finished_at")
                                .values("id", "path", "last_error", "attempts", "bot_name")[:10]),
    }
