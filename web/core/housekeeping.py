"""
Chores that keep the assembly line healthy. Nothing here starts or stops a bot process.

  return_stuck_items()     items held too long by a silent bot go back to their line
  skip_switched_off()      images waiting for a switched-off stage (preprocessing) move on
  route_forgotten_folders() folders whose images are all finished but nobody routed
  assign_reader_shards()   alive reader bots share the folders: 1/3, 2/3, 3/3
  remove_dead_bots()       rows of bots whose container is gone (Docker mode)

Who runs them: the bot supervisor (`make start`), or, when Docker runs the bots, the
`housekeeper` container (python web/manage.py housekeeper).
"""
from datetime import timedelta

from django.utils import timezone

from . import stage_queue
from .models import Bot, BotEvent, IntakeFolder
from pipeline.stages import STAGES


def return_stuck_items() -> dict[str, int]:
    moved = {}
    for name in STAGES:
        c = stage_queue.settings_of(name)
        n = stage_queue.return_stuck(name, c["stuck_after_minutes"], c["max_attempts"])
        if n:
            moved[name] = n
    return moved


def route_forgotten_folders(limit: int = 500) -> int:
    """Normally the OCR bot that reads a folder's last image routes it. A folder can be
    missed when its last image FAILED somewhere else (in preprocessing, or returned as stuck):
    this routes those, so no folder waits in 'reading' forever."""
    from .ocr_results import finish_folder
    ids = list(IntakeFolder.objects.filter(stage="reading").values_list("id", flat=True)[:limit])
    cfg = stage_queue.settings_of("ocr")
    return sum(finish_folder(f, cfg) for f in stage_queue.folders_ready(ids))


def _alive(stale_seconds: int):
    return Bot.objects.filter(last_heartbeat__gte=timezone.now() - timedelta(seconds=stale_seconds))


def skip_switched_off() -> int:
    return stage_queue.skip_switched_off()


def assign_reader_shards(stale_seconds: int) -> int:
    """Docker mode: every alive reader gets its share, in name order. A reader reads its
    shard from the database before each scan, so no restart is needed. Returns changes.
    Kafka mode: nothing to do, Kafka shares the topic's partitions between the readers."""
    from pipeline.config import reader_mode
    if reader_mode() == "kafka":
        return 0
    readers = list(_alive(stale_seconds).filter(stage=Bot.Stage.READER).order_by("name"))
    changed = 0
    for i, bot in enumerate(readers, 1):
        shard = f"{i}/{len(readers)}"
        if bot.shard != shard:
            Bot.objects.filter(pk=bot.pk).update(shard=shard)
            BotEvent.objects.create(bot=bot, message=f"Reader count changed: now shard {shard}")
            changed += 1
    return changed


def remove_dead_bots(after_minutes: float, stopped_after_seconds: float = 60) -> list[str]:
    """Docker mode: a scaled-down or replaced container leaves its row behind. Removed:
      * a container that STOPPED CLEANLY (state stopped while still switched on: a paused bot is
        switched off and keeps its heartbeat) after `stopped_after_seconds`
      * a container that died without saying so (killed, machine off) after `after_minutes`
    Items they still held go straight back to their line. Their results stay (results don't
    point at bots)."""
    now = timezone.now()
    cutoff = now - timedelta(minutes=after_minutes)
    dead = (Bot.objects.exclude(last_heartbeat__gte=cutoff).exclude(last_heartbeat=None)
            | Bot.objects.filter(last_heartbeat=None, created_at__lt=cutoff)
            | Bot.objects.filter(state=Bot.State.STOPPED, desired_running=True,
                                 last_heartbeat__lt=now - timedelta(seconds=stopped_after_seconds)))
    rows = list(dead.values_list("name", "stage"))
    for name, stage in rows:              # what it held goes back to the line now, not after
        if stage in STAGES:               # stuck_after_minutes (a recreated container has a new name)
            stage_queue.reclaim(stage, name, stage_queue.settings_of(stage)["max_attempts"])
    dead.delete()
    return [name for name, _ in rows]
