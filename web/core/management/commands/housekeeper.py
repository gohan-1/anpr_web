"""
The housekeeper:  python manage.py housekeeper [--once]
    make housekeep          one round, normal development (supervisor) setup
    make docker-housekeep   one round inside Docker (the `housekeeper` container runs it forever)

Every `--interval` seconds (default 30):
  * items held too long by a silent bot go back to their line            (both setups)
  * images waiting for a switched-off stage (preprocessing) move on       (both setups)
  * folders whose images are all finished but nobody routed get routed   (both setups)
  * alive reader bots get their shard (1/3, 2/3, 3/3)                     (Docker only)
  * rows of bots whose container is gone are removed                     (Docker only)

Docker (ANPR_BOTS_MANAGED_BY=docker): there is no supervisor, so the `housekeeper` container
does all of them. Normal development: the supervisor already does the first three every 30 s and
numbers the readers itself; running this by hand just does those three right now. The last two
are skipped there: a bot with no heartbeat is simply switched off, not gone.
"""
import signal
import time

from django.conf import settings
from django.core.management.base import BaseCommand

from core import housekeeping
from core.safety import require_writable
from pipeline.config import load_config


class Command(BaseCommand):
    help = "Keep the assembly line healthy when Docker runs the bots"

    def add_arguments(self, parser):
        parser.add_argument("--interval", type=float, default=30.0)
        parser.add_argument("--once", action="store_true", help="one round, then exit (for tests and cron)")

    def handle(self, *args, **opts):
        docker = settings.BOTS_MANAGED_BY == "docker"
        require_writable("run the housekeeper")
        bcfg = load_config()["bots"]
        stale = bcfg["stale_after_seconds"]
        dead_after = bcfg.get("remove_dead_after_minutes", 10)
        self.running = True
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "running", False))   # docker stop
        mode = "Docker: all chores" if docker else "development: stuck items, skipped stage, forgotten folders"
        self.stdout.write(f"Housekeeper ({mode})" + ("" if opts["once"] else
                          f", every {opts['interval']:g}s. Ctrl+C to stop."))
        while self.running:
            for name, n in housekeeping.return_stuck_items().items():
                self.stdout.write(f"[{name}] returned {n} stuck item(s) to the line")
            if skipped := housekeeping.skip_switched_off():
                self.stdout.write(f"Moved {skipped} image(s) past the switched-off preprocessing")
            if routed := housekeeping.route_forgotten_folders():
                self.stdout.write(f"Routed {routed} folder(s) whose images were all finished")
            if docker and (changed := housekeeping.assign_reader_shards(stale)):
                self.stdout.write(f"Gave {changed} reader bot(s) a new shard")
            if docker and (gone := housekeeping.remove_dead_bots(dead_after)):
                self.stdout.write(f"Removed {len(gone)} bot(s) whose container is gone: {', '.join(gone)}")
            if opts["once"]:
                break
            end = time.monotonic() + opts["interval"]
            while self.running and time.monotonic() < end:
                time.sleep(0.5)
        self.stdout.write("Housekeeper stopped.")
