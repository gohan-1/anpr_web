"""
`make bots`: every bot (stage, wanted vs real state, alive?) and the assembly line.
The same information as the Bots and Pipeline pages, for the terminal.
"""
from django.core.management.base import BaseCommand
from django.utils import timezone

from core import stage_queue
from core.models import Bot
from pipeline.config import load_config
from pipeline.stages import BOT_STAGES, STAGE_LABELS


class Command(BaseCommand):
    help = "List the bots and the assembly line"

    def handle(self, *args, **opts):
        stale = load_config()["bots"]["stale_after_seconds"]
        now = timezone.now()
        order = {s: i for i, s in enumerate(BOT_STAGES)}
        bots = sorted(Bot.objects.all(), key=lambda b: (order.get(b.stage, 99), b.name))
        if not bots:
            self.stdout.write("No bots yet. Add them on the Bots page (or + on the Pipeline page).")
        else:
            self.stdout.write(f"{'BOT':<16}{'STAGE':<12}{'WANTED':<8}{'STATE':<10}{'ALIVE':<7}{'PID':<9}"
                              f"{'DONE':>7}{'FAILED':>8}{'RESTARTS':>10}  ACCOUNT / SHARD")
        alive_count = 0
        for b in bots:
            alive = (b.state == Bot.State.RUNNING and b.last_heartbeat is not None
                     and (now - b.last_heartbeat).total_seconds() <= stale)
            alive_count += alive
            wanted = "delete" if b.delete_requested else ("on" if b.desired_running else "off")
            extra = f"shard {b.shard}" if b.shard else (b.api_username or ("no account" if b.stage == "ocr" else ""))
            if b.rate_limit:
                extra += f", {b.rate_limit:g} calls/s"
            self.stdout.write(f"{b.name:<16}{b.stage:<12}{wanted:<8}{b.state:<10}{('yes' if alive else 'no'):<7}"
                              f"{str(b.pid or '-'):<9}{b.processed:>7}{b.failed:>8}{b.restarts:>10}  {extra}")
        if bots:
            wanted_on = sum(b.desired_running for b in bots)
            self.stdout.write(f"\n{len(bots)} bot(s): {wanted_on} switched on, {alive_count} actually running "
                              f"(heartbeat in the last {stale}s).")
            if wanted_on > alive_count:
                self.stdout.write("Switched on but not running? Is the supervisor running?  make start-supervisor")

        stats = stage_queue.line_stats()
        self.stdout.write("\nAssembly line:")
        self.stdout.write(f"  {'STAGE':<15}{'WAITING':>9}{'WORKING':>9}{'PER MIN':>9}{'BOTS ON':>9}")
        for name in BOT_STAGES:
            st = stats["stages"][name]
            on = sum(1 for b in bots if b.stage == name and b.desired_running and not b.delete_requested)
            note = "   <- items waiting, no bots" if st["waiting"] and not on else ""
            self.stdout.write(f"  {STAGE_LABELS[name]:<15}{st['waiting']:>9}{st['working']:>9}"
                              f"{st['per_minute']:>9}{on:>9}{note}")
        self.stdout.write("  Folders: " + ", ".join(f"{k}={v}" for k, v in sorted(stats["folders"].items()))
                          if stats["folders"] else "  No folders registered yet: add a reader bot.")
