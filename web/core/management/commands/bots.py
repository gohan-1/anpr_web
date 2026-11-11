"""
    python manage.py bots        (or: make bots)

Shows every bot: what you asked for (on/off), what is really happening (state, heartbeat),
its input folder and counters, plus a summary of the queue.
"""
from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from core.models import Bot, WorkItem
from pipeline.config import load_config


class Command(BaseCommand):
    help = "List bots and the queue"

    def handle(self, *args, **opts):
        stale = load_config()["bots"]["stale_after_seconds"]
        now = timezone.now()
        bots = list(Bot.objects.order_by("name"))
        if not bots:
            self.stdout.write("No bots yet. Add one on the Bots page of the web app.")
        else:
            self.stdout.write(f"{'BOT':<14}{'WANTED':<8}{'STATE':<10}{'ALIVE':<7}{'PID':<9}"
                              f"{'DONE':>6}{'FAILED':>8}{'RESTARTS':>10}  INPUT FOLDER")
        alive_count = 0
        for b in bots:
            alive = (b.state == Bot.State.RUNNING and b.last_heartbeat is not None
                     and (now - b.last_heartbeat).total_seconds() <= stale)
            alive_count += alive
            wanted = "delete" if b.delete_requested else ("on" if b.desired_running else "off")
            self.stdout.write(f"{b.name:<14}{wanted:<8}{b.state:<10}{('yes' if alive else 'no'):<7}"
                              f"{str(b.pid or '-'):<9}{b.processed:>6}{b.failed:>8}{b.restarts:>10}  "
                              f"{b.input_dir or '(shared queue)'}")
        if bots:
            wanted_on = sum(b.desired_running for b in bots)
            self.stdout.write(f"\n{len(bots)} bot(s): {wanted_on} switched on, {alive_count} actually running "
                              "(heartbeat in the last %ss)." % stale)
            if wanted_on > alive_count:
                self.stdout.write("Switched on but not running? The supervisor isn't running: make start-supervisor")

        rows = WorkItem.objects.values_list("assigned_bot__name", "status").annotate(n=Count("id"))
        queue: dict[str, dict[str, int]] = {}
        for bot_name, status, n in rows:
            queue.setdefault(bot_name or "(shared)", {})[status] = n
        if queue:
            self.stdout.write("\nQueue (folders):")
            for owner, counts in sorted(queue.items()):
                self.stdout.write(f"  {owner:<16}" + "  ".join(f"{s}={n}" for s, n in sorted(counts.items())))
            shared_waiting = queue.get("(shared)", {}).get("pending", 0)
            help_shared = load_config()["bots"].get("help_shared_queue", True)
            takers = [b for b in bots if b.desired_running and (help_shared or not b.input_dir)]
            if shared_waiting and not takers:
                who = "a switched-on bot" if help_shared else "a switched-on bot WITHOUT an input folder"
                self.stdout.write(f"  Note: {shared_waiting} shared folder(s) wait for {who}; there is none.")
