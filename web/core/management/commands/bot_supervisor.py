"""
The supervisor:  python manage.py bot_supervisor

This is the ONLY command needed to process folders. It watches:
  * each bot's own "Input folder" (set on the Bots page) -> only that bot processes them
  * paths.input_dir from config/settings.yaml -> shared by bots that have no input folder
    (override with --input, or turn all scanning off with --no-scan)

It runs forever and, every 2 seconds, makes reality match the Bots table:
  * a bot that SHOULD run but has no process      -> start it
  * a bot that should stop, or is being deleted     -> it stops itself; force it if it hangs
  * a bot being deleted that has stopped            -> delete its row
  * a bot whose process died unexpectedly           -> mark crashed, restart it (with a limit)
  * folders stuck with a dead bot                   -> put them back in the queue
  * every scan_every_seconds, queue any new sub-folders of the watched folders

This "compare what we want with what is, then fix the difference" loop is called
reconciliation. Kubernetes works exactly the same way, which makes it easy to move later.
"""
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core import queue
from core.models import Bot, BotEvent
from pipeline.config import load_config

MANAGE_PY = str(Path(settings.BASE_DIR) / "manage.py")
MAX_RESTARTS = 5                 # crashes within RESTART_WINDOW before we give up
RESTART_WINDOW = timedelta(minutes=10)


class Command(BaseCommand):
    help = "Start, stop, restart and clean up bots so they match the Bots table"

    def add_arguments(self, parser):
        parser.add_argument("--input", help="folder to watch (default: paths.input_dir in settings.yaml)")
        parser.add_argument("--no-scan", action="store_true", help="don't queue folders automatically")
        parser.add_argument("--interval", type=float, default=2.0)

    def handle(self, *args, **opts):
        cfg = load_config()
        self.bcfg = cfg["bots"]
        self.procs: dict[int, subprocess.Popen] = {}     # bot id -> its process
        self.stop_asked: dict[int, float] = {}           # bot id -> when we first saw "stop"
        self.crashes: dict[int, list] = {}               # bot id -> recent crash times
        scan = not opts["no_scan"]
        input_dir = None
        if scan:
            input_dir = Path(opts["input"]).resolve() if opts["input"] else queue.resolve_dir(cfg["paths"]["input_dir"])
            if not input_dir.is_dir():
                raise CommandError(f"Input folder {input_dir} does not exist. Set paths.input_dir in settings.yaml.")
        last_scan = 0.0

        # Bots that were "running" before the supervisor (re)started have no process now.
        Bot.objects.filter(state__in=[Bot.State.RUNNING, Bot.State.STARTING, Bot.State.STOPPING],
                           pid__isnull=False).update(state=Bot.State.STOPPED, pid=None)
        self.stdout.write(f"Supervisor running. Default folder: {input_dir or 'none (--no-scan)'}; "
                          "bots with their own input folder are watched too. "
                          "Ctrl+C to stop all bots and exit.")
        try:
            while True:
                self.reconcile()
                moved = queue.requeue_stale(self.bcfg["claim_timeout_seconds"], self.bcfg["max_attempts"])
                if moved:
                    self.stdout.write(f"Returned {moved} stuck folder(s) to the queue")
                if scan and time.monotonic() - last_scan >= self.bcfg["scan_every_seconds"]:
                    last_scan = time.monotonic()
                    for where, added in queue.scan_all(input_dir, cfg["processing"]["min_folder_age_seconds"]).items():
                        if added:
                            self.stdout.write(f"Queued {added} new folder(s) from {where}")
                time.sleep(opts["interval"])
        except KeyboardInterrupt:
            self.stdout.write("Stopping all bots...")
            for proc in self.procs.values():
                proc.terminate()                         # bots catch this and finish gracefully
            for proc in self.procs.values():
                try:
                    proc.wait(timeout=self.bcfg["stop_grace_seconds"])
                except subprocess.TimeoutExpired:
                    proc.kill()

    # ------------------------------------------------------------------ the core loop
    def reconcile(self):
        now = timezone.now()
        bots = {b.id: b for b in Bot.objects.all()}

        for bot in bots.values():
            proc = self.procs.get(bot.id)
            alive = proc is not None and proc.poll() is None
            wants_stop = bot.delete_requested or not bot.desired_running

            if wants_stop:
                if alive:
                    first = self.stop_asked.setdefault(bot.id, time.monotonic())
                    if time.monotonic() - first > self.bcfg["stop_grace_seconds"]:
                        self.event(bot, "Did not stop in time; forcing it", "warning")
                        proc.kill()
                    continue
                self.stop_asked.pop(bot.id, None)
                self.procs.pop(bot.id, None)
                if bot.delete_requested:
                    self.stdout.write(f"Deleted bot {bot.name}")
                    bot.delete()
                elif bot.state in (Bot.State.RUNNING, Bot.State.STARTING, Bot.State.STOPPING):
                    Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STOPPED, pid=None, current_task="")
                continue

            # From here on, the bot SHOULD be running.
            if alive:
                if self.heartbeat_stale(bot, now) and bot.state == Bot.State.RUNNING:
                    self.event(bot, "No heartbeat; restarting it", "warning")
                    proc.kill()
                continue

            if proc is not None:                         # it had a process, and it died
                code = proc.returncode
                self.procs.pop(bot.id)
                if code != 0:
                    if not self.record_crash(bot, code):
                        continue                         # too many crashes: left switched off
            elif bot.state == Bot.State.RUNNING and not self.heartbeat_stale(bot, now):
                continue                                 # running outside this supervisor (e.g. another machine)

            self.start(bot)

        # Processes whose bot row disappeared (deleted in the admin, say)
        for bot_id in [i for i in self.procs if i not in bots]:
            self.procs.pop(bot_id).terminate()

    # ------------------------------------------------------------------ helpers
    def start(self, bot: Bot):
        proc = subprocess.Popen([sys.executable, MANAGE_PY, "run_bot", bot.name])
        self.procs[bot.id] = proc
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STARTING, pid=proc.pid, last_heartbeat=timezone.now())
        self.stdout.write(f"Started bot {bot.name} (pid {proc.pid})")

    def record_crash(self, bot: Bot, code: int) -> bool:
        """Returns True if we should restart it, False if it crashed too often."""
        now = timezone.now()
        recent = [t for t in self.crashes.get(bot.id, []) if now - t < RESTART_WINDOW] + [now]
        self.crashes[bot.id] = recent
        if len(recent) > MAX_RESTARTS:
            Bot.objects.filter(pk=bot.pk).update(state=Bot.State.CRASHED, desired_running=False, pid=None,
                                                 current_task="")
            self.event(bot, f"Crashed {len(recent)} times in 10 minutes; switched off. "
                            "Check its last error, then press Start.", "error")
            return False
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.CRASHED, restarts=bot.restarts + 1, pid=None)
        self.event(bot, f"Process exited unexpectedly (code {code}); restarting", "error")
        return True

    def heartbeat_stale(self, bot: Bot, now) -> bool:
        return (bot.last_heartbeat is None
                or (now - bot.last_heartbeat).total_seconds() > self.bcfg["stale_after_seconds"])

    def event(self, bot: Bot, message: str, level: str = "info"):
        BotEvent.objects.create(bot=bot, message=message, level=level)
        self.stdout.write(f"[{bot.name}] {message}")
