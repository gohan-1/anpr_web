"""
The supervisor:  python manage.py bot_supervisor      (make start runs it for you)

Every bot is a row in the Bots table and belongs to one stage of the assembly line
(reader, preprocess, ocr, llm, trip). For every bot that should run, the supervisor starts
`python -m bots --stage X --name Y` and keeps it alive. Reader bots share the folders on
disk by shard (1/3, 2/3, 3/3); when you add or remove one, the shards are renumbered and
running readers restart with their new share.

It runs forever and, every 2 seconds, makes reality match the Bots table:
  * a bot that SHOULD run but has no process      -> start it
  * a bot that should stop, or is being deleted     -> it stops itself; force it if it hangs
  * a bot being deleted that has stopped            -> delete its row
  * a bot whose process died unexpectedly           -> mark crashed, restart it (with a limit)
  * every 30 seconds: items held too long by a silent bot go back to their line, and
    folders whose images are all finished but were never routed get routed

This "compare what we want with what is, then fix the difference" loop is called
reconciliation. Kubernetes works exactly the same way, which makes it easy to move later.
"""
import signal
import subprocess
import sys
import time
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from core import stage_queue
from core.models import Bot, BotEvent, IntakeFolder
from core.safety import add_yes_option, confirm_environment, require_writable
from pipeline.config import load_config
from pipeline.stages import STAGES

PROJECT_ROOT = str(settings.PROJECT_ROOT)        # where "python -m bots" must run
MAX_RESTARTS = 5                 # crashes within RESTART_WINDOW before we give up
RESTART_WINDOW = timedelta(minutes=10)


class Command(BaseCommand):
    help = "Start, stop, restart and clean up bots so they match the Bots table"

    def add_arguments(self, parser):
        parser.add_argument("--interval", type=float, default=2.0)
        add_yes_option(parser)

    def handle(self, *args, **opts):
        require_writable("start the bot supervisor")
        confirm_environment("start the bot supervisor", assume_yes=opts.get("yes", False), stdout=self.stdout)
        cfg = load_config()
        self.bcfg = cfg["bots"]
        self.procs: dict[int, subprocess.Popen] = {}     # bot id -> its process
        self.stop_asked: dict[int, float] = {}           # bot id -> when we first saw "stop"
        self.crashes: dict[int, list] = {}               # bot id -> recent crash times
        self.restarting: set[int] = set()                # reader bots being restarted with a new shard
        self.shards: dict[int, str] = {}
        last_stuck_check = 0.0

        # `kill`, `make stop` and systemd send SIGTERM, not Ctrl+C. Treat it the same way, so the
        # bots are stopped gracefully instead of being left running without a supervisor.
        def _terminate(*_):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, _terminate)

        # Bots that were "running" before the supervisor (re)started have no process now.
        Bot.objects.filter(state__in=[Bot.State.RUNNING, Bot.State.STARTING, Bot.State.STOPPING],
                           pid__isnull=False).update(state=Bot.State.STOPPED, pid=None)
        self.stdout.write("Supervisor running. Ctrl+C to stop all bots and exit.")
        try:
            while True:
                self.reconcile()
                if time.monotonic() - last_stuck_check >= 30:
                    last_stuck_check = time.monotonic()
                    for name in STAGES:
                        c = stage_queue.settings_of(name)
                        back = stage_queue.return_stuck(name, c["stuck_after_minutes"], c["max_attempts"])
                        if back:
                            self.stdout.write(f"[{name}] returned {back} stuck item(s) to the line")
                    routed = self.route_forgotten_folders()
                    if routed:
                        self.stdout.write(f"Routed {routed} folder(s) whose images were all finished")
                time.sleep(opts["interval"])
        except KeyboardInterrupt:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)   # a second stop signal must not cut this short
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

        # Reader bots split the folders on disk: with 3 of them they get 1/3, 2/3 and 3/3.
        # When you add or remove one, the numbers change, and running readers restart with theirs.
        readers = sorted((b for b in bots.values() if b.stage == Bot.Stage.READER
                          and b.desired_running and not b.delete_requested), key=lambda b: b.name)
        self.shards = {b.id: f"{i}/{len(readers)}" for i, b in enumerate(readers, 1)}

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
            if alive and bot.stage == Bot.Stage.READER and bot.shard and bot.shard != self.shards.get(bot.id):
                if bot.id not in self.restarting:          # finish the current scan, then restart
                    self.restarting.add(bot.id)
                    self.event(bot, f"Reader count changed: restarting as shard {self.shards.get(bot.id)}")
                    proc.terminate()
                continue

            if alive:
                if self.heartbeat_stale(bot, now) and bot.state == Bot.State.RUNNING:
                    self.event(bot, "No heartbeat; restarting it", "warning")
                    proc.kill()
                continue

            if proc is not None:                         # it had a process, and it died
                code = proc.returncode
                self.procs.pop(bot.id)
                if bot.id in self.restarting:              # a planned restart, not a crash
                    self.restarting.discard(bot.id)
                elif code != 0:
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
        # The child inherits ANPR_ENV and the database settings, so it uses the same database.
        # --yes: the supervisor itself was already confirmed; a bot must never sit waiting for typing.
        shard = self.shards.get(bot.id, "") if bot.stage == Bot.Stage.READER else ""
        cmd = [sys.executable, "-m", "bots", "--stage", bot.stage, "--name", bot.name, "--yes"]
        if shard:
            cmd += ["--shard", shard]
        proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT)
        self.procs[bot.id] = proc
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STARTING, pid=proc.pid, shard=shard,
                                             last_heartbeat=timezone.now())
        self.stdout.write(f"Started {bot.stage} bot {bot.name}{' as shard ' + shard if shard else ''} (pid {proc.pid})")

    def route_forgotten_folders(self, limit: int = 500) -> int:
        """Normally the OCR bot that reads a folder's last image routes it. A folder can be
        missed when its last image FAILED somewhere else (in preprocessing, or returned as stuck):
        this sweep routes those, so no folder waits in 'reading' forever."""
        from core.ocr_results import finish_folder
        ids = list(IntakeFolder.objects.filter(stage="reading").values_list("id", flat=True)[:limit])
        cfg = stage_queue.settings_of("ocr")
        return sum(finish_folder(f, cfg) for f in stage_queue.folders_ready(ids))

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
