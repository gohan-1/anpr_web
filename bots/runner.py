"""
The bot loop, one for every stage.

    forever:
        if told to stop (Stop/Remove in the UI, Ctrl+C, Docker stop): exit after this batch
        if the next line is too full (backpressure): wait
        take a batch from MY line                     -> stage_queue.take()
        do the work on every item, in parallel        -> handlers (later: a service call each)
        save each result: next line, or back to mine  -> stage_queue.finish() / fail()
        (OCR only) folders whose images are all read get routed: trip / LLM / manual

A separate thread sends a heartbeat every few seconds, so the supervisor knows the bot
is alive even while it works on a long batch.

Two ways bots are run (settings ANPR_BOTS_MANAGED_BY):
  supervisor  the supervisor starts this process; Stop in the web app = finish the batch and EXIT
  docker      this process IS the container; Stop in the web app = PAUSE (stay alive, take no
              work), because an exited container would just be restarted by Docker.
              The container's BOT_API_USERNAME / BOT_API_PASSWORD / BOT_RATE_LIMIT, if set,
              become the bot's API account and rate limit when it starts.
"""
from __future__ import annotations

import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from django.conf import settings
from django.db import close_old_connections, connection
from django.db.models import F
from django.utils import timezone

from core import stage_queue
from core.models import Bot, BotEvent, IntakeFolder, IntakeImage
from pipeline.config import load_config
from pipeline.stages import STAGES, next_line_of

from .handlers import CONTEXTS, HANDLERS, BotContext, Outcome, payload_of


def log(bot_id: int, message: str, level: str = "info") -> None:
    BotEvent.objects.create(bot_id=bot_id, message=message, level=level)


class BaseBot:
    def __init__(self, name: str, stage: str):
        self.name, self.stage = name, stage
        self.cfg = load_config()
        self.pipe = self.cfg["pipeline"]
        self.stopping = False
        self._beat_stop = threading.Event()
        signal.signal(signal.SIGTERM, self._on_signal)     # docker stop / supervisor
        signal.signal(signal.SIGINT, self._on_signal)      # Ctrl+C

    def _on_signal(self, *_):
        self.stopping = True

    @property
    def in_docker(self) -> bool:
        return settings.BOTS_MANAGED_BY == "docker"

    # ---------------------------------------------------------------- bot row + heartbeat
    def register(self, shard: str = "") -> None:
        bot, created = Bot.objects.get_or_create(
            name=self.name, defaults={"stage": self.stage, "desired_running": True})
        if bot.stage != self.stage:
            raise SystemExit(f"Bot '{self.name}' belongs to stage '{bot.stage}', not '{self.stage}'.")
        self.bot_id = bot.id
        self.account_from_environment(bot)
        Bot.objects.filter(pk=bot.id).update(
            state=Bot.State.RUNNING, pid=os.getpid(), host=socket.gethostname(), shard=shard,
            started_at=timezone.now(), last_heartbeat=timezone.now(), last_error="", current_task="")
        log(bot.id, f"Started ({self.stage}{', shard ' + shard if shard else ''}) on "
                    f"{socket.gethostname()}, pid {os.getpid()}")
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def _heartbeat(self) -> None:
        while not self._beat_stop.wait(self.pipe["heartbeat_seconds"]):
            try:
                close_old_connections()
                Bot.objects.filter(pk=self.bot_id).update(last_heartbeat=timezone.now())
            except Exception:
                pass                              # a missed heartbeat is not worth crashing for
        connection.close()

    def account_from_environment(self, bot: Bot) -> None:
        """Docker: the container's environment sets the API account and rate limit."""
        username, password = os.environ.get("BOT_API_USERNAME", ""), os.environ.get("BOT_API_PASSWORD", "")
        rate = os.environ.get("BOT_RATE_LIMIT", "")
        if not (username or rate):
            return
        if username and password:
            bot.api_username = username
            bot.set_password(password)
        if rate:
            bot.rate_limit = float(rate)
        bot.save(update_fields=["api_username", "api_password_encrypted", "rate_limit"])

    def should_stop(self) -> bool:
        """True when the process must end. In Docker mode Stop/Start in the web app only pause
        (see wait_while_paused); the process ends only when its container is stopped."""
        if self.stopping:
            return True
        row = Bot.objects.filter(pk=self.bot_id).values("desired_running", "delete_requested").first()
        if row is None or row["delete_requested"] or (not row["desired_running"] and not self.in_docker):
            self.stopping = True
        return self.stopping

    def wait_while_paused(self) -> bool:
        """Docker mode: while switched off in the web app, stay alive and idle.
        Returns True if the bot was paused (the caller then checks again)."""
        if not self.in_docker:
            return False
        if Bot.objects.filter(pk=self.bot_id, desired_running=False).exists():
            if not getattr(self, "_paused", False):
                self._paused = True
                Bot.objects.filter(pk=self.bot_id).update(state=Bot.State.STOPPED, current_task="Paused from the web app")
                log(self.bot_id, "Paused: takes no work until Start is pressed")
            self.nap(self.pipe["idle_sleep_seconds"])
            return True
        if getattr(self, "_paused", False):
            self._paused = False
            Bot.objects.filter(pk=self.bot_id).update(state=Bot.State.RUNNING, current_task="")
            log(self.bot_id, "Resumed")
        return False

    def set_task(self, text: str) -> None:
        Bot.objects.filter(pk=self.bot_id).update(current_task=text[:500])

    def shutdown(self) -> None:
        self._beat_stop.set()
        Bot.objects.filter(pk=self.bot_id).update(state=Bot.State.STOPPED, pid=None, current_task="")
        log(self.bot_id, "Stopped")

    def nap(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end and not self.stopping:
            time.sleep(0.2)


class StageBot(BaseBot):
    """A bot for preprocess, ocr, llm or trip."""

    def __init__(self, name: str, stage: str):
        super().__init__(name, stage)
        self.spec = STAGES[stage]
        self.scfg = stage_queue.settings_of(stage)
        self.handler = HANDLERS[stage]
        from pipeline.ratelimit import RateLimiter
        self.ctx = CONTEXTS[name] = BotContext(name, api_cfg=self.cfg.get("api", {}), limiter=RateLimiter(0))
        self._account = None

    def run(self) -> None:
        self.register()
        Bot.objects.filter(pk=self.bot_id).update(state=Bot.State.RUNNING)
        # Items still held under this bot's name come from its previous run (it crashed, or its
        # container restarted): give them back now instead of waiting stuck_after_minutes.
        if back := stage_queue.reclaim(self.stage, self.name, self.scfg["max_attempts"]):
            log(self.bot_id, f"Gave back {back} item(s) held by this bot before it restarted", "warning")
        batches = 0
        try:
            while not self.should_stop():
                if self.wait_while_paused():
                    continue
                self.scfg = stage_queue.settings_of(self.stage)    # changes on the Pipeline page apply here
                if not self.refresh_account():
                    self.set_task("Waiting: this OCR bot has no API account (set it on the Bots page)")
                    self.nap(self.pipe["idle_sleep_seconds"] * 5)
                    continue
                if self.line_too_full():
                    self.set_task("Paused: the next line is full")
                    self.nap(self.pipe["idle_sleep_seconds"])
                    continue
                items = stage_queue.take(self.stage, self.name, self.scfg["batch_size"])
                if not items:
                    self.set_task("")
                    self.nap(self.pipe["idle_sleep_seconds"])
                    continue
                batches += 1
                self.set_task(f"Batch {batches}: {len(items)} items")
                outcomes = self.process(items, batches)
                self.save(items, outcomes)
        finally:
            self.shutdown()

    def refresh_account(self) -> bool:
        """Re-reads the bot's API account and rate limit before every batch, so edits on the
        Bots page apply without a restart. Returns False if the bot can't work without one."""
        row = Bot.objects.filter(pk=self.bot_id).values("api_username", "api_password_encrypted", "rate_limit").first()
        if row is None:
            return False
        if (row["rate_limit"] or 0.0) != self.ctx.limiter.per_second:
            self.ctx.limiter.set_rate(row["rate_limit"])
            log(self.bot_id, f"Rate limit is now {row['rate_limit'] or 'unlimited'} calls/s")
        account = (row["api_username"], row["api_password_encrypted"])
        if account != self._account:
            self._account = account
            from core.crypto import decrypt
            self.ctx.credentials = (row["api_username"], decrypt(row["api_password_encrypted"])) \
                if row["api_username"] and row["api_password_encrypted"] else None
        needs_account = self.stage == "ocr" and not self.ctx.api_cfg.get("mock", True)
        if needs_account and not self.ctx.credentials and not getattr(self, "_warned", False):
            self._warned = True
            log(self.bot_id, "The OCR API is real (api.mock is false) but this bot has no API account. "
                             "Set its username and password on the Bots page.", "error")
        return not (needs_account and not self.ctx.credentials)

    def line_too_full(self) -> bool:
        limit = self.scfg.get("pause_if_next_line_over")    # missing = no limit (0 is a real limit)
        nxt = next_line_of(self.stage)
        if limit is None or not nxt:
            return False
        Model = IntakeImage if nxt[0] == "image" else IntakeFolder
        return Model.objects.filter(stage=nxt[1]).count() > limit

    # ---------------------------------------------------------------- doing the work
    def process(self, items, batch_no: int) -> list[Outcome]:
        payloads = [payload_of(self.spec.level, i) for i in items]
        if self.pipe.get("use_prefect"):
            from .prefect_batch import prefect_reachable, run_batch   # only imported when Prefect is used
            if prefect_reachable():
                if getattr(self, "_prefect_down", False):
                    self._prefect_down = False
                    log(self.bot_id, "Prefect is reachable again: batches are recorded there again")
                return run_batch(self.stage, self.name, batch_no, payloads, self.scfg)
            if not getattr(self, "_prefect_down", False):     # say it once, not every batch
                self._prefect_down = True
                log(self.bot_id, "Prefect server not reachable: working WITHOUT recording batches in Prefect "
                                 "(start it, or set pipeline.use_prefect: false)", "warning")

        def safe(p):
            try:
                return self.handler(p, self.scfg, self.ctx)
            except Exception as exc:                       # one bad item never stops the batch
                return Outcome(p["id"], False, error=f"{type(exc).__name__}: {exc}")

        with ThreadPoolExecutor(max_workers=self.scfg.get("parallel", 4)) as pool:
            return list(pool.map(safe, payloads))

    # ---------------------------------------------------------------- saving results
    def save(self, items, outcomes: list[Outcome]) -> None:
        by_id = {i.id: i for i in items}
        done = failed = 0
        for o in outcomes:
            item = by_id[o.id]
            if o.ok:
                if stage_queue.finish(self.stage, item, self.name, o.to or self.spec.goes_to, **o.fields):
                    done += 1
            else:
                stage_queue.fail(self.stage, item, self.name, o.error, self.scfg["max_attempts"])
                failed += 1
        if self.stage == "ocr":
            self.route_finished_folders({i.folder_id for i in items})
        stage_queue.record(self.stage, done, failed)
        Bot.objects.filter(pk=self.bot_id).update(processed=F("processed") + done, failed=F("failed") + failed)
        if failed:
            last = next(o.error for o in outcomes if not o.ok)
            Bot.objects.filter(pk=self.bot_id).update(last_error=last[:2000])
            log(self.bot_id, f"{failed} of {len(outcomes)} items failed, e.g. {last[:200]}", "error")

    def route_finished_folders(self, folder_ids) -> None:
        """Folders whose images are all through OCR leave the image lines: their route is
        decided from the plates read, and their result appears on the Folders page."""
        from core.ocr_results import finish_folder
        for folder in stage_queue.folders_ready(folder_ids):
            finish_folder(folder, self.scfg)


class ReaderBot(BaseBot):
    """Scans its share (shard) of the source folders every few seconds and fills the first line."""

    def __init__(self, name: str, shard: str):
        super().__init__(name, "reader")
        from services.reader.scanner import Shard, load_sources
        from django.conf import settings
        self.shard = Shard.parse(shard or "1/1")
        rcfg = self.cfg["reader"]
        self.sources = sorted(load_sources(rcfg, settings.PROJECT_ROOT), key=lambda s: s.priority)
        self.rcfg = rcfg
        self.every = stage_queue.settings_of("reader").get("scan_every_seconds", 30)

    def follow_assigned_shard(self) -> None:
        """Docker mode: the housekeeper numbers the alive readers (1/3, 2/3, 3/3) in the database."""
        if not self.in_docker:
            return
        from services.reader.scanner import Shard
        assigned = Bot.objects.filter(pk=self.bot_id).values_list("shard", flat=True).first()
        if assigned and assigned != str(self.shard):
            self.shard = Shard.parse(assigned)
            log(self.bot_id, f"Now scanning shard {assigned}")

    def run(self) -> None:
        from services.reader.scanner import scan_source
        self.register(shard="" if self.in_docker else str(self.shard))
        try:
            while not self.should_stop():
                if self.wait_while_paused():
                    continue
                self.follow_assigned_shard()
                self.set_task(f"Scanning, shard {self.shard}")
                registered = errors = 0
                for source in self.sources:
                    r = scan_source(source, self.shard, self.name,
                                    extensions={e.lower() for e in self.rcfg["image_extensions"]},
                                    min_age_seconds=self.rcfg["min_folder_age_seconds"],
                                    workers=self.rcfg["read_workers"])
                    registered += r.registered
                    errors += len(r.errors)
                    for e in r.errors[:5]:
                        log(self.bot_id, f"{source.name}: {e}", "warning")
                stage_queue.record("reader", registered, errors)
                Bot.objects.filter(pk=self.bot_id).update(processed=F("processed") + registered)
                self.set_task(f"Shard {self.shard}: last scan added {registered} folders")
                self.every = stage_queue.settings_of("reader").get("scan_every_seconds", self.every)
                self.nap(self.every)
        finally:
            self.shutdown()
