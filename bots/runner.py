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
"""
from __future__ import annotations

import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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

    # ---------------------------------------------------------------- bot row + heartbeat
    def register(self, shard: str = "") -> None:
        bot, created = Bot.objects.get_or_create(
            name=self.name, defaults={"stage": self.stage, "desired_running": True})
        if bot.stage != self.stage:
            raise SystemExit(f"Bot '{self.name}' belongs to stage '{bot.stage}', not '{self.stage}'.")
        self.bot_id = bot.id
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

    def should_stop(self) -> bool:
        if self.stopping:
            return True
        row = Bot.objects.filter(pk=self.bot_id).values("desired_running", "delete_requested").first()
        if row is None or not row["desired_running"] or row["delete_requested"]:
            self.stopping = True
        return self.stopping

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
        batches = 0
        try:
            while not self.should_stop():
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
            from .prefect_batch import run_batch          # only imported when Prefect is used
            return run_batch(self.stage, self.name, batch_no, payloads, self.scfg)

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
        self.shard = Shard.parse(shard)
        rcfg = self.cfg["reader"]
        self.sources = sorted(load_sources(rcfg, settings.PROJECT_ROOT), key=lambda s: s.priority)
        self.rcfg = rcfg
        self.every = stage_queue.settings_of("reader").get("scan_every_seconds", 30)

    def run(self) -> None:
        from services.reader.scanner import scan_source
        self.register(shard=str(self.shard))
        try:
            while not self.should_stop():
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
