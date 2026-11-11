"""
Runs ONE bot:  python manage.py run_bot <bot-name>

Normally the supervisor starts this for you. You can also run it by hand, even on
another machine that uses the same database, and it will show up on the Bots page.

WHAT THE LOOP DOES, every turn:
  1. keep up to `concurrency` folders in progress (claim more from the queue if there's room)
  2. save any folder that finished (database writes only happen in this main thread)
  3. every few seconds, send a heartbeat and re-read the bot's settings
  4. if someone pressed Stop or Delete: claim nothing new, finish what's running, exit
"""
import os
import signal
import socket
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import F
from django.utils import timezone

from core import queue
from core.ingest import save_folder_result
from core.models import Bot, BotEvent
from pipeline.config import load_config
from pipeline.processor import analyse_folder
from pipeline.ratelimit import RateLimiter


def log(bot: Bot, message: str, level: str = "info") -> None:
    BotEvent.objects.create(bot=bot, message=message, level=level)
    # keep only the latest 200 events per bot
    old = BotEvent.objects.filter(bot=bot).values_list("id", flat=True)[200:]
    if old:
        BotEvent.objects.filter(id__in=list(old)).delete()


class Command(BaseCommand):
    help = "Run one bot that processes folders from the queue"

    def add_arguments(self, parser):
        parser.add_argument("name")

    def handle(self, *args, name, **opts):
        try:
            bot = Bot.objects.get(name=name)
        except Bot.DoesNotExist:
            raise CommandError(f"No bot named '{name}'. Create it on the Bots page first.")

        cfg = load_config()
        bcfg = cfg["bots"]
        credentials = (bot.api_username, bot.get_password())
        limiter = RateLimiter(bot.rate_limit)

        # Prefect mode: each folder becomes a flow run in the Prefect dashboard.
        use_prefect = bcfg.get("use_prefect", False)
        if use_prefect:
            from pipeline.prefect_bot import analyse_folder_with_prefect, register_bot
            register_bot(bot.name, cfg, credentials, limiter)
            work = lambda path: analyse_folder_with_prefect(path, bot.name)      # noqa: E731
        else:
            work = lambda path: analyse_folder(path, cfg, credentials, limiter)  # noqa: E731

        # Ctrl+C or the supervisor's "terminate" -> stop gracefully instead of dying mid-folder
        self.stop_signal = False
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop_signal", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop_signal", True))

        Bot.objects.filter(pk=bot.pk).update(
            state=Bot.State.RUNNING, pid=os.getpid(), host=socket.gethostname(),
            started_at=timezone.now(), last_heartbeat=timezone.now(), last_error="", current_task="")
        log(bot, f"Started on {socket.gethostname()} (pid {os.getpid()}), concurrency {bot.concurrency}"
                 + (", via Prefect" if use_prefect else ""))
        self.stdout.write(f"Bot {name} running. Ctrl+C to stop.")

        pool = ThreadPoolExecutor(max_workers=bcfg["max_concurrency"])
        running = {}                    # future -> WorkItem
        stopping = False
        last_beat = 0.0

        try:
            while True:
                # --- heartbeat + re-read settings (Stop / Delete / new concurrency) -------------
                if time.monotonic() - last_beat >= bcfg["heartbeat_seconds"]:
                    last_beat = time.monotonic()
                    fresh = Bot.objects.filter(pk=bot.pk).first()
                    if fresh is None or not fresh.desired_running or fresh.delete_requested or self.stop_signal:
                        if not stopping:
                            stopping = True
                            if fresh:
                                Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STOPPING)
                                log(bot, f"Stopping: finishing {len(running)} folder(s) in progress")
                    if fresh:
                        bot = fresh
                        if (bot.rate_limit or 0.0) != limiter.per_second:     # edited on the Bots page
                            limiter.set_rate(bot.rate_limit)
                            log(bot, f"Rate limit is now {bot.rate_limit or 'unlimited'} calls/s")
                        Bot.objects.filter(pk=bot.pk).update(
                            last_heartbeat=timezone.now(),
                            current_task="\n".join(Path(i.path).name for i in running.values()))
                    elif stopping and not running:
                        break

                # --- claim more work if there's room -------------------------------------
                if not stopping:
                    room = min(bot.concurrency, bcfg["max_concurrency"]) - len(running)
                    for item in queue.claim(bot, room, help_shared=bcfg.get("help_shared_queue", True)):
                        running[pool.submit(work, Path(item.path))] = item

                if stopping and not running:
                    break
                if not running:
                    time.sleep(1)       # queue is empty: idle quietly
                    continue

                # --- save finished folders --------------------------------------------------
                done, _ = wait(running, timeout=1, return_when=FIRST_COMPLETED)
                for future in done:
                    item = running.pop(future)
                    try:
                        images = future.result()
                        folder = save_folder_result(Path(item.path), images, processed_by=bot.name)
                        queue.complete(item, folder)
                        # Count it in the database right away (F() = "add 1 to whatever is stored"),
                        # so the numbers stay exact even if this process is killed a moment later.
                        Bot.objects.filter(pk=bot.pk).update(processed=F("processed") + 1)
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        status = queue.fail(item, error, bcfg["max_attempts"])
                        Bot.objects.filter(pk=bot.pk).update(failed=F("failed") + 1,
                                                             last_error=f"{Path(item.path).name}: {error}"[:2000])
                        log(bot, f"{Path(item.path).name} failed ({error}); "
                                 f"{'gave up' if status == 'failed' else 'back in queue'}", "error")
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
            if running:
                queue.release(running.values())
            fresh = Bot.objects.filter(pk=bot.pk).first()
            if fresh:
                Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STOPPED, pid=None, current_task="")
                log(bot, "Stopped")
            self.stdout.write(f"Bot {name} stopped.")
