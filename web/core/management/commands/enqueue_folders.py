"""
Adds folders to the queue:  python manage.py enqueue_folders --input ../data

Each sub-folder of --input becomes one WorkItem. Folders already queued are skipped,
so it's safe to run this again and again (or let the supervisor do it with --input).
"""
from pathlib import Path

from django.core.management.base import BaseCommand

from core.queue import enqueue_folders


class Command(BaseCommand):
    help = "Queue every sub-folder of --input for the bots"

    def add_arguments(self, parser):
        parser.add_argument("--input", required=True)
        parser.add_argument("--min-age", type=int, default=0, help="skip folders modified in the last N seconds")

    def handle(self, *args, **opts):
        added = enqueue_folders(Path(opts["input"]).resolve(), opts["min_age"])
        self.stdout.write(self.style.SUCCESS(f"Queued {added} new folder(s)."))
