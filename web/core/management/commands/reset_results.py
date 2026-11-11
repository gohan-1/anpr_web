"""
Deletes processed results so folders get processed again. Bots, their API logins and
user accounts are KEPT. Image files on disk are never touched.

    python manage.py reset_results                      # ALL results       (make reset)
    python manage.py reset_results --folder ocr_4393072 --folder ocr_4393075
                                                        # only these folders (make reprocess FOLDER=…)
    add --yes to skip the "are you sure?" question

Why a folder is processed again afterwards: a folder is queued only if it has no
WorkItem row. Deleting its WorkItem (and its old Folder/Image results) makes the
supervisor queue it again on its next scan (every bots.scan_every_seconds).
"""
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Q

from core.models import Bot, BotEvent, Folder, Image, IntakeFolder, IntakeImage, Job, WorkItem


class Command(BaseCommand):
    help = "Delete processed results so folders are processed again (bots and users are kept)"

    def add_arguments(self, parser):
        parser.add_argument("--folder", action="append", default=[],
                            help="folder name or full path; repeat for several. Without it: ALL results")
        parser.add_argument("--yes", action="store_true", help="don't ask for confirmation")

    def handle(self, *args, **o):
        if o["folder"]:
            self.reset_folders(o["folder"], o["yes"])
        else:
            self.reset_all(o["yes"])

    def confirm(self, question: str, yes: bool) -> None:
        if yes:
            return
        try:
            answer = input(f"{question} Type 'yes' to continue: ")
        except EOFError:                                   # no keyboard (script, cron): never delete silently
            answer = ""
        if answer.strip().lower() != "yes":
            raise CommandError("Cancelled, nothing deleted. (Use --yes when running without a keyboard.)")

    # ---------------------------------------------------------------- everything
    def reset_all(self, yes: bool):
        models = [WorkItem, Image, Folder, BotEvent, Job, IntakeImage, IntakeFolder]
        counts = {m.__name__: m.objects.count() for m in models}
        self.stdout.write("Will delete: " + ", ".join(f"{n} {name}" for name, n in counts.items()))
        self.stdout.write(f"Will keep: {Bot.objects.count()} bot(s) (counters reset to 0) and all user logins.")
        if WorkItem.objects.filter(status=WorkItem.Status.IN_PROGRESS).exists():
            self.stdout.write(self.style.WARNING(
                "Bots are working on folders right now. Stop everything first (make stop), "
                "or their results will be saved after the reset."))
        self.confirm("Delete ALL processed results?", yes)
        with transaction.atomic():
            tables = ", ".join(m._meta.db_table for m in models)
            if connection.vendor == "postgresql":
                connection.cursor().execute(f"TRUNCATE {tables} RESTART IDENTITY")   # fast, ids start at 1 again
            else:
                for m in models:
                    m.objects.all().delete()
            Bot.objects.update(processed=0, failed=0, restarts=0, last_error="", current_task="")
        self.stdout.write(self.style.SUCCESS(
            "Done. Every folder will be queued and processed again on the supervisor's next scan."))

    # ---------------------------------------------------------------- some folders
    def reset_folders(self, names: list[str], yes: bool):
        match = Q()
        for n in names:
            match |= Q(path=str(Path(n).resolve())) if "/" in n else Q(path__endswith=f"/{n}")
        items = WorkItem.objects.filter(match)
        busy = list(items.filter(status=WorkItem.Status.IN_PROGRESS).values_list("path", flat=True))
        items = items.exclude(status=WorkItem.Status.IN_PROGRESS)
        paths = list(items.values_list("path", flat=True))
        folders = Folder.objects.filter(path__in=paths)
        for p in busy:
            self.stdout.write(self.style.WARNING(f"Skipped (a bot is processing it right now): {p}"))
        if not paths:
            raise CommandError("No processed folder matches " + ", ".join(names)
                               + ". Use the folder name (ocr_4393072) or its full path.")
        self.stdout.write(f"Will reprocess {len(paths)} folder(s) ("
                          f"{folders.count()} result(s), {Image.objects.filter(folder__in=folders).count()} image row(s)):")
        for p in paths[:20]:
            self.stdout.write(f"  {p}")
        if len(paths) > 20:
            self.stdout.write(f"  … and {len(paths) - 20} more")
        self.confirm("Delete these results?", yes)
        with transaction.atomic():
            folders.delete()                                   # their Image rows go with them
            IntakeFolder.objects.filter(path__in=paths).delete()
            items.delete()
        self.stdout.write(self.style.SUCCESS(
            "Done. They will be queued and processed again on the supervisor's next scan."))
