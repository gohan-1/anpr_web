"""
Deletes processed results so folders go down the assembly line again. Bots, their API
accounts, stage settings and user accounts are KEPT. Image files on disk are never touched.

    python manage.py reset_results                      # ALL results       (make reset)
    python manage.py reset_results --folder ocr_4393072 --folder ocr_4393075
                                                        # only these folders (make reprocess FOLDER=…)
    add --yes to skip the "are you sure?" question

Why a folder is processed again afterwards: a folder enters the line when a reader finds
it and it isn't registered yet. Deleting its registration (and its old result) makes the
next reader scan register it again, and its images start again at the first stage.
"""
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Q

from core.models import Bot, BotEvent, Folder, Image, IntakeFolder, IntakeImage, Job, StageMinute
from core.safety import confirm_environment, require_writable

WORKING = ("llm_running", "trip_running")                 # folder stages a bot holds right now


class Command(BaseCommand):
    help = "Delete processed results so folders are processed again (bots, settings and users are kept)"

    def add_arguments(self, parser):
        parser.add_argument("--folder", action="append", default=[],
                            help="folder name or full path; repeat for several. Without it: ALL results")
        parser.add_argument("--yes", action="store_true", help="don't ask for confirmation")

    def handle(self, *args, **o):
        require_writable("delete results")
        confirm_environment("delete processed results", assume_yes=o["yes"], stdout=self.stdout)
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
        models = [Image, Folder, BotEvent, Job, IntakeImage, IntakeFolder, StageMinute]
        counts = {m.__name__: m.objects.count() for m in models}
        self.stdout.write("Will delete: " + ", ".join(f"{n} {name}" for name, n in counts.items()))
        self.stdout.write(f"Will keep: {Bot.objects.count()} bot(s) (counters reset to 0), "
                          "stage settings and all user logins.")
        if Bot.objects.filter(state=Bot.State.RUNNING).exists():
            self.stdout.write(self.style.WARNING(
                "Bots are running. Stop everything first (make stop), or their results will be saved after the reset."))
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
            "Done. Every folder is registered again by the next reader scan and goes down the whole line."))

    # ---------------------------------------------------------------- some folders
    def reset_folders(self, names: list[str], yes: bool):
        match = Q()
        for n in names:
            match |= Q(path=str(Path(n).resolve())) if "/" in n else Q(name=n)
        found = IntakeFolder.objects.filter(match)
        busy_q = Q(stage__in=WORKING) | Q(images__stage__in=("preprocessing", "ocr_running"))
        busy = list(found.filter(busy_q).values_list("path", flat=True).distinct())
        folders = found.exclude(id__in=found.filter(busy_q).values("id"))
        paths = list(folders.values_list("path", flat=True))
        for p in busy:
            self.stdout.write(self.style.WARNING(f"Skipped (a bot is working on it right now): {p}"))
        if not paths:
            raise CommandError("No registered folder matches " + ", ".join(names)
                               + ". Use the folder name (ocr_4393072) or its full path.")
        results = Folder.objects.filter(path__in=paths)
        self.stdout.write(f"Will process {len(paths)} folder(s) again ({results.count()} result(s), "
                          f"{IntakeImage.objects.filter(folder__in=folders).count()} image(s)):")
        for p in paths[:20]:
            self.stdout.write(f"  {p}")
        if len(paths) > 20:
            self.stdout.write(f"  … and {len(paths) - 20} more")
        self.confirm("Delete these results?", yes)
        with transaction.atomic():
            results.delete()                                   # their Image rows go with them
            folders.delete()                                   # their IntakeImage rows go with them
        self.stdout.write(self.style.SUCCESS(
            "Done. The next reader scan registers them again and they go down the whole line."))
