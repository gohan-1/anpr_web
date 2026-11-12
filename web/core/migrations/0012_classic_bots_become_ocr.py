"""Data step: classic bots become OCR bots (keeping their API account and rate limit), and
images "read" by the old OCR placeholder (no plate, no reader) go back to the OCR line so
they are read for real. Their folders are reopened, so they are decided again."""
from django.db import migrations


def forwards(apps, schema_editor):
    Bot = apps.get_model("core", "Bot")
    IntakeImage = apps.get_model("core", "IntakeImage")
    IntakeFolder = apps.get_model("core", "IntakeFolder")
    Bot.objects.filter(stage="legacy").update(stage="ocr", shard="")
    fake = IntakeImage.objects.filter(stage="read", read_by="")
    folder_ids = set(fake.values_list("folder_id", flat=True))
    fake.update(stage="cleaned", attempts=0)
    IntakeFolder.objects.filter(id__in=folder_ids).update(
        stage="reading", route_reason="", finished_at=None, taken_by="", taken_at=None, attempts=0)


class Migration(migrations.Migration):
    dependencies = [("core", "0011_stage_bots_only")]
    operations = [migrations.RunPython(forwards, migrations.RunPython.noop)]
