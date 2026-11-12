"""Assembly line, step 2: put rows registered before the assembly line into their line."""
from django.db import migrations
from django.db.models import OuterRef, Subquery

# old IntakeFolder.status -> new IntakeFolder.stage
STATUS_TO_STAGE = {"new": "reading", "processing": "reading", "done": "done", "failed": "failed"}


def place_existing(apps, schema_editor):
    IntakeFolder = apps.get_model("core", "IntakeFolder")
    IntakeImage = apps.get_model("core", "IntakeImage")
    for status, stage in STATUS_TO_STAGE.items():
        IntakeFolder.objects.filter(status=status).update(stage=stage)
    # images take their folder's priority; duplicates never need processing
    IntakeImage.objects.update(priority=Subquery(
        IntakeFolder.objects.filter(pk=OuterRef("folder_id")).values("priority")[:1]))
    IntakeImage.objects.filter(is_duplicate=True).update(stage="skipped")
    # images of finished folders don't go down the line again
    IntakeImage.objects.filter(folder__stage__in=("done", "failed"), is_duplicate=False).update(stage="read")


class Migration(migrations.Migration):
    dependencies = [("core", "0008_assembly_line")]
    operations = [migrations.RunPython(place_existing, migrations.RunPython.noop)]
