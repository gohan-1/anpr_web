"""Assembly line, step 3: IntakeFolder.status is replaced by IntakeFolder.stage."""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("core", "0009_assembly_line_data")]
    operations = [migrations.RemoveField(model_name="intakefolder", name="status")]
