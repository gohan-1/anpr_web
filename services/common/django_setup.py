"""
Lets a microservice use the Django models (and the --env / envs/ settings).

WHY: every service shares one PostgreSQL database and one set of migrations, owned
by the Django project in web/. Instead of defining the tables twice, a service
loads Django's settings and uses the same models. Import this FIRST in a service.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]          # the anpr-project folder
for path in (ROOT, ROOT / "web"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "anpr_web.settings")

import django  # noqa: E402
from django.apps import apps  # noqa: E402

if not apps.ready:
    django.setup()
