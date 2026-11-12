#!/usr/bin/env python
"""Django's command-line utility for administrative tasks.

Add --env staging or --env production to any command to use that environment's
database (see anpr_web/env.py). Without --env you get your local database.
"""
import os
import sys


def main():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "anpr_web.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    from anpr_web.env import take_env_option
    take_env_option(sys.argv)          # handles --env before Django reads the arguments
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
