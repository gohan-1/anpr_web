"""
Chooses which environment Django runs against: local, staging or production.

HOW YOU PICK ONE (first match wins):
  1. the --env option:          python web/manage.py runserver --env staging
  2. the ANPR_ENV variable:     ANPR_ENV=staging python web/manage.py runserver
  3. otherwise:                 local

Each environment's settings live in envs/.env.<name> (e.g. envs/.env.staging).
Those files hold passwords, so they are in .gitignore and must NEVER be committed.
Copy the matching .example file to create one.
"""
import os
import sys
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENVS_DIR = PROJECT_ROOT / "envs"
ENVIRONMENTS = ("local", "staging", "production")
ALIASES = {"dev": "local", "stage": "staging", "prod": "production"}


def take_env_option(argv: list[str]) -> None:
    """Removes "--env X" (or "--env=X") from the command line and stores X in ANPR_ENV.

    WHY remove it: Django's own commands (runserver, migrate...) don't know --env and
    would complain about an unknown option.
    """
    for i, arg in enumerate(list(argv)):
        if arg == "--env" and i + 1 < len(argv):
            os.environ["ANPR_ENV"] = argv[i + 1]
            del argv[i:i + 2]
            return
        if arg.startswith("--env="):
            os.environ["ANPR_ENV"] = arg.split("=", 1)[1]
            del argv[i]
            return


def load_environment() -> str:
    name = os.environ.get("ANPR_ENV", "local").strip().lower()
    name = ALIASES.get(name, name)
    if name not in ENVIRONMENTS:
        raise ImproperlyConfigured(f"Unknown environment '{name}'. Use one of: {', '.join(ENVIRONMENTS)}.")
    env_file = ENVS_DIR / f".env.{name}"
    if env_file.exists():
        # override=False: variables already set in the real environment win. On a server,
        # Docker or the hosting platform sets them, and no env file is needed at all.
        load_dotenv(env_file, override=False)
    elif name != "local":
        raise ImproperlyConfigured(
            f"No settings file for '{name}'. Create {env_file} by copying envs/.env.{name}.example "
            f"and filling in the database address and secret key.")
    os.environ["ANPR_ENV"] = name
    return name


def env_bool(key: str, default: bool = False) -> bool:
    value = os.environ.get(key)
    return default if value is None else value.strip().lower() in ("1", "true", "yes", "on")


def env_list(key: str, default: str = "") -> list[str]:
    return [item.strip() for item in os.environ.get(key, default).split(",") if item.strip()]


# Make "--env" work for every program that loads Django settings, not only manage.py.
take_env_option(sys.argv)
