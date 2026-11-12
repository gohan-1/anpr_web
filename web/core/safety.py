"""
Guards for commands that change a database.

  require_writable()      -> refuses to run in read-only mode
  confirm_environment()   -> on staging/production, asks you to type the environment's name

WHY typing the name: pressing "y" becomes a habit; typing "production" makes you
stop and read which database you're about to change.
"""
from django.conf import settings
from django.core.management.base import CommandError


def require_writable(action: str) -> None:
    if settings.READ_ONLY:
        raise CommandError(
            f"Can't {action}: this connection to {settings.ANPR_ENV} is read-only "
            f"(ANPR_READ_ONLY=true in envs/.env.{settings.ANPR_ENV}).")


def confirm_environment(action: str, *, assume_yes: bool = False, interactive: bool = True,
                        stdout=None) -> None:
    if settings.IS_LOCAL or assume_yes:
        return
    if not interactive:
        raise CommandError(f"Refusing to {action} on {settings.ANPR_ENV} without confirmation. "
                           "Add --yes if you're sure (for example in a deployment script).")
    write = stdout.write if stdout else print
    write(f"\n  You are about to {action} on {settings.ANPR_ENV.upper()}.\n"
          f"  Database: {settings.DATABASE_LABEL}\n")
    answer = input(f"  Type '{settings.ANPR_ENV}' to continue: ").strip().lower()
    if answer != settings.ANPR_ENV:
        raise CommandError("Cancelled. Nothing was changed.")


def add_yes_option(parser) -> None:
    parser.add_argument("--yes", action="store_true",
                        help="skip the confirmation on staging/production (for scripts)")
