"""Django's flush (deletes ALL data). Allowed only on your local database."""
from django.conf import settings
from django.core.management.base import CommandError
from django.core.management.commands.flush import Command as DjangoFlush


class Command(DjangoFlush):
    def handle(self, *args, **options):
        if not settings.IS_LOCAL:
            raise CommandError(f"flush deletes everything, so it only works on local, not on {settings.ANPR_ENV}.")
        return super().handle(*args, **options)
