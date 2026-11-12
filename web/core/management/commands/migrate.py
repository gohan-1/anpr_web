"""Django's migrate, plus a safety check on staging/production and in read-only mode."""
from django.core.management.commands.migrate import Command as DjangoMigrate

from core.safety import add_yes_option, confirm_environment, require_writable


class Command(DjangoMigrate):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        add_yes_option(parser)

    def handle(self, *args, **options):
        require_writable("run migrations")
        confirm_environment("run database migrations", assume_yes=options["yes"],
                            interactive=options["interactive"], stdout=self.stdout)
        return super().handle(*args, **options)
