"""
Django admin (http://127.0.0.1:8000/admin/): look at and fix any table by hand.

Day-to-day work happens in the web app (Bots, Pipeline, Folders pages); the admin is for
looking things up and repairs. Only tables (models) can be registered here: Route, for
example, is a list of choices used by Folder.route, not a table.
"""
from django.contrib import admin

from .models import Bot, BotEvent, Folder, Image, IntakeFolder, IntakeImage, Job, StageMinute, StageSettings


# ============================================================================ the assembly line
class IntakeImageInline(admin.TabularInline):
    model = IntakeImage
    extra = 0
    fields = ("file_name", "stage", "plate", "confidence", "read_by", "duration_ms", "attempts", "is_duplicate",
              "last_error")
    readonly_fields = fields
    can_delete = False
    show_change_link = True


@admin.register(IntakeFolder)
class IntakeFolderAdmin(admin.ModelAdmin):
    """A vehicle folder registered by a reader, and where it is on the line."""
    list_display = ("name", "stage", "plate", "confidence", "route_reason", "image_count", "discovered_at", "read_at")
    list_filter = ("stage", "accuracy", "source")
    search_fields = ("name", "path", "plate")
    date_hierarchy = "discovered_at"
    readonly_fields = ("discovered_at", "discovered_by", "read_at", "finished_at")
    inlines = [IntakeImageInline]


@admin.register(IntakeImage)
class IntakeImageAdmin(admin.ModelAdmin):
    """One image on the line: its stage, and what the OCR read."""
    list_display = ("file_name", "folder", "stage", "plate", "confidence", "read_by", "duration_ms", "attempts",
                    "failed_in")
    list_filter = ("stage", "failed_in", "read_by", "is_duplicate")
    search_fields = ("file_name", "plate", "folder__name")
    list_select_related = ("folder",)
    raw_id_fields = ("folder",)           # a drop-down of every folder would be far too long


@admin.register(StageSettings)
class StageSettingsAdmin(admin.ModelAdmin):
    """Stage settings changed on the Pipeline page (they override config/settings.yaml)."""
    list_display = ("stage", "values", "updated_at", "updated_by")
    readonly_fields = ("updated_at",)


@admin.register(StageMinute)
class StageMinuteAdmin(admin.ModelAdmin):
    """One row per stage per minute: items finished (done) and failed. The speeds on the Pipeline page."""
    list_display = ("stage", "minute", "done", "failed")
    list_filter = ("stage",)
    ordering = ("-minute",)
    date_hierarchy = "minute"


# ============================================================================ bots
class BotEventInline(admin.TabularInline):
    model = BotEvent
    extra = 0
    fields = ("created_at", "level", "message")
    readonly_fields = fields
    can_delete = False
    max_num = 0                           # read-only: events are written by the bots

    def get_queryset(self, request):
        return super().get_queryset(request).order_by("-created_at")


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = ("name", "stage", "shard", "state", "desired_running", "api_username", "rate_limit",
                    "processed", "failed", "last_heartbeat", "host")
    list_filter = ("stage", "state", "desired_running")
    search_fields = ("name", "api_username")
    # The password is never shown; set or change it on the Bots page (it is stored encrypted).
    exclude = ("api_password_encrypted",)
    # Reported by the bot and the supervisor: change "desired_running" to start/stop instead.
    readonly_fields = ("shard", "state", "pid", "host", "started_at", "last_heartbeat", "current_task",
                       "processed", "failed", "last_error", "restarts", "created_at")
    inlines = [BotEventInline]


@admin.register(BotEvent)
class BotEventAdmin(admin.ModelAdmin):
    list_display = ("created_at", "bot", "level", "message")
    list_filter = ("level", "bot")
    search_fields = ("message",)


# ============================================================================ results (Folders page)
class ImageInline(admin.TabularInline):
    model = Image
    extra = 0
    fields = ("file_name", "plate", "confidence", "duration_ms", "api_attempts", "is_duplicate", "api_error")
    readonly_fields = fields
    can_delete = False


@admin.register(Folder)
class FolderAdmin(admin.ModelAdmin):
    list_display = ("name", "route", "final_plate", "final_score", "processed_by", "reviewed_plate", "created_at")
    list_filter = ("route", "processed_by")
    search_fields = ("name", "final_plate", "reviewed_plate")
    date_hierarchy = "created_at"
    readonly_fields = ("job", "path", "scenario", "reason", "final_plate", "final_score", "best_image", "candidates",
                       "processed_by", "created_at")
    inlines = [ImageInline]


@admin.register(Image)
class ImageAdmin(admin.ModelAdmin):
    list_display = ("file_name", "folder", "plate", "confidence", "duration_ms", "api_attempts", "is_duplicate")
    list_filter = ("is_duplicate", "is_corrupt")
    search_fields = ("file_name", "plate", "folder__name")
    list_select_related = ("folder",)
    raw_id_fields = ("folder",)


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    """Runs of the old standalone pipeline (pipeline/flows.py); kept for its history."""
    list_display = ("id", "input_path", "status", "folder_count", "started_at", "finished_at")
    list_filter = ("status",)
