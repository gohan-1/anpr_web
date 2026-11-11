from django.contrib import admin

from .models import Bot, BotEvent, Folder, Image, IntakeFolder, IntakeImage, Job, WorkItem


class ImageInline(admin.TabularInline):
    model = Image
    extra = 0
    fields = ("file_name", "quality", "plate", "confidence", "final_score", "is_duplicate", "is_corrupt", "api_error")
    readonly_fields = fields
    can_delete = False


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ("id", "input_path", "status", "folder_count", "started_at", "finished_at")
    list_filter = ("status",)


@admin.register(Folder)
class FolderAdmin(admin.ModelAdmin):
    list_display = ("name", "route", "scenario", "final_plate", "final_score", "reviewed_plate", "job")
    list_filter = ("route", "scenario", "job")
    search_fields = ("name", "final_plate", "reviewed_plate")
    readonly_fields = ("job", "path", "scenario", "reason", "final_plate", "final_score", "best_image", "candidates")
    inlines = [ImageInline]


@admin.register(Image)
class ImageAdmin(admin.ModelAdmin):
    list_display = ("file_name", "folder", "plate", "confidence", "quality", "final_score", "is_duplicate", "is_corrupt")
    list_filter = ("is_duplicate", "is_corrupt")
    search_fields = ("file_name", "plate", "folder__name")


@admin.register(Bot)
class BotAdmin(admin.ModelAdmin):
    list_display = ("name", "state", "desired_running", "concurrency", "processed", "failed", "last_heartbeat", "host")
    list_filter = ("state", "desired_running")
    exclude = ("api_password_encrypted",)   # never show the encrypted password


@admin.register(WorkItem)
class WorkItemAdmin(admin.ModelAdmin):
    list_display = ("path", "status", "bot_name", "attempts", "claimed_at", "finished_at")
    list_filter = ("status", "bot_name")
    search_fields = ("path",)


@admin.register(BotEvent)
class BotEventAdmin(admin.ModelAdmin):
    list_display = ("created_at", "bot", "level", "message")
    list_filter = ("level", "bot")


class IntakeImageInline(admin.TabularInline):
    model = IntakeImage
    extra = 0
    fields = ("file_name", "size_bytes", "sha256", "is_duplicate", "modified_at")
    readonly_fields = fields
    can_delete = False


@admin.register(IntakeFolder)
class IntakeFolderAdmin(admin.ModelAdmin):
    list_display = ("name", "source", "accuracy", "priority", "status", "image_count", "discovered_at", "discovered_by")
    list_filter = ("status", "accuracy", "source")
    search_fields = ("name", "path")
    inlines = [IntakeImageInline]