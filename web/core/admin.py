from django.contrib import admin

from .models import Bot, BotEvent, Folder, Image, IntakeFolder, IntakeImage, Job, StageMinute, StageSettings


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
    list_display = ("name", "stage", "shard", "state", "desired_running", "processed", "failed", "last_heartbeat", "host")
    list_filter = ("stage", "state", "desired_running")
    exclude = ("api_password_encrypted",)   # never show the encrypted password


@admin.register(StageSettings)
class StageSettingsAdmin(admin.ModelAdmin):
    list_display = ("stage", "values", "updated_at", "updated_by")


@admin.register(BotEvent)
class BotEventAdmin(admin.ModelAdmin):
    list_display = ("created_at", "bot", "level", "message")
    list_filter = ("level", "bot")


class IntakeImageInline(admin.TabularInline):
    model = IntakeImage
    extra = 0
    fields = ("file_name", "stage", "plate", "confidence", "read_by", "attempts", "is_duplicate", "last_error")
    readonly_fields = fields
    can_delete = False


@admin.register(IntakeFolder)
class IntakeFolderAdmin(admin.ModelAdmin):
    list_display = ("name", "stage", "plate", "confidence", "route_reason", "image_count", "discovered_at")
    list_filter = ("stage", "accuracy", "source")
    search_fields = ("name", "path")
    inlines = [IntakeImageInline]
    
# print(Route)
# @admin.register(Route)
# # class RouteAdmin(admin.ModelAdmin):
# #     # list_display = ("name", "description", "created_at", "updated_at")
# #     # search_fields = ("name", "description")
    
@admin.register(StageMinute)
class StageMinuteAdmin(admin.ModelAdmin):
    # One row per stage per minute: how many items it finished (done) and failed.
    list_display = ("stage", "minute", "done", "failed")
    list_filter = ("stage",)
    ordering = ("-minute",)
    
# @admin.register(StageSettings)
# class StageSettingsAdmin(admin.ModelAdmin):
#     list_display = ("stage", "settings", "updated_at", "updated_by")
#     list_filter = ("stage",)
    