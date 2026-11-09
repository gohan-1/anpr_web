from django.contrib import admin

from .models import Folder, Image, Job


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
