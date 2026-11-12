"""
Serializers turn Django model objects into JSON (and back).

WHY: React can't read Python objects. A serializer lists which fields to send
and in what shape. Think of it as the "translator" between the database and React.
"""
from rest_framework import serializers

from pipeline.config import load_config

from .models import Folder, Image, Job


class ImageSerializer(serializers.ModelSerializer):
    # ModelSerializer reads the model and builds the fields for us.
    class Meta:
        model = Image
        fields = ["id", "file_name", "is_duplicate", "is_corrupt", "quality", "plate",
                  "confidence", "final_score", "api_error"]


class FolderListSerializer(serializers.ModelSerializer):
    """Small version for the folder table: no images, so the list stays fast."""
    route_label = serializers.CharField(source="get_route_display", read_only=True)
    plate = serializers.SerializerMethodField()

    class Meta:
        model = Folder
        fields = ["id", "name", "route", "route_label", "scenario", "plate", "final_score",
                  "reviewed_by", "created_at", "job"]

    def get_plate(self, folder):
        # A reviewer's correction wins over what the pipeline decided.
        return folder.reviewed_plate or folder.final_plate


class FolderDetailSerializer(FolderListSerializer):
    """Full version for the review screen: includes every image."""
    images = serializers.SerializerMethodField()
    min_quality = serializers.SerializerMethodField()   # so React can flag low-quality images

    class Meta(FolderListSerializer.Meta):
        fields = FolderListSerializer.Meta.fields + [
            "reason", "final_plate", "reviewed_plate", "reviewed_at", "best_image", "candidates",
            "images", "min_quality"]

    def get_min_quality(self, folder):
        return load_config()["decision"]["min_quality"]

    def get_images(self, folder):
        ordered = folder.images.order_by("is_corrupt", "is_duplicate", "-final_score", "file_name")
        return ImageSerializer(ordered, many=True).data


class JobSerializer(serializers.ModelSerializer):
    accepted = serializers.IntegerField(read_only=True)
    in_review = serializers.IntegerField(read_only=True)

    class Meta:
        model = Job
        fields = ["id", "input_path", "status", "folder_count", "accepted", "in_review",
                  "started_at", "finished_at"]


class ReviewActionSerializer(serializers.Serializer):
    """Checks the data React sends when a reviewer clicks a button."""
    action = serializers.ChoiceField(choices=["approve", "correct", "reject"])
    plate = serializers.CharField(required=False, allow_blank=True, default="")


# ============================================================================ bots
from django.utils import timezone  # noqa: E402

from .models import Bot, BotEvent  # noqa: E402


class BotSerializer(serializers.ModelSerializer):
    # write_only: React can SEND a password, but the API never sends it back.
    password = serializers.CharField(write_only=True, required=False, allow_blank=True,
                                     style={"input_type": "password"})
    heartbeat_age = serializers.SerializerMethodField()
    healthy = serializers.SerializerMethodField()
    has_account = serializers.SerializerMethodField()

    class Meta:
        model = Bot
        fields = ["id", "name", "stage", "shard", "api_username", "password", "has_account", "rate_limit",
                  "desired_running", "delete_requested", "state", "pid", "host", "started_at",
                  "last_heartbeat", "heartbeat_age", "healthy", "current_task",
                  "processed", "failed", "last_error", "restarts", "created_at"]
        # Fields only the bot/supervisor may change:
        read_only_fields = ["shard", "desired_running", "delete_requested", "state", "pid", "host", "started_at",
                            "last_heartbeat", "current_task", "processed", "failed", "last_error",
                            "restarts", "created_at"]

    def validate_stage(self, value):
        if self.instance is not None and value != self.instance.stage:
            raise serializers.ValidationError("A bot's stage can't change; remove it and add one to the other stage.")
        return value

    def validate_rate_limit(self, value):
        if value < 0:
            raise serializers.ValidationError("Use 0 (no limit) or more.")
        return value

    def validate(self, data):
        # An account is a username AND a password: never half of one.
        username = data.get("api_username", getattr(self.instance, "api_username", ""))
        has_password = bool(data.get("password")) or bool(getattr(self.instance, "api_password_encrypted", ""))
        if username and not has_password:
            raise serializers.ValidationError({"password": "Give the password for this API username."})
        if data.get("password") and not username:
            raise serializers.ValidationError({"api_username": "Give the API username for this password."})
        return data

    def create(self, data):
        password = data.pop("password", "")
        bot = Bot(**data)
        if password:
            bot.set_password(password)       # encrypted before saving
        bot.save()
        return bot

    def update(self, bot, data):
        password = data.pop("password", None)
        if password:
            bot.set_password(password)
        if data.get("api_username") == "":    # account removed
            bot.api_password_encrypted = ""
        return super().update(bot, data)

    def get_heartbeat_age(self, bot):
        return None if bot.last_heartbeat is None else round((timezone.now() - bot.last_heartbeat).total_seconds())

    def get_healthy(self, bot):
        age = self.get_heartbeat_age(bot)
        return bot.state == Bot.State.RUNNING and age is not None and age <= load_config()["bots"]["stale_after_seconds"]

    def get_has_account(self, bot):
        return bool(bot.api_username and bot.api_password_encrypted)


class BotEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = BotEvent
        fields = ["id", "created_at", "level", "message"]