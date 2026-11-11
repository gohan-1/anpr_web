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

from .models import Bot, BotEvent, WorkItem  # noqa: E402


class BotSerializer(serializers.ModelSerializer):
    # write_only: React can SEND a password, but the API never sends it back.
    password = serializers.CharField(write_only=True, required=False, style={"input_type": "password"})
    heartbeat_age = serializers.SerializerMethodField()
    healthy = serializers.SerializerMethodField()
    per_minute = serializers.SerializerMethodField()
    in_progress = serializers.SerializerMethodField()

    class Meta:
        model = Bot
        fields = ["id", "name", "api_username", "password", "concurrency", "rate_limit", "input_dir",
        "desired_running", "delete_requested", "state", "pid", "host", "started_at",
        "last_heartbeat", "heartbeat_age", "healthy", "current_task", "in_progress",
        "processed", "failed", "per_minute", "last_error", "restarts", "created_at"]
        # Fields only the bot/supervisor may change:
        read_only_fields = ["desired_running", "delete_requested", "state", "pid", "host", "started_at",
                            "last_heartbeat", "current_task", "processed", "failed", "last_error",
                            "restarts", "created_at"]

    def validate_concurrency(self, value):
        limit = load_config()["bots"]["max_concurrency"]
        if not 1 <= value <= limit:
            raise serializers.ValidationError(f"Use a number from 1 to {limit}.")
        return value

    def validate_input_dir(self, value):
        """Optional. Must exist on the server, and no two bots may watch the same folder."""
        from .queue import resolve_dir
        value = value.strip()
        if not value:
            return ""
        path = resolve_dir(value)
        if not path.is_dir():
            raise serializers.ValidationError(f"Folder {path} does not exist on the server.")
        others = Bot.objects.exclude(input_dir="").exclude(pk=getattr(self.instance, "pk", None))
        for other in others:
            if resolve_dir(other.input_dir) == path:
                raise serializers.ValidationError(f"Bot {other.name} already watches this folder.")
        return str(path)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Creating a bot (no existing instance): the password is required, so DRF reports a
        # missing password together with any other field errors, all in one response.
        # Editing a bot: leave it out to keep the current password.
        if self.instance is None:
            self.fields["password"].required = True

    def create(self, data):
        password = data.pop("password")
        bot = Bot(**data)
        bot.set_password(password)          # encrypted before saving
        bot.save()
        return bot

    def update(self, bot, data):
        if password := data.pop("password", None):
            bot.set_password(password)
        return super().update(bot, data)

    def get_heartbeat_age(self, bot):
        return None if bot.last_heartbeat is None else round((timezone.now() - bot.last_heartbeat).total_seconds())

    def get_healthy(self, bot):
        age = self.get_heartbeat_age(bot)
        return bot.state == Bot.State.RUNNING and age is not None and age <= load_config()["bots"]["stale_after_seconds"]

    def get_per_minute(self, bot):
        from .queue import folders_per_minute
        return folders_per_minute(WorkItem.objects.filter(bot_name=bot.name))

    def get_in_progress(self, bot):
        return [line for line in bot.current_task.splitlines() if line]


class BotEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = BotEvent
        fields = ["id", "created_at", "level", "message"]
