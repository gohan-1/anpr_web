"""
API for managing bots and the assembly line (the Bots and Pipeline pages).

A ViewSet bundles all the endpoints for one kind of thing. ModelViewSet gives us,
for free:  GET /bots/ (list), POST /bots/ (create), GET/PATCH/DELETE /bots/<id>/.
@action adds our own extra endpoints, like POST /bots/<id>/start/.
"""
from rest_framework import status, viewsets
from rest_framework.decorators import action, api_view
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from pipeline.config import load_config
from pipeline.stages import BOT_STAGES, STAGE_LABELS, STAGES

from . import stage_queue
from .models import Bot, BotEvent
from .serializers import BotEventSerializer, BotSerializer


class BotPages(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"      # the Bots page asks for all of them at once
    max_page_size = 1000


class BotViewSet(viewsets.ModelViewSet):
    queryset = Bot.objects.order_by("name")
    serializer_class = BotSerializer
    pagination_class = BotPages

    def get_queryset(self):
        qs = super().get_queryset()
        if stage := self.request.query_params.get("stage"):      # GET /api/bots/?stage=ocr
            qs = qs.filter(stage=stage)
        return qs

    @action(detail=True, methods=["post"])
    def start(self, request, pk=None):
        bot = self.get_object()
        if bot.delete_requested:
            return Response({"detail": "This bot is being removed."}, status=status.HTTP_409_CONFLICT)
        bot.desired_running = True
        if bot.state == Bot.State.CRASHED:
            bot.state = Bot.State.STOPPED          # a manual Start clears the crashed flag
        bot.save(update_fields=["desired_running", "state"])
        BotEvent.objects.create(bot=bot, message=f"Start requested by {request.user.get_username()}")
        return Response(self.get_serializer(bot).data)

    @action(detail=True, methods=["post"])
    def stop(self, request, pk=None):
        bot = self.get_object()
        bot.desired_running = False
        bot.save(update_fields=["desired_running"])
        BotEvent.objects.create(bot=bot, message=f"Stop requested by {request.user.get_username()}")
        return Response(self.get_serializer(bot).data)

    def destroy(self, request, *args, **kwargs):
        """Deleting doesn't remove the row immediately: the bot first finishes its folders,
        then the supervisor deletes it. A bot that isn't running is removed right away."""
        bot = self.get_object()
        if bot.state in (Bot.State.STOPPED, Bot.State.CRASHED) and not bot.desired_running:
            bot.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)
        bot.delete_requested, bot.desired_running = True, False
        bot.save(update_fields=["delete_requested", "desired_running"])
        BotEvent.objects.create(bot=bot, message=f"Removal requested by {request.user.get_username()}")
        return Response(self.get_serializer(bot).data, status=status.HTTP_202_ACCEPTED)

    @action(detail=True, methods=["get"])
    def events(self, request, pk=None):
        events = self.get_object().events.all()[:50]
        return Response(BotEventSerializer(events, many=True).data)


# ---------------------------------------------------------------- assembly line
def _stage_bots(stage: str):
    return Bot.objects.filter(stage=stage).order_by("name")


@api_view(["GET"])
def pipeline_overview(request):
    """Everything the Pipeline page shows: per stage, its line, its settings and its bots."""
    return Response(_overview())


def _overview() -> dict:
    stats = stage_queue.line_stats()
    stages = []
    for name in BOT_STAGES:
        bots = list(_stage_bots(name))
        active = [b for b in bots if b.desired_running and not b.delete_requested]
        settings_now = stage_queue.settings_of(name)
        defaults = load_config().get("stages", {}).get(name, {})
        stages.append({
            "stage": name,
            "label": STAGE_LABELS[name],
            "level": STAGES[name].level if name in STAGES else "folder",
            **stats["stages"][name],
            "failed": _failed_count(name),
            "bots_wanted": len(active),
            "bots_running": sum(b.state == Bot.State.RUNNING for b in bots),
            "settings": settings_now,
            "editable": {k: {"value": settings_now.get(k), "default": defaults.get(k),
                             "min": lo, "max": hi, "decimal": isinstance(lo, float)}
                         for k, (lo, hi) in stage_queue.EDITABLE_SETTINGS.items() if k in settings_now},
            "bots": BotSerializer(bots, many=True).data,
        })
    cfg = load_config()
    return {
        "stages": stages,
        "images": stats["images"],
        "folders": stats["folders"],
        "sources": [{k: src.get(k) for k in ("name", "path", "accuracy", "priority")}
                    for src in cfg.get("reader", {}).get("sources", [])],
        "use_prefect": bool(cfg.get("pipeline", {}).get("use_prefect")),
        "ocr_api_mock": bool(cfg.get("api", {}).get("mock", True)),    # real API: OCR bots need an account
        "max_bots_per_stage": cfg["bots"].get("max_bots_per_stage", 50),
    }


def _failed_count(stage: str) -> int:
    from .models import IntakeFolder, IntakeImage
    if stage not in STAGES:
        return 0
    Model = IntakeImage if STAGES[stage].level == "image" else IntakeFolder
    return Model.objects.filter(stage="failed", failed_in=stage).count()


@api_view(["PATCH"])
def pipeline_settings(request, stage):
    """PATCH {"batch_size": 50, "parallel": null}: change a stage's settings (null = back to default).
    Bots pick the change up before their next batch."""
    if stage not in BOT_STAGES:
        return Response({"detail": f"Unknown stage '{stage}'."}, status=status.HTTP_404_NOT_FOUND)
    if not isinstance(request.data, dict) or not request.data:
        return Response({"detail": "Send the settings to change."}, status=status.HTTP_400_BAD_REQUEST)
    try:
        values = stage_queue.save_settings(stage, dict(request.data), request.user.get_username())
    except ValueError as exc:
        return Response(exc.args[0], status=status.HTTP_400_BAD_REQUEST)
    return Response({"stage": stage, "settings": values})


@api_view(["GET"])
def pipeline_failed(request):
    """GET ?level=image|folder: the newest failed items with their error."""
    level = request.query_params.get("level", "image")
    if level not in ("image", "folder"):
        return Response({"detail": "level must be image or folder."}, status=status.HTTP_400_BAD_REQUEST)
    return Response(stage_queue.failed_items(level, limit=200))


@api_view(["POST"])
def pipeline_retry(request):
    """POST {"level": "image", "ids": [1, 2]}: failed items go back to the line they failed in.
    Without ids: every failed item of that level."""
    level = request.data.get("level")
    ids = request.data.get("ids")
    if level not in ("image", "folder"):
        return Response({"detail": "level must be image or folder."}, status=status.HTTP_400_BAD_REQUEST)
    if ids is not None and (not isinstance(ids, list) or not all(isinstance(i, int) for i in ids)):
        return Response({"detail": "ids must be a list of numbers."}, status=status.HTTP_400_BAD_REQUEST)
    return Response(stage_queue.retry(level, ids))


@api_view(["POST"])
def pipeline_scale(request, stage):
    """POST {"count": 4}: make this stage have exactly 4 bots.
    More: new bots are added (stage-1, stage-2...). Fewer: the newest are removed,
    each finishing its current batch first."""
    if stage not in BOT_STAGES:
        return Response({"detail": f"Unknown stage '{stage}'."}, status=status.HTTP_404_NOT_FOUND)
    try:
        count = int(request.data.get("count"))
    except (TypeError, ValueError):
        return Response({"detail": "Send the number of bots, e.g. {\"count\": 4}."}, status=status.HTTP_400_BAD_REQUEST)
    limit = load_config()["bots"]["max_bots_per_stage"]
    if not 0 <= count <= limit:
        return Response({"detail": f"Use a number from 0 to {limit}."}, status=status.HTTP_400_BAD_REQUEST)

    active = [b for b in _stage_bots(stage) if not b.delete_requested]
    user = request.user.get_username()
    if count > len(active):
        names = set(Bot.objects.values_list("name", flat=True))
        n = 1
        for _ in range(count - len(active)):
            while f"{stage}-{n}" in names:
                n += 1
            bot = Bot.objects.create(name=f"{stage}-{n}", stage=stage, desired_running=True)
            # (an OCR bot added with + has no API account yet: fine with the mock API; for the
            # real one, set its account on the Bots page, until then it waits instead of failing)
            names.add(bot.name)
            BotEvent.objects.create(bot=bot, message=f"Added by {user}")
    elif count < len(active):
        # remove the newest first; a bot that isn't running is deleted straight away
        for bot in sorted(active, key=lambda b: b.created_at, reverse=True)[:len(active) - count]:
            if bot.state in (Bot.State.STOPPED, Bot.State.CRASHED) and not bot.pid:
                bot.delete()
            else:
                bot.delete_requested, bot.desired_running = True, False
                bot.save(update_fields=["delete_requested", "desired_running"])
                BotEvent.objects.create(bot=bot, message=f"Removal requested by {user}")
    return Response(_overview())