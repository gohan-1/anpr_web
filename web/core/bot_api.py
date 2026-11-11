"""
API for managing bots and watching the queue.

A ViewSet bundles all the endpoints for one kind of thing. ModelViewSet gives us,
for free:  GET /bots/ (list), POST /bots/ (create), GET/PATCH/DELETE /bots/<id>/.
@action adds our own extra endpoints, like POST /bots/<id>/start/.
"""
from pathlib import Path

from django.conf import settings
from rest_framework import status, viewsets
from rest_framework.decorators import action, api_view
from rest_framework.response import Response

from . import queue
from .models import Bot, BotEvent
from .serializers import BotEventSerializer, BotSerializer


class BotViewSet(viewsets.ModelViewSet):
    queryset = Bot.objects.order_by("name")
    serializer_class = BotSerializer

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


@api_view(["GET"])
def queue_status(request):
    return Response(queue.queue_stats())


@api_view(["POST"])
def queue_add(request):
    """POST {"path": "data"}: queues every sub-folder of that path on the SERVER's disk.
    Relative paths start from the anpr-project folder."""
    raw = str(request.data.get("path", "")).strip()
    if not raw:
        return Response({"detail": "Type the folder path to queue."}, status=status.HTTP_400_BAD_REQUEST)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = settings.PROJECT_ROOT / path
    path = path.resolve()
    if not path.is_dir():
        return Response({"detail": f"Folder not found on the server: {path}"}, status=status.HTTP_400_BAD_REQUEST)
    return Response({"added": queue.enqueue_folders(path), "path": str(path)})


@api_view(["POST"])
def queue_retry_failed(request):
    return Response({"requeued": queue.retry_failed()})
