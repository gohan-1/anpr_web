"""
API documentation: the request/response shapes and sample bodies shown in Swagger
(/api/docs/) and ReDoc (/api/redoc/).

WHY a separate file: most of our views are plain functions (@api_view), and the schema
generator can't guess what they read and return. Each view gets one decorator from here,
e.g. @docs.LOGIN, so the views themselves stay short and readable.
Nothing here changes how the API behaves; it only describes it.
"""
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import (OpenApiExample, OpenApiParameter, OpenApiResponse, extend_schema,
                                   extend_schema_view, inline_serializer)
from rest_framework import serializers

from pipeline.stages import BOT_STAGES

from .serializers import BotEventSerializer, BotSerializer, FolderDetailSerializer, ReviewActionSerializer

STAGE_CHOICES = list(BOT_STAGES)          # reader, preprocess, ocr, llm, trip

ERROR = inline_serializer("Error", {"detail": serializers.CharField()})
STAGE_PATH = OpenApiParameter("stage", str, OpenApiParameter.PATH, enum=STAGE_CHOICES,
                              description="Stage of the assembly line")
NO_LOGIN = []                             # endpoints that work without a token


def _ex(name, value, summary=None, request=False, response=False, status=None):
    return OpenApiExample(name, value=value, summary=summary, request_only=request, response_only=response,
                          status_codes=[str(status)] if status else None)


# ============================================================================ Auth
LOGIN = extend_schema(
    tags=["Auth"], auth=NO_LOGIN, summary="Log in and get a token",
    description="Send the token back in every request as `Authorization: Token <token>`.",
    request=inline_serializer("LoginRequest", {"username": serializers.CharField(),
                                               "password": serializers.CharField()}),
    responses={200: inline_serializer("LoginResponse", {"token": serializers.CharField(),
                                                        "username": serializers.CharField()}),
               400: OpenApiResponse(ERROR, description="Wrong username or password")},
    examples=[_ex("Login", {"username": "operator", "password": "your-password"}, request=True),
              _ex("Token", {"token": "9944b09199c62bcf9418ad846dd0e4bbdfc6ee4b", "username": "operator"},
                  response=True, status=200),
              _ex("Wrong password", {"detail": "That username and password don't match."}, response=True, status=400)])

LOGOUT = extend_schema(tags=["Auth"], summary="Log out (the token stops working)", request=None,
                       responses={204: OpenApiResponse(description="Logged out")})

ME = extend_schema(
    tags=["Auth"], summary="Who am I",
    responses=inline_serializer("Me", {"username": serializers.CharField(),
                                       "review_count": serializers.IntegerField(help_text="folders waiting for review")}),
    examples=[_ex("Me", {"username": "operator", "review_count": 0}, response=True)])

# ============================================================================ Dashboard
ENVIRONMENT = extend_schema(
    tags=["Dashboard"], auth=NO_LOGIN, summary="Which environment and database (never a password)",
    responses=inline_serializer("Environment", {"env": serializers.ChoiceField(["local", "staging", "production"]),
                                                "read_only": serializers.BooleanField(),
                                                "database": serializers.CharField()}),
    examples=[_ex("Local", {"env": "local", "read_only": False, "database": "PostgreSQL localhost/anpr"}, response=True)])

STATS = extend_schema(
    tags=["Dashboard"], summary="Everything the Overview charts need",
    parameters=[OpenApiParameter("job", int, description="only this old pipeline run")],
    responses=OpenApiTypes.OBJECT,
    examples=[_ex("Stats", {
        "total": 143, "routes": {"processed": 140, "accepted": 0, "review": 0, "no_plate": 0, "failed": 3},
        "scenarios": [], "daily": [{"day": "2026-11-12", "processed": 140, "accepted": 0, "review": 0,
                                    "no_plate": 0, "failed": 3}],
        "score_bins": [{"bin": "0.85", "processed": 12, "accepted": 0, "review": 0, "no_plate": 0, "failed": 0}],
        "scatter": [{"confidence": 0.91, "quality": None, "route": "processed"}],
        "thresholds": {"accept_score": 0.85, "min_quality": 0.4}, "reviewed_today": 0, "jobs": []}, response=True)])

# ============================================================================ Folders
FOLDER_LIST = extend_schema(
    tags=["Folders"], summary="Folder results (Folders page), newest first",
    parameters=[OpenApiParameter("q", str, description="folder name or plate contains"),
                OpenApiParameter("route", str, description="comma-separated, e.g. processed,failed"),
                OpenApiParameter("scenario", str, description="comma-separated scenario codes"),
                OpenApiParameter("page", int)])

FOLDER_DETAIL = extend_schema(tags=["Folders"], summary="One folder with all its images")

REVIEW_NEXT = extend_schema(
    tags=["Folders"], summary="The oldest folder waiting for review",
    responses={200: FolderDetailSerializer, 204: OpenApiResponse(description="Nothing waiting for review")})

REVIEW = extend_schema(
    tags=["Folders"], summary="Approve, correct or reject a folder's plate",
    request=ReviewActionSerializer,
    responses={200: inline_serializer("ReviewResult", {"message": serializers.CharField(),
                                                       "folder": FolderDetailSerializer()}),
               400: OpenApiResponse(ERROR, description="e.g. the corrected plate has the wrong format")},
    examples=[_ex("Approve", {"action": "approve"}, request=True),
              _ex("Correct", {"action": "correct", "plate": "MH12AB1234"}, request=True),
              _ex("Reject", {"action": "reject"}, request=True)])

IMAGE_FILE = extend_schema(
    tags=["Folders"], summary="The image file itself",
    responses={(200, "image/*"): OpenApiTypes.BINARY, 404: OpenApiResponse(description="File not available")})

# ============================================================================ Bots
_BOT_REQ_EXAMPLES = [
    _ex("OCR bot with its API account", {"name": "ocr-gate1", "stage": "ocr", "api_username": "gate1-account",
                                         "password": "api-password", "rate_limit": 2}, request=True),
    _ex("Reader bot", {"name": "reader-1", "stage": "reader"}, request=True),
    _ex("Trip bot", {"name": "trip-1", "stage": "trip"}, request=True),
]
_BOT_RESPONSE = {
    "id": 7, "name": "ocr-gate1", "stage": "ocr", "shard": "", "api_username": "gate1-account", "has_account": True,
    "rate_limit": 2.0, "desired_running": True, "delete_requested": False, "state": "running", "pid": 104233,
    "host": "anpr-server", "started_at": "2026-11-13T09:12:04Z", "last_heartbeat": "2026-11-13T09:20:31Z",
    "heartbeat_age": 1, "healthy": True, "current_task": "Batch 12: 20 items", "processed": 240, "failed": 0,
    "last_error": "", "restarts": 0, "created_at": "2026-11-13T09:11:58Z"}

BOTS = extend_schema_view(
    list=extend_schema(tags=["Bots"], summary="All bots", parameters=[
        OpenApiParameter("stage", str, enum=STAGE_CHOICES, description="only bots of this stage"),
        OpenApiParameter("page_size", int, description="bots per page (default 50, max 1000)")]),
    create=extend_schema(tags=["Bots"], summary="Create a bot (switched off)", examples=_BOT_REQ_EXAMPLES + [
        _ex("Created", _BOT_RESPONSE | {"desired_running": False, "state": "stopped", "pid": None, "processed": 0},
            response=True, status=201)],
        description="OCR bots call the OCR API with their own account (`api_username` + `password`, stored "
                    "encrypted, never returned). The account is optional while the API is mocked."),
    retrieve=extend_schema(tags=["Bots"], summary="One bot", examples=[_ex("Bot", _BOT_RESPONSE, response=True)]),
    partial_update=extend_schema(tags=["Bots"], summary="Change a bot's API account or rate limit", examples=[
        _ex("New rate limit", {"rate_limit": 5}, request=True),
        _ex("New password", {"password": "new-api-password"}, request=True),
        _ex("Remove the account", {"api_username": ""}, request=True)],
        description="Applies from the bot's next batch. The stage can't change."),
    update=extend_schema(tags=["Bots"], summary="Replace a bot's editable fields"),
    destroy=extend_schema(tags=["Bots"], summary="Remove a bot", responses={
        204: OpenApiResponse(description="Removed at once (it wasn't running)"),
        202: OpenApiResponse(BotSerializer, description="Running: it finishes its batch, then the supervisor removes it")}),
    start=extend_schema(tags=["Bots"], summary="Switch a bot on", request=None, responses={
        200: BotSerializer, 409: OpenApiResponse(ERROR, description="It is being removed")}),
    stop=extend_schema(tags=["Bots"], summary="Switch a bot off (it finishes its batch first)", request=None,
                       responses=BotSerializer),
    events=extend_schema(tags=["Bots"], summary="A bot's last 50 events", responses=BotEventSerializer(many=True)),
)

# ============================================================================ Pipeline
_STAGE_EXAMPLE = {
    "stage": "ocr", "label": "OCR", "level": "image", "waiting": 157, "working": 20, "per_minute": 839.5,
    "failed": 0, "bots_wanted": 1, "bots_running": 1,
    "settings": {"trip_if_confidence_at_least": 0.85, "batch_size": 20, "parallel": 8, "max_attempts": 3,
                 "stuck_after_minutes": 10},
    "editable": {"batch_size": {"value": 20, "default": 20, "min": 1, "max": 1000, "decimal": False}},
    "can_switch_off": False, "enabled": True,
    "bots": ["… same shape as GET /api/bots/{id}/ …"]}

PIPELINE = extend_schema(
    tags=["Pipeline"], summary="Every stage: its line, speed, settings and bots",
    responses=OpenApiTypes.OBJECT,
    examples=[_ex("Overview", {
        "stages": [_STAGE_EXAMPLE], "images": {"cleaned": 157, "ocr_running": 20, "read": 560, "skipped": 14},
        "folders": {"reading": 36, "needs_llm": 91, "needs_trip": 16},
        "sources": [{"name": "data", "path": "./data", "accuracy": "high", "priority": 1}],
        "reader_mode": "scan", "kafka_topic": "anpr.folders",
        "use_prefect": False, "ocr_api_mock": True, "max_bots_per_stage": 50, "bots_managed_by": "supervisor"},
        response=True)])

SCALE = extend_schema(
    tags=["Pipeline"], summary="Give a stage exactly N bots (the + / − buttons)",
    description="More: new bots `<stage>-<n>` are added, switched on. Fewer: the newest are removed, each finishing "
                "its batch first. Answers with the same body as GET /api/pipeline/.",
    parameters=[STAGE_PATH],
    request=inline_serializer("ScaleRequest", {"count": serializers.IntegerField(min_value=0)}),
    responses={200: OpenApiTypes.OBJECT, 400: ERROR, 404: OpenApiResponse(ERROR, description="Unknown stage"),
               403: OpenApiResponse(ERROR, description="Docker mode: scale the bot-<stage> service instead")},
    examples=[_ex("Two OCR bots", {"count": 2}, request=True)])

SETTINGS = extend_schema(
    tags=["Pipeline"], summary="Change a stage's settings (null = back to the settings.yaml default)",
    description="Editable: batch_size, parallel, max_attempts, stuck_after_minutes, pause_if_next_line_over, "
                "scan_every_seconds (reader), trip_if_confidence_at_least (ocr, 0–1). Bots use them from their "
                "next batch. `enabled` (true/false, preprocess only) switches preprocessing off: its waiting "
                "images move straight to the OCR line, and new images skip it.",
    parameters=[STAGE_PATH], request=OpenApiTypes.OBJECT,
    responses={200: inline_serializer("StageSettingsResult", {"stage": serializers.CharField(),
                                                              "settings": serializers.DictField()}),
               400: OpenApiResponse(OpenApiTypes.OBJECT, description="{setting: problem}")},
    examples=[_ex("Bigger batches", {"batch_size": 50, "parallel": 16}, request=True),
              _ex("Back to default", {"batch_size": None}, request=True),
              _ex("OCR: straight to trip from 0.9", {"trip_if_confidence_at_least": 0.9}, request=True),
              _ex("Switch preprocessing off", {"enabled": False}, request=True),
              _ex("Invalid", {"batch_size": "Use a number from 1 to 1000."}, response=True, status=400)])

_FAILED_ITEMS = inline_serializer("FailedItem", many=True, fields={
    "id": serializers.IntegerField(), "name": serializers.CharField(), "folder": serializers.CharField(),
    "failed_in": serializers.ChoiceField(STAGE_CHOICES), "error": serializers.CharField(),
    "attempts": serializers.IntegerField()})

FAILED = extend_schema(
    tags=["Pipeline"], summary="The newest failed items (max 200) with their error",
    parameters=[OpenApiParameter("level", str, enum=["image", "folder"], default="image")],
    responses=_FAILED_ITEMS,
    examples=[_ex("Failed image", [{"id": 812, "name": "cam_3.jpg", "folder": "ocr_4393072", "failed_in": "ocr",
                                    "error": "ConnectError: API unreachable", "attempts": 3}], response=True)])

RETRY = extend_schema(
    tags=["Pipeline"], summary="Send failed items back to the line they failed in",
    request=inline_serializer("RetryRequest", {
        "level": serializers.ChoiceField(["image", "folder"]),
        "ids": serializers.ListField(child=serializers.IntegerField(), required=False,
                                     help_text="only these; leave out for every failed item")}),
    responses={200: inline_serializer("RetryResult", {"moved": serializers.IntegerField(),
                                                      "folders_reopened": serializers.IntegerField()}),
               400: ERROR},
    examples=[_ex("Retry all failed images", {"level": "image"}, request=True),
              _ex("Retry two images", {"level": "image", "ids": [812, 813]}, request=True),
              _ex("Result", {"moved": 2, "folders_reopened": 1}, response=True, status=200)])
