"""API addresses. Every one starts with /api/ (see anpr_web/urls.py)."""
from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import api, bot_api

# A router creates all the URLs for a ViewSet automatically:
#   /bots/  /bots/<id>/  /bots/<id>/start/  /bots/<id>/stop/  /bots/<id>/events/
router = DefaultRouter()
router.register("bots", bot_api.BotViewSet)

urlpatterns = [
    path("auth/login/", api.login),
    path("auth/logout/", api.logout),
    path("auth/me/", api.me),
    path("env/", api.environment),
    path("stats/", api.stats),
    path("folders/", api.FolderList.as_view()),
    path("folders/<int:pk>/", api.FolderDetail.as_view()),
    path("folders/<int:pk>/review/", api.review_folder),
    path("review/next/", api.review_next),
    path("images/<int:pk>/file/", api.image_file),
    path("pipeline/", bot_api.pipeline_overview),
    path("pipeline/failed/", bot_api.pipeline_failed),
    path("pipeline/retry/", bot_api.pipeline_retry),
    path("pipeline/<str:stage>/scale/", bot_api.pipeline_scale),
    path("pipeline/<str:stage>/settings/", bot_api.pipeline_settings),
    path("", include(router.urls)),
]