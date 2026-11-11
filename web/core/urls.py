"""API addresses. Every one starts with /api/ (see anpr_web/urls.py)."""
from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import api, bot_api


router = DefaultRouter()
router.register("bots", bot_api.BotViewSet)

urlpatterns = [
    path("auth/login/", api.login),
    path("auth/logout/", api.logout),
    path("auth/me/", api.me),
    path("stats/", api.stats),
    path("browse/", api.browse),
    path("folders/", api.FolderList.as_view()),
    path("folders/<int:pk>/", api.FolderDetail.as_view()),
    path("folders/<int:pk>/review/", api.review_folder),
    path("review/next/", api.review_next),
    path("images/<int:pk>/file/", api.image_file),
    path("queue/", bot_api.queue_status),
    path("queue/add/", bot_api.queue_add),
    path("queue/retry-failed/", bot_api.queue_retry_failed),
    path("", include(router.urls)),
]