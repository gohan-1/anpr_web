from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

urlpatterns = [
    path("admin/", admin.site.urls),      # Django admin (back office)
    path("api/", include("core.urls")),   # the API that React talks to
]

if settings.API_DOCS:                     # see API_DOCS in settings.py
    urlpatterns += [
        path("api/schema/", SpectacularAPIView.as_view(), name="schema"),                    # OpenAPI 3 (YAML/JSON)
        path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger"),  # Swagger UI
        path("api/redoc/", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),     # ReDoc
    ]
