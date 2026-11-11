from django.contrib import admin
from django.urls import include, path

urlpatterns = [

    path("admin/", admin.site.urls),      # Django admin (back office)
    path("api/", include("core.urls")),   # the API that React talks to
]
