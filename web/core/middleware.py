"""
Read-only protection.

A middleware runs on EVERY request before it reaches a view. This one refuses any
request that would change data (POST, PUT, PATCH, DELETE) when READ_ONLY is on, so
nothing in the app, including the Django admin, can modify that database.
Signing in and out is still allowed.
"""
from django.conf import settings
from django.http import JsonResponse

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
ALWAYS_ALLOWED = ("/api/auth/", "/admin/login/", "/admin/logout/")


class ReadOnlyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if (settings.READ_ONLY and request.method not in SAFE_METHODS
                and not request.path.startswith(ALWAYS_ALLOWED)):
            return JsonResponse(
                {"detail": f"This is a read-only connection to {settings.ANPR_ENV}. Changes are disabled."},
                status=403)
        return self.get_response(request)
