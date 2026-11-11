"""
API views: each function or class answers one kind of request from React.

The pattern is always the same:
  1. read what React sent (URL, query string, or JSON body)
  2. query the database with the ORM
  3. return a Response, which DRF turns into JSON
"""
import os
from pathlib import Path

from django.contrib.auth import authenticate
from django.db.models import Count, Q
from django.db.models.functions import TruncDate
from django.http import FileResponse, Http404
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import generics, status
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from pipeline.config import load_config

from .models import Folder, Image, Job, Route
from .serializers import (FolderDetailSerializer, FolderListSerializer, JobSerializer,ReviewActionSerializer)
from .services import ReviewError, apply_review

# ---------------------------------------------------------------- folder browser
MAX_BROWSE_ENTRIES = 2000


def _browse_roots() -> list[Path]:
    from .queue import resolve_dir
    roots = load_config()["paths"].get("browse_roots") or [load_config()["paths"]["input_dir"]]
    return [r for r in (resolve_dir(str(x)) for x in roots) if r.is_dir()]


@api_view(["GET"])
def browse(request):
    """GET /api/browse/?path=  ->  the sub-folders of `path` (only inside paths.browse_roots).

    No path = the list of roots. Used by the "Browse" button when choosing a bot's input folder.
    """
    from .queue import resolve_dir
    roots = _browse_roots()
    raw = request.query_params.get("path", "").strip()
    if not raw:
        return Response({"path": "", "parent": None, "roots": [str(r) for r in roots],
                         "dirs": [{"name": str(r), "path": str(r)} for r in roots], "truncated": False})
    path = resolve_dir(raw)
    root = next((r for r in roots if path == r or path.is_relative_to(r)), None)
    if root is None:
        return Response({"detail": "That folder is outside the allowed browse folders "
                                   "(paths.browse_roots in settings.yaml)."}, status=status.HTTP_403_FORBIDDEN)
    if not path.is_dir():
        return Response({"detail": f"{path} does not exist."}, status=status.HTTP_404_NOT_FOUND)
    dirs = []
    try:
        with os.scandir(path) as entries:
            for e in entries:
                if e.name.startswith(".") or not e.is_dir(follow_symlinks=True):
                    continue
                dirs.append({"name": e.name, "path": str(Path(e.path).resolve())})
                if len(dirs) >= MAX_BROWSE_ENTRIES:
                    break
    except PermissionError:
        return Response({"detail": f"No permission to read {path}."}, status=status.HTTP_403_FORBIDDEN)
    dirs.sort(key=lambda d: d["name"].lower())
    return Response({"path": str(path), "parent": None if path == root else str(path.parent),
                     "roots": [str(r) for r in roots], "dirs": dirs,
                     "truncated": len(dirs) >= MAX_BROWSE_ENTRIES})


ROUTE_KEYS = [Route.PROCESSED, Route.ACCEPTED, Route.REVIEW, Route.NO_PLATE, Route.FAILED]


# ---------------------------------------------------------------- login
@api_view(["POST"])
@permission_classes([AllowAny])            # you can't need a token to get a token
def login(request):
    user = authenticate(username=request.data.get("username"), password=request.data.get("password"))
    if user is None:
        return Response({"detail": "That username and password don't match."},
                        status=status.HTTP_400_BAD_REQUEST)
    token, _ = Token.objects.get_or_create(user=user)   # one token per user
    return Response({"token": token.key, "username": user.get_username()})


@api_view(["POST"])
def logout(request):
    request.auth.delete()                  # the token used for this request stops working
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(["GET"])
def me(request):
    return Response({"username": request.user.get_username(),
                     "review_count": Folder.objects.filter(route=Route.REVIEW).count()})


# ---------------------------------------------------------------- dashboard
@api_view(["GET"])
def stats(request):
    """Everything the dashboard charts need, in one request."""
    folders = Folder.objects.all()
    images = Image.objects.filter(is_duplicate=False, is_corrupt=False, confidence__isnull=False)
    if job := request.query_params.get("job"):
        folders = folders.filter(job_id=job)
        images = images.filter(folder__job_id=job)

    routes = dict(folders.values_list("route").annotate(n=Count("id")))

    scenarios = [{"code": r["scenario"], "route": r["route"], "count": r["n"]}
                 for r in folders.exclude(scenario="").values("scenario", "route").annotate(n=Count("id"))]

    daily = {}
    for r in (folders.annotate(day=TruncDate("created_at"))
              .values("day", "route").annotate(n=Count("id")).order_by("day")):
        row = daily.setdefault(str(r["day"]), {"day": str(r["day"]), **{k: 0 for k in ROUTE_KEYS}})
        row[r["route"]] = r["n"]

    # Score histogram in 0.05-wide buckets, split by route
    bins = {}
    for score, route in folders.exclude(final_score__isnull=True).values_list("final_score", "route"):
        start = min(int(score * 20), 19) / 20
        row = bins.setdefault(start, {"bin": f"{start:.2f}", **{k: 0 for k in ROUTE_KEYS}})
        row[route] += 1

    # A sample of images for the scatter chart (enough to see the shape, still fast)
    scatter = list(images.order_by("?")[:3000]
                   .values("confidence", "quality", "folder__route"))

    jobs = (Job.objects.order_by("-started_at")
            .annotate(accepted=Count("folders", filter=Q(folders__route=Route.ACCEPTED)),
                      in_review=Count("folders", filter=Q(folders__route=Route.REVIEW)))[:10])

    return Response({
        "total": sum(routes.values()),
        "routes": {k: routes.get(k, 0) for k in ROUTE_KEYS},
        "scenarios": scenarios,
        "daily": list(daily.values()),
        "score_bins": _fill_bins(bins),
        "scatter": [{"confidence": s["confidence"], "quality": s["quality"], "route": s["folder__route"]}
                    for s in scatter],
        "thresholds": load_config()["decision"],
        "reviewed_today": folders.filter(reviewed_at__date=timezone.localdate()).count(),
        "jobs": JobSerializer(jobs, many=True).data,
    })


def _fill_bins(bins: dict) -> list:
    """Adds empty buckets between the lowest score and 1.0, so the chart axis has no gaps."""
    if not bins:
        return []
    start = int(min(bins) * 20)
    return [bins.get(i / 20, {"bin": f"{i / 20:.2f}", **{k: 0 for k in ROUTE_KEYS}}) for i in range(start, 20)]


# ---------------------------------------------------------------- folders
class FolderList(generics.ListAPIView):
    """GET /api/folders/?q=&route=&scenario=&page=  (a generic view: DRF does the listing and paging)"""
    serializer_class = FolderListSerializer

    def get_queryset(self):
        qs = Folder.objects.order_by("-created_at")
        p = self.request.query_params
        if p.get("route"):
            qs = qs.filter(route__in=p["route"].split(","))
        if p.get("scenario"):
            qs = qs.filter(scenario__in=p["scenario"].split(","))
        if q := p.get("q", "").strip():
            plate = q.upper().replace(" ", "")
            qs = qs.filter(Q(name__icontains=q) | Q(final_plate__icontains=plate)
                           | Q(reviewed_plate__icontains=plate))
        return qs


class FolderDetail(generics.RetrieveAPIView):
    """GET /api/folders/<id>/"""
    queryset = Folder.objects.all()
    serializer_class = FolderDetailSerializer


@api_view(["GET"])
def review_next(request):
    """The oldest folder waiting for review, or 204 No Content if the queue is empty."""
    folder = Folder.objects.filter(route=Route.REVIEW).order_by("created_at").first()
    if folder is None:
        return Response(status=status.HTTP_204_NO_CONTENT)
    data = FolderDetailSerializer(folder).data
    data["queue_left"] = Folder.objects.filter(route=Route.REVIEW).count()
    return Response(data)


@api_view(["POST"])
def review_folder(request, pk):
    """POST /api/folders/<id>/review/  body: {"action": "approve"|"correct"|"reject", "plate": "..."}"""
    folder = get_object_or_404(Folder, pk=pk)
    body = ReviewActionSerializer(data=request.data)
    body.is_valid(raise_exception=True)     # sends a 400 with details if the data is wrong
    try:
        message = apply_review(folder, body.validated_data["action"],
                               request.user.get_username(), body.validated_data["plate"])
    except ReviewError as exc:
        return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
    return Response({"message": message, "folder": FolderDetailSerializer(folder).data})


# ---------------------------------------------------------------- image files
@api_view(["GET"])
def image_file(request, pk):
    """Sends the actual image file so React can display it."""
    img = get_object_or_404(Image, pk=pk)
    path = Path(img.path)
    if img.is_corrupt or not path.is_file():
        raise Http404("Image file is not available")
    return FileResponse(open(path, "rb"))
