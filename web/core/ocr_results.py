"""
What happens to a folder once ALL its images are through OCR.

  1. its route is decided from the plates the OCR read (settings.yaml stages.ocr):
       a plate with confidence >= trip_if_confidence_at_least  -> needs_trip (create the trip)
       a plate, but less sure                                  -> needs_llm  (the LLM decides)
       no plate in any image                                   -> manual     (a person checks)
  2. its result is saved to Folder / Image, which the Folders and Overview pages show.

This replaces the placeholder routing. With 4 OCR engines later (step 4), only `decide()`
changes: it will count how many engines agree instead of looking at one confidence.
"""
from collections import Counter

from django.db import transaction
from django.utils import timezone

from . import stage_queue
from .models import Folder, Image, IntakeFolder, Route


def decide(images, cfg: dict) -> tuple[str, str, object]:
    """(next line, reason, best image or None) for a folder's finished images."""
    read = [i for i in images if i.stage == "read"]
    if not read:
        return "manual", "No image could be read by the OCR API", None
    with_plate = [i for i in read if i.plate]
    if not with_plate:
        return "manual", f"No plate found in {len(read)} image(s)", None
    best = max(with_plate, key=lambda i: i.confidence or 0)
    sure = cfg.get("trip_if_confidence_at_least", 0.85)
    agree = sum(1 for i in with_plate if i.plate == best.plate)
    if (best.confidence or 0) >= sure:
        return "needs_trip", f"{best.plate} read with confidence {best.confidence:.2f} ({agree} image(s) agree)", best
    return "needs_llm", f"Best read {best.plate} has confidence {best.confidence or 0:.2f}, below {sure}", best


def finish_folder(folder: IntakeFolder, cfg: dict) -> bool:
    """Routes the folder and saves its result, exactly once (False if another bot already did)."""
    images = list(folder.images.all())
    to, reason, best = decide(images, cfg)
    with transaction.atomic():
        if not stage_queue.route_folder(folder, to, reason):
            return False
        IntakeFolder.objects.filter(pk=folder.pk).update(
            plate=best.plate if best else "", confidence=best.confidence if best else None, read_at=timezone.now())
        save_result(folder, images, reason)
    return True


def save_result(folder: IntakeFolder, images, reason: str) -> Folder:
    """Writes (or rewrites, when a folder is processed again) its row on the Folders page."""
    Folder.objects.filter(path=folder.path).delete()          # its images go with it
    readers = Counter(i.read_by for i in images if i.read_by)
    result = Folder.objects.create(path=folder.path, name=folder.name, reason=reason,
                                   processed_by=", ".join(name for name, _ in readers.most_common()))
    Image.objects.bulk_create([
        Image(folder=result, path=i.path, file_name=i.file_name, sha256=i.sha256, is_duplicate=i.is_duplicate,
              plate=i.plate, confidence=i.confidence, final_score=i.confidence, api_response=i.api_response,
              api_error=i.last_error if i.stage == "failed" else "", duration_ms=i.duration_ms,
              api_attempts=i.api_attempts)
        for i in images])
    best = result.images.exclude(plate="").order_by("-confidence").first()
    answered = any(i.stage == "read" for i in images)
    result.route = Route.PROCESSED if answered else Route.FAILED
    if best:
        result.final_plate, result.final_score, result.best_image = best.plate, best.confidence, best
    result.save()
    return result
