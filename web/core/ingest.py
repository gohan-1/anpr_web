"""Saves the OCR results of one folder to the database. No decision rules: just what the API read."""
from pathlib import Path

from pipeline.processor import ImageAnalysis

from .models import Folder, Image, Route


def save_folder_result(folder_path: Path, images: list[ImageAnalysis], job=None, processed_by: str = "") -> Folder:
    folder = Folder.objects.create(job=job, path=str(folder_path), name=folder_path.name,
                                   processed_by=processed_by)
    Image.objects.bulk_create([
        Image(folder=folder, path=str(a.path), file_name=a.path.name, sha256="",
              plate=a.plate or "", confidence=a.confidence, final_score=a.confidence,
              api_response=a.raw, api_error=a.error,
              duration_ms=round(a.seconds * 1000) if a.seconds is not None else None,
              api_attempts=a.attempts or None)
        for a in images])

    # Summary for the folder list: the highest-confidence read (for display only, nothing is decided).
    best = folder.images.exclude(plate="").order_by("-confidence").first()
    answered = [a for a in images if not a.error]
    folder.route = Route.PROCESSED if answered else Route.FAILED
    folder.reason = "" if answered else "No image could be read by the OCR API"
    if best:
        folder.final_plate, folder.final_score, folder.best_image = best.plate, best.confidence, best
    folder.save()
    return folder
