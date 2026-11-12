"""
Creates realistic sample data so you can try the Streamlit app without the real API.

Run from the web folder:
    python manage.py seed_demo              # 120 folders over the last 7 days
    python manage.py seed_demo --folders 300

HOW THIS WORKS: any file in core/management/commands/ becomes a manage.py command
named after the file. Django calls handle() when you run it.

The fake image results go through the REAL decide() function, so every folder's
route and scenario are exactly what the pipeline would produce.
"""
import random
from datetime import timedelta
from pathlib import Path

import cv2
import numpy as np
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core.models import Folder, Image, Job
from pipeline.config import load_config
from pipeline.decision import ImageResult, Thresholds, decide, image_score

STATES = ["MH", "KA", "DL", "TN", "GJ", "UP"]
LOOKALIKE = {"8": "B", "B": "8", "0": "D", "D": "0", "1": "I", "5": "S", "S": "5", "2": "Z"}


def random_plate(rng):
    return f"{rng.choice(STATES)}{rng.randint(1, 20):02d}{rng.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}" \
           f"{rng.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{rng.randint(1000, 9999)}"


def misread(plate, rng):
    i = rng.randrange(len(plate))
    return plate[:i] + LOOKALIKE.get(plate[i], plate[i]) + plate[i + 1:]


def draw_image(path, plate, quality, rng):
    """A simple fake 'car park' photo with the plate drawn on it, blurred/darkened by quality."""
    img = np.full((360, 480, 3), rng.randint(60, 95), np.uint8)
    cv2.rectangle(img, (0, 250), (480, 360), (48, 52, 58), -1)
    cv2.rectangle(img, (95, 140), (385, 255), (rng.randint(40, 200),) * 3, -1)  # car body
    cv2.rectangle(img, (130, 195), (350, 245), (245, 245, 245), -1)
    cv2.rectangle(img, (130, 195), (350, 245), (20, 20, 20), 3)
    cv2.putText(img, plate, (140, 230), cv2.FONT_HERSHEY_SIMPLEX, 0.85, (20, 20, 20), 2)
    if quality < 0.6:
        k = int((0.6 - quality) * 60) * 2 + 1
        img = cv2.GaussianBlur(img, (k, k), 0)
    if quality < 0.3:
        img = (img * 0.45).astype(np.uint8)
    cv2.imwrite(str(path), img)


class Command(BaseCommand):
    help = "Create sample jobs, folders and images for trying out the app"

    def add_arguments(self, parser):
        parser.add_argument("--folders", type=int, default=120)
        parser.add_argument("--seed", type=int, default=7)

    def handle(self, *args, **opts):
        if not settings.IS_LOCAL:
            raise CommandError(f"seed_demo adds fake data, so it only runs on local, not on {settings.ANPR_ENV}.")
        rng = random.Random(opts["seed"])
        cfg = load_config()
        t = Thresholds.from_config({**cfg["decision"], "plate_regex": r"^[A-Z]{2}\d{2}[A-Z]{2}\d{4}$"})
        image_root = Path(settings.PROJECT_ROOT) / "data" / "demo_images"
        image_root.mkdir(parents=True, exist_ok=True)

        now = timezone.now()
        jobs = [Job.objects.create(input_path=f"/data/carpark_day_{d}", status=Job.Status.DONE,
                                   finished_at=now - timedelta(days=d)) for d in range(6, -1, -1)]
        made = 0
        for n in range(opts["folders"]):
            day = rng.randrange(7)
            job = jobs[day]
            created = now - timedelta(days=6 - day, minutes=rng.randint(0, 600))
            folder = Folder.objects.create(job=job, path=f"/data/{job.input_path}/car_{n:04}",
                                           name=f"car_{n:04}")
            true_plate = random_plate(rng)
            empty_spot = rng.random() < 0.05          # photo of an empty bay: no plate at all
            results, images = [], []
            for k in range(rng.randint(1, 4)):
                name = f"cam_{k + 1}.jpg"
                path = image_root / f"{folder.name}_{name}"
                quality = round(min(1.0, max(0.05, rng.betavariate(5, 2))), 4)
                if rng.random() < 0.02:               # corrupt file
                    path.write_bytes(b"broken")
                    results.append(ImageResult(name, corrupt=True))
                    images.append(Image(folder=folder, path=str(path), file_name=name,
                                        sha256=f"{folder.name}{k}", is_corrupt=True))
                    continue
                draw_image(path, "" if empty_spot else true_plate, quality, rng)
                error = "Read timed out" if rng.random() < 0.02 else None
                plate = conf = None
                if not error and not empty_spot and rng.random() > 0.04:
                    conf = round(min(0.99, max(0.3, rng.gauss(0.55 + 0.4 * quality, 0.08))), 3)
                    plate = misread(true_plate, rng) if rng.random() < (0.35 - 0.3 * quality) else true_plate
                r = ImageResult(name, quality=quality, plate=plate, confidence=conf, error=error)
                results.append(r)
                images.append(Image(folder=folder, path=str(path), file_name=name,
                                    sha256=f"{folder.name}{k}", quality=quality,
                                    quality_details={"quality": quality}, plate=plate or "",
                                    confidence=conf, final_score=image_score(r, t) if plate else None,
                                    api_error=error or ""))
            Image.objects.bulk_create(images)
            d = decide(results, t)
            folder.route, folder.scenario, folder.reason = d.route.value, d.scenario, d.reason
            folder.final_plate, folder.final_score, folder.candidates = d.plate or "", d.score, d.candidates
            if d.best_image_id:
                folder.best_image = folder.images.filter(file_name=d.best_image_id).first()
            folder.save()
            Folder.objects.filter(pk=folder.pk).update(created_at=created)
            made += 1

        for job in jobs:
            job.folder_count = job.folders.count()
            job.save()
        self.stdout.write(self.style.SUCCESS(f"Created {made} folders across {len(jobs)} runs."))
