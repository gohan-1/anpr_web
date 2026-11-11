"""
Scanning logic of the reader service (no web code here, so it's easy to test).

What one scan does, for each configured source:
  1. list the sub-folders (os.scandir: fast, even with millions of entries)
  2. keep only the folders that belong to THIS copy's shard
  3. skip folders already registered, or changed in the last few seconds
  4. read the rest in parallel threads: list images, size, fingerprint (sha256), duplicates
  5. register each folder and its images in ONE database transaction

HOW SEVERAL COPIES SPLIT THE WORK (sharding):
  Each copy is told "you are copy 2 of 3". A folder's name is turned into a number
  (a hash); the folder belongs to copy  (number % 3) + 1. Every copy computes the
  same answer without talking to the others, and each folder has exactly one owner.
  If two copies ever did register the same folder, the database's unique path
  constraint lets only the first one succeed.
"""
from __future__ import annotations

import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone as dt_timezone
from pathlib import Path

from django.db import IntegrityError, transaction

from core.models import IntakeFolder, IntakeImage


# ---------------------------------------------------------------- sharding
@dataclass(frozen=True)
class Shard:
    number: int    # 1-based: "copy 2 of 3" -> number=2, count=3
    count: int

    @classmethod
    def parse(cls, text: str) -> "Shard":
        """'2/3' -> Shard(2, 3). Raises ValueError for anything else."""
        number, count = (int(x) for x in text.strip().split("/"))
        if not 1 <= number <= count:
            raise ValueError(f"Shard '{text}': the first number must be between 1 and {count}")
        return cls(number, count)

    def owns(self, folder_name: str) -> bool:
        if self.count == 1:
            return True
        # md5 here is just a fast, stable way to turn a name into a number (not for security)
        value = int(hashlib.md5(folder_name.encode()).hexdigest()[:8], 16)
        return value % self.count == self.number - 1

    def __str__(self):
        return f"{self.number}/{self.count}"


# ---------------------------------------------------------------- settings
@dataclass(frozen=True)
class Source:
    name: str
    path: Path
    accuracy: str     # "low" or "high"
    priority: int


def load_sources(reader_cfg: dict, project_root: Path) -> list[Source]:
    sources = []
    for s in reader_cfg["sources"]:
        path = Path(s["path"]).expanduser()
        if not path.is_absolute():
            path = project_root / path          # relative paths start from the anpr-project folder
        if s["accuracy"] not in IntakeFolder.Accuracy.values:
            raise ValueError(f"Source '{s['name']}': accuracy must be 'low' or 'high'")
        sources.append(Source(s["name"], path.resolve(), s["accuracy"], int(s["priority"])))
    return sources


# ---------------------------------------------------------------- reading one folder
@dataclass
class ImageInfo:
    file_name: str
    path: str
    size_bytes: int
    sha256: str
    modified_at: datetime
    is_duplicate: bool = False


@dataclass
class FolderInfo:
    path: str
    name: str
    modified_at: datetime
    images: list[ImageInfo] = field(default_factory=list)
    error: str = ""


def _fingerprint(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_folder(path: str, extensions: set[str]) -> FolderInfo:
    """Reads one folder's images. Runs in a worker thread: no database access here."""
    folder = Path(path)
    info = FolderInfo(path=path, name=folder.name,
                      modified_at=datetime.fromtimestamp(folder.stat().st_mtime, dt_timezone.utc))
    try:
        seen = set()
        with os.scandir(path) as entries:
            files = sorted((e for e in entries if e.is_file() and Path(e.name).suffix.lower() in extensions),
                           key=lambda e: e.name)
        for entry in files:
            stat = entry.stat()
            digest = _fingerprint(entry.path)
            info.images.append(ImageInfo(
                file_name=entry.name, path=entry.path, size_bytes=stat.st_size, sha256=digest,
                modified_at=datetime.fromtimestamp(stat.st_mtime, dt_timezone.utc),
                is_duplicate=digest in seen))
            seen.add(digest)
    except OSError as exc:                       # unreadable folder or file
        info.error = f"{type(exc).__name__}: {exc}"
    return info


# ---------------------------------------------------------------- registering
def register(info: FolderInfo, source: Source, instance: str) -> bool:
    """Saves the folder and its images together. Returns False if it was already registered."""
    try:
        with transaction.atomic():               # all or nothing: never a folder without its images
            folder = IntakeFolder.objects.create(
                path=info.path, name=info.name, source=source.name, accuracy=source.accuracy,
                priority=source.priority, image_count=len(info.images),
                duplicate_count=sum(i.is_duplicate for i in info.images),
                total_bytes=sum(i.size_bytes for i in info.images),
                folder_modified_at=info.modified_at, discovered_by=instance)
            IntakeImage.objects.bulk_create([
                IntakeImage(folder=folder, file_name=i.file_name, path=i.path, size_bytes=i.size_bytes,
                            sha256=i.sha256, is_duplicate=i.is_duplicate, modified_at=i.modified_at)
                for i in info.images])
        return True
    except IntegrityError:                       # another reader copy registered it a moment earlier
        return False


# ---------------------------------------------------------------- one scan
@dataclass
class ScanResult:
    source: str
    seen: int = 0              # sub-folders in the source
    other_shard: int = 0       # belong to another reader copy
    already_known: int = 0     # registered before
    too_new: int = 0           # still being written; picked up by a later scan
    registered: int = 0        # new intake records created now
    images: int = 0
    bytes: int = 0
    errors: list[str] = field(default_factory=list)
    seconds: float = 0.0


def scan_source(source: Source, shard: Shard, instance: str, *, extensions: set[str],
                min_age_seconds: int, workers: int, batch_size: int = 1000) -> ScanResult:
    started = time.monotonic()
    result = ScanResult(source=source.name)
    if not source.path.is_dir():
        result.errors.append(f"Source folder not found: {source.path}")
        return result

    now = time.time()
    candidates = []
    with os.scandir(source.path) as entries:
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            result.seen += 1
            if not shard.owns(entry.name):
                result.other_shard += 1
            elif now - entry.stat().st_mtime < min_age_seconds:
                result.too_new += 1
            else:
                candidates.append(entry.path)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0, len(candidates), batch_size):
            batch = candidates[start:start + batch_size]
            known = set(IntakeFolder.objects.filter(path__in=batch).values_list("path", flat=True))
            result.already_known += len(known)
            todo = [p for p in batch if p not in known]
            # Threads read the files (slow disk work); this thread does the database writes.
            for info in pool.map(lambda p: read_folder(p, extensions), todo):
                if info.error:
                    result.errors.append(f"{info.name}: {info.error}")
                    continue
                if register(info, source, instance):
                    result.registered += 1
                    result.images += len(info.images)
                    result.bytes += sum(i.size_bytes for i in info.images)
                else:
                    result.already_known += 1

    result.seconds = round(time.monotonic() - started, 2)
    return result
