"""
Reader service tests. They use a throwaway SQLite database and temporary folders,
so your real data and database are never touched.  Run: pytest -q tests/test_reader.py
"""
import importlib
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
TMP = Path(tempfile.mkdtemp())
LOW, HIGH = TMP / "low", TMP / "high"

# --- point the app at a test database and test sources BEFORE importing it ---
os.environ["ANPR_DB_PATH"] = str(TMP / "test.sqlite3")
cfg = yaml.safe_load(open(ROOT / "config" / "settings.yaml"))
cfg["reader"]["sources"] = [
    {"name": "low-accuracy", "path": str(LOW), "accuracy": "low", "priority": 1},
    {"name": "high-accuracy", "path": str(HIGH), "accuracy": "high", "priority": 2},
]
cfg["reader"]["min_folder_age_seconds"] = 0
yaml.safe_dump(cfg, open(TMP / "settings.yaml", "w"))
os.environ["ANPR_CONFIG"] = str(TMP / "settings.yaml")
os.environ["READER_SHARD"] = "1/1"
if "pipeline.config" in sys.modules:                 # make sure the test config is the one loaded
    importlib.reload(sys.modules["pipeline.config"])

import services.common.django_setup  # noqa: E402,F401
from django.core.management import call_command  # noqa: E402
from django.test import override_settings  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from core.models import IntakeFolder, IntakeImage  # noqa: E402
from services.reader.app import app  # noqa: E402
from services.reader.scanner import Shard, Source, scan_source  # noqa: E402

EXT = {".jpg", ".png"}
LOW_SRC = Source("low-accuracy", LOW, "low", 1)
client = TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def database():
    call_command("migrate", verbosity=0)


@pytest.fixture(autouse=True)
def clean():
    IntakeFolder.objects.all().delete()
    for root in (LOW, HIGH):
        if root.exists():
            for p in sorted(root.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
        root.mkdir(parents=True, exist_ok=True)


def make_folder(root: Path, name: str, contents=(b"image-a", b"image-b")) -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    for i, data in enumerate(contents):
        (folder / f"cam_{i}.jpg").write_bytes(data)
    (folder / "notes.txt").write_text("not an image")
    return folder


def scan(src=LOW_SRC, shard="1/1", min_age=0):
    return scan_source(src, Shard.parse(shard), "test", extensions=EXT, min_age_seconds=min_age, workers=4)


# ---------------------------------------------------------------- sharding
def test_shards_split_every_folder_exactly_once():
    names = [f"car_{i:04}" for i in range(600)]
    shards = [Shard.parse(f"{n}/3") for n in (1, 2, 3)]
    owners = Counter(sum(s.owns(name) for s in shards) for name in names)
    assert owners == {1: 600}                      # every folder has exactly one owner
    per_shard = [sum(s.owns(n) for n in names) for s in shards]
    assert all(150 < c < 250 for c in per_shard)   # and the split is roughly even


@pytest.mark.parametrize("bad", ["0/2", "3/2", "two", "1-2"])
def test_bad_shard_values_are_rejected(bad):
    with pytest.raises(ValueError):
        Shard.parse(bad)


# ---------------------------------------------------------------- scanning
def test_scan_reads_images_and_flags_duplicates():
    make_folder(LOW, "car_1", contents=(b"same", b"same", b"other"))
    r = scan()
    assert (r.seen, r.registered, r.images, r.errors) == (1, 1, 3, [])
    folder = IntakeFolder.objects.get()
    assert (folder.accuracy, folder.priority, folder.image_count, folder.duplicate_count) == ("low", 1, 3, 1)
    assert [i.is_duplicate for i in folder.images.all()] == [False, True, False]
    assert len(folder.images.first().sha256) == 64


def test_scanning_twice_registers_nothing_new():
    make_folder(LOW, "car_1")
    assert scan().registered == 1
    again = scan()
    assert (again.registered, again.already_known) == (0, 1)


def test_folders_still_being_written_wait_for_a_later_scan():
    make_folder(LOW, "car_1")
    r = scan(min_age=3600)
    assert (r.too_new, r.registered) == (1, 0)


def test_two_reader_copies_share_the_work_without_overlap():
    for i in range(40):
        make_folder(LOW, f"car_{i}", contents=(f"{i}".encode(),))
    a, b = scan(shard="1/2"), scan(shard="2/2")
    assert a.registered + b.registered == 40
    assert a.registered > 0 and b.registered > 0
    assert IntakeFolder.objects.values("path").distinct().count() == 40


def test_missing_source_is_reported_not_crashed():
    r = scan(Source("gone", TMP / "missing", "low", 1))
    assert r.registered == 0 and "not found" in r.errors[0]


# ---------------------------------------------------------------- the HTTP API
def test_health():
    body = client.get("/health").json()
    assert body["status"] == "ok" and body["shard"] == "1/1"
    assert [s["name"] for s in body["sources"]] == ["low-accuracy", "high-accuracy"]


def test_scan_then_folders_come_out_low_accuracy_first():
    make_folder(HIGH, "car_high")              # registered first...
    client.post("/scan")
    make_folder(LOW, "car_low")                # ...low accuracy arrives later
    assert client.post("/scan").json()["registered"] == 1
    names = [f["name"] for f in client.get("/folders").json()]
    assert names == ["car_low", "car_high"]    # but still comes out first


def test_folder_detail_and_image_file():
    make_folder(LOW, "car_1", contents=(b"\x89PNG-bytes",))
    client.post("/scan")
    folder_id = client.get("/folders").json()[0]["id"]
    detail = client.get(f"/folders/{folder_id}").json()
    assert [i["file_name"] for i in detail["images"]] == ["cam_0.jpg"]
    file = client.get(f"/images/{detail['images'][0]['id']}/file")
    assert file.status_code == 200 and file.content == b"\x89PNG-bytes"


def test_files_outside_the_sources_are_refused():
    make_folder(LOW, "car_1")
    client.post("/scan")
    img = IntakeImage.objects.first()
    IntakeImage.objects.filter(pk=img.pk).update(path="/etc/passwd")
    assert client.get(f"/images/{img.pk}/file").status_code == 403


def test_read_only_refuses_to_scan():
    with override_settings(READ_ONLY=True):
        r = client.post("/scan")
    assert r.status_code == 403 and "Read-only" in r.json()["detail"]


def test_stats():
    make_folder(LOW, "a")
    make_folder(HIGH, "b")
    client.post("/scan")
    s = client.get("/stats").json()
    assert (s["total_folders"], s["by_accuracy"]) == (2, {"low": 1, "high": 1})
