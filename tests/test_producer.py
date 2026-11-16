"""
Producer tests (scripts/publish_folders.py): which folders become Kafka messages, and that
watch mode remembers what it sent. No Kafka needed: sending is replaced by a list.
Run: pytest -q tests/test_producer.py
"""
import importlib.util
import os
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("publish_folders", ROOT / "scripts" / "publish_folders.py")
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)


def cfg_for(root: Path) -> dict:
    return {"reader": {"storage_root": str(root), "image_extensions": [".jpg"], "min_folder_age_seconds": 0,
                       "sources": [{"name": "low", "path": str(root / "low"), "accuracy": "low", "priority": 1}]},
            "kafka": {"topics": {"folders": "anpr.folders", "failed": "anpr.failed"}, "partitions": 6}}


def vehicle(root: Path, rel: str, age: float = 3600) -> Path:
    folder = root / rel
    folder.mkdir(parents=True)
    (folder / "cam_1.jpg").write_bytes(b"x")
    old = time.time() - age
    os.utime(folder, (old, old))
    return folder


def run_round(cfg, sent, min_age=0):
    out = []
    with patch.object(pf.kc, "send", side_effect=lambda p, topic, key, data: out.append((topic, key, data))), \
         patch.object(pf.kc, "flush"):
        n = pf.publish_round(cfg, None, pf.watched(cfg, None, None), sent, min_age=min_age, list_images=True)
    return n, out


def test_new_folders_are_sent_once_with_relative_path_and_accuracy(tmp_path):
    cfg, sent = cfg_for(tmp_path), pf.SentLog(str(tmp_path / "sent.txt"))
    vehicle(tmp_path, "low/car_1")
    n, out = run_round(cfg, sent)
    assert n == 1
    topic, key, data = out[0]
    assert (topic, key) == ("anpr.folders", "car_1")
    assert (data["folder"], data["accuracy"], data["images"]) == ("low/car_1", "low", ["cam_1.jpg"])
    vehicle(tmp_path, "low/car_2")
    n, out = run_round(cfg, sent)                      # only the new one
    assert [o[1] for o in out] == ["car_2"]


def test_folders_still_being_written_wait(tmp_path):
    cfg, sent = cfg_for(tmp_path), pf.SentLog(str(tmp_path / "sent.txt"))
    vehicle(tmp_path, "low/car_1", age=0)
    assert run_round(cfg, sent, min_age=60)[0] == 0
    assert sent.load() == set()                        # not remembered: sent once it's finished


def test_forget_makes_watch_mode_send_again(tmp_path):
    cfg, sent = cfg_for(tmp_path), pf.SentLog(str(tmp_path / "sent.txt"))
    vehicle(tmp_path, "low/car_1")
    vehicle(tmp_path, "low/car_2")
    run_round(cfg, sent)
    assert sent.forget(["car_1"]) == 1
    assert [o[1] for o in run_round(cfg, sent)[1]] == ["car_1"]
    assert sent.forget(None) == 2
    assert run_round(cfg, sent)[0] == 2
