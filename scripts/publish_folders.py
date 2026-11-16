"""
The folder PRODUCER: turns vehicle folders on disk into Kafka messages for the reader bots.

    files in data/  ->  this producer  ->  topic anpr.folders  ->  reader bots (consumer group)

Watch mode (a service: `make start` in Kafka mode, the `producer` container in Docker):

    python scripts/publish_folders.py --watch

  every stages.reader.scan_every_seconds it looks in every reader.sources path, and sends
  ONE message per vehicle folder that is new (not sent before) and finished (unchanged for
  reader.min_folder_age_seconds, so the camera is done writing). The accuracy of the source
  becomes the message's "accuracy" (its priority). What was sent is remembered in a state
  file, so a restart doesn't send everything again.

One-off (sends everything it finds, sent before or not; e.g. after `make reset`):

    python scripts/publish_folders.py                         every source in settings.yaml
    python scripts/publish_folders.py low_accuracy --accuracy low --limit 10

Forget folders, so watch mode sends them again (make reprocess / make reset do this):

    python scripts/publish_folders.py --forget ocr_4393072 ocr_4393075
    python scripts/publish_folders.py --forget-all

Folders in messages are RELATIVE to reader.storage_root (./data), so the same message works
on the laptop and in Docker. Kafka's address: KAFKA_BOOTSTRAP_SERVERS (from envs/.env.<env>).
Sending the same folder twice is harmless: the reader registers a folder path only once.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "web"))

from anpr_web.env import load_environment       # noqa: E402  (KAFKA_BOOTSTRAP_SERVERS, ANPR_READER_MODE)

load_environment()

from pipeline import kafka_client as kc         # noqa: E402
from pipeline.config import load_config, reader_mode   # noqa: E402

DEFAULT_STATE = os.environ.get("ANPR_PRODUCER_STATE", str(ROOT / ".run" / "kafka_producer_sent.txt"))


# ---------------------------------------------------------------- what was sent
class SentLog:
    """One line per sent folder (its path relative to the storage root). Re-read every round,
    so `--forget` from another terminal takes effect without restarting the producer."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> set[str]:
        return set(self.path.read_text().split("\n")) - {""} if self.path.exists() else set()

    def add(self, folders: list[str]) -> None:
        with open(self.path, "a") as f:
            f.writelines(f"{x}\n" for x in folders)

    def forget(self, names: list[str] | None) -> int:
        sent = self.load()
        keep = set() if names is None else {s for s in sent if Path(s).name not in names and s not in names}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text("".join(f"{x}\n" for x in sorted(keep)))
        tmp.replace(self.path)                     # atomic: the producer never reads half a file
        return len(sent) - len(keep)


# ---------------------------------------------------------------- finding folders
def storage_root(cfg: dict) -> Path:
    root = Path(cfg["reader"].get("storage_root", "./data")).expanduser()
    return root if root.is_absolute() else (ROOT / root).resolve()


def watched(cfg: dict, folder: str | None, accuracy: str | None) -> list[tuple[Path, str]]:
    """(parent folder holding vehicle folders, accuracy) pairs: the command line's, or every source."""
    root = storage_root(cfg)
    if folder:
        return [((root / folder).resolve(), accuracy)]
    pairs = []
    for s in cfg["reader"]["sources"]:
        path = Path(s["path"]).expanduser()
        pairs.append(((path if path.is_absolute() else ROOT / path).resolve(), s["accuracy"]))
    return pairs


def vehicle_folders(parent: Path, min_age: float) -> list[Path]:
    now = time.time()
    with os.scandir(parent) as entries:
        return sorted(Path(e.path) for e in entries
                      if e.is_dir(follow_symlinks=False) and now - e.stat().st_mtime >= min_age)


def message_for(vehicle: Path, root: Path, accuracy: str, extensions: set[str], list_images: bool) -> dict:
    msg = {"folder_id": vehicle.name, "folder": str(vehicle.relative_to(root)), "accuracy": accuracy,
           "captured_at": datetime.fromtimestamp(vehicle.stat().st_mtime, timezone.utc).isoformat()}
    if list_images:
        msg["images"] = sorted(f.name for f in vehicle.iterdir() if f.is_file() and f.suffix.lower() in extensions)
    return msg


# ---------------------------------------------------------------- one round
def publish_round(cfg, producer, sources, sent: SentLog | None, *, min_age, list_images, limit=None) -> int:
    """Sends the folders not sent yet (or all, when sent is None). Returns how many."""
    root, topic = storage_root(cfg), cfg["kafka"]["topics"]["folders"]
    extensions = {e.lower() for e in cfg["reader"]["image_extensions"]}
    already = sent.load() if sent else set()
    count, new = 0, []
    for parent, accuracy in sources:
        if not parent.is_dir():
            print(f"Not a folder (skipped): {parent}", flush=True)
            continue
        if not parent.is_relative_to(root):
            sys.exit(f"{parent} is outside reader.storage_root ({root}): readers could not find it.")
        for vehicle in vehicle_folders(parent, min_age):
            rel = str(vehicle.relative_to(root))
            if rel in already:
                continue
            kc.send(producer, topic, key=vehicle.name,
                    data=message_for(vehicle, root, accuracy, extensions, list_images))
            new.append(rel)
            count += 1
            if limit and count >= limit:
                break
            if len(new) >= 500:                     # confirm in chunks: a crash loses little
                kc.flush(producer)
                if sent:
                    sent.add(new)
                new = []
    kc.flush(producer)                              # only remember what Kafka confirmed
    if sent and new:
        sent.add(new)
    return count


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish one Kafka message per vehicle folder")
    ap.add_argument("folder", nargs="?", help="folder under the storage root holding vehicle folders "
                                              "(default: every reader.sources path)")
    ap.add_argument("--accuracy", choices=["low", "high"], help="needed with a folder")
    ap.add_argument("--watch", action="store_true", help="keep running; send new folders as they arrive")
    ap.add_argument("--limit", type=int, help="one-off: send at most this many")
    ap.add_argument("--list-images", action="store_true", help='include the "images" list in each message')
    ap.add_argument("--state", default=DEFAULT_STATE, help="file remembering sent folders (watch mode)")
    ap.add_argument("--forget", nargs="+", metavar="FOLDER", help="watch mode will send these folders again")
    ap.add_argument("--forget-all", action="store_true", help="watch mode will send every folder again")
    args = ap.parse_args()

    sent = SentLog(args.state)
    if args.forget or args.forget_all:
        n = sent.forget(None if args.forget_all else args.forget)
        print(f"Forgot {n} sent folder(s): the producer sends them again in its next round.")
        return
    if args.folder and not args.accuracy:
        ap.error("--accuracy low|high is needed with a folder")

    cfg = load_config()
    sources = watched(cfg, args.folder, args.accuracy)
    kc.ensure_topics([cfg["kafka"]["topics"]["folders"], cfg["kafka"]["topics"]["failed"]],
                     cfg["kafka"]["partitions"])
    producer = kc.make_producer()
    topic = cfg["kafka"]["topics"]["folders"]

    if not args.watch:
        n = publish_round(cfg, producer, sources, None, min_age=0, list_images=args.list_images, limit=args.limit)
        print(f"Published {n} folder message(s) to {topic} (Kafka at {kc.servers()})")
        return

    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"Producer watching {', '.join(str(p) for p, _ in sources)} -> {topic} (Kafka at {kc.servers()}). "
          f"Sent so far: {len(sent.load())}. Ctrl+C to stop.", flush=True)
    if reader_mode() != "kafka":
        print("Note: ANPR_READER_MODE is not 'kafka', so no reader bot consumes these messages.", flush=True)
    while running:
        cfg = load_config()
        every = cfg["stages"]["reader"].get("scan_every_seconds", 30)
        try:
            n = publish_round(cfg, producer, sources, sent, min_age=cfg["reader"]["min_folder_age_seconds"],
                              list_images=args.list_images)
            if n:
                print(f"{datetime.now():%H:%M:%S} published {n} new folder(s)", flush=True)
        except kc.KafkaException as exc:           # Kafka down: nothing remembered, retried next round
            print(f"{datetime.now():%H:%M:%S} Kafka problem, will retry: {exc}", flush=True)
        end = time.monotonic() + every
        while running and time.monotonic() < end:
            time.sleep(0.5)
    producer.flush(10)
    print("Producer stopped.", flush=True)


if __name__ == "__main__":
    main()
