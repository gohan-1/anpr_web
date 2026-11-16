"""
The reader bot in Kafka mode: listens to the folders topic instead of scanning disks.

    forever:
        take the next message from my share of the partitions
        handle it: check -> wait for the images -> read them -> register in PostgreSQL
        bad message (can never work)  -> send it to the failed topic, with the reason
        COMMIT: only now does Kafka consider the message done

    Database down or another unexpected error: the bot stops WITHOUT committing.
    Docker (or you) restarts it, and Kafka delivers the same message again.
    A re-delivered message is harmless: the unique folder path means it's registered once.

Every reader bot joins the same consumer group, so Kafka shares the partitions between
them: no shards are needed in Kafka mode. A topic with 6 partitions keeps at most 6
readers busy; a 7th would get no partition and sit idle.

Docker Pause (web app): the bot leaves the group, so its partitions move to the other
readers at once; Resume joins again.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from django.conf import settings
from django.db.models import F

from core import stage_queue
from core.models import Bot
from pipeline import kafka_client as kc
from services.reader.kafka_intake import BadMessage, handle_message, priorities_from

from .runner import BaseBot, log


class KafkaReaderBot(BaseBot):
    def __init__(self, name: str):
        super().__init__(name, "reader")
        rcfg, kcfg = self.cfg["reader"], self.cfg["kafka"]
        root = Path(rcfg.get("storage_root", "./data")).expanduser()
        self.root = root if root.is_absolute() else (Path(settings.PROJECT_ROOT) / root).resolve()
        self.extensions = {e.lower() for e in rcfg["image_extensions"]}
        self.priorities = priorities_from(rcfg)
        self.wait_seconds = float(rcfg.get("wait_for_files_seconds", 30))
        self.topic = kcfg["topics"]["folders"]
        self.failed_topic = kcfg["topics"]["failed"]
        self.group = kcfg.get("groups", {}).get("reader", "anpr-readers")
        self.partitions = int(kcfg.get("partitions", 6))

    def run(self) -> None:
        self.register(shard="kafka")
        log(self.bot_id, f"Kafka mode: group {self.group}, topic {self.topic}, Kafka at {kc.servers()}, "
                         f"images under {self.root}")
        kc.ensure_topics([self.topic, self.failed_topic], self.partitions)
        consumer = kc.make_consumer(self.group, [self.topic])
        producer = kc.make_producer()
        subscribed = True
        try:
            while not self.should_stop():
                if self.wait_while_paused():                      # Docker: Pause in the web app
                    if subscribed:                                # leave the group: the other readers
                        consumer.unsubscribe()                    # take over this bot's partitions
                        subscribed = False
                    continue
                if not subscribed:                                # Resume: join the group again
                    consumer.subscribe([self.topic])
                    subscribed = True
                msg = consumer.poll(1.0)
                if msg is None:
                    self.set_task(f"Waiting for messages on {self.topic}")
                    continue
                if msg.error():                                   # e.g. Kafka briefly unreachable
                    log(self.bot_id, f"Kafka: {msg.error()}", "warning")
                    time.sleep(1)
                    continue
                self.handle(msg, producer)
                consumer.commit(message=msg, asynchronous=False)  # "done", only after handling
        finally:
            consumer.close()                                      # leave the group cleanly
            producer.flush(10)
            self.shutdown()

    def handle(self, msg, producer) -> None:
        key = msg.key().decode() if msg.key() else "(no key)"
        self.set_task(f"Reading {key} (partition {msg.partition()}, offset {msg.offset()})")
        try:
            data = json.loads(msg.value())
            result = handle_message(data, root=self.root, extensions=self.extensions,
                                    priorities=self.priorities, instance=self.name,
                                    first_stage=stage_queue.first_image_stage(),
                                    wait_seconds=self.wait_seconds)
        except (BadMessage, ValueError) as exc:                   # ValueError: not valid JSON
            self.dead_letter(msg, producer, str(exc))
            return
        if result.registered:
            stage_queue.record("reader", 1, 0)
            Bot.objects.filter(pk=self.bot_id).update(processed=F("processed") + 1)

    def dead_letter(self, msg, producer, reason: str) -> None:
        """Parks a message that can never succeed in the failed topic, with the reason,
        so it doesn't block the partition and someone can look at it later."""
        raw = msg.value() or b""
        kc.send(producer, self.failed_topic, key=(msg.key() or b"").decode(errors="replace"), data={
            "reason": reason,
            "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
            "message": raw.decode(errors="replace")[:5000],
            "bot": self.name, "at": datetime.now(timezone.utc).isoformat(),
        })
        kc.flush(producer)                     # it must really be stored before we commit the original
        stage_queue.record("reader", 0, 1)
        Bot.objects.filter(pk=self.bot_id).update(failed=F("failed") + 1, last_error=reason[:2000])
        log(self.bot_id, f"Message at partition {msg.partition()} offset {msg.offset()} "
                         f"sent to {self.failed_topic}: {reason}", "error")