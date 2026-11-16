"""
Kafka-mode reader tests. No Kafka server needed: the message handling is tested directly,
and the bot is given fake messages and a fake producer.
Run: python manage.py test core.test_kafka_intake
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase

from core import stage_queue
from core.models import Bot, IntakeFolder, IntakeImage
from services.reader.kafka_intake import BadMessage, handle_message, priorities_from

EXT = {".jpg", ".png"}


class Storage:
    """A temporary image storage with vehicle folders inside."""
    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)

    def vehicle(self, rel, contents=(b"a", b"b")):
        folder = self.root / rel
        folder.mkdir(parents=True)
        for i, data in enumerate(contents):
            (folder / f"cam_{i}.jpg").write_bytes(data)
        return folder


def handle(storage, data, wait=0.2):
    return handle_message(data, root=storage.root, extensions=EXT, priorities={"low": 1, "high": 2},
                          instance="test", first_stage=stage_queue.first_image_stage(), wait_seconds=wait)


class HandleMessageTests(TestCase):
    def setUp(self):
        self.s = Storage()

    def tearDown(self):
        self.s.dir.cleanup()

    def test_registers_a_folder_with_its_priority(self):
        self.s.vehicle("high_accuracy/car_1", contents=(b"x", b"x", b"y"))
        r = handle(self.s, {"folder_id": "car_1", "folder": "high_accuracy/car_1", "accuracy": "high"})
        self.assertEqual((r.registered, r.images), (True, 3))
        folder = IntakeFolder.objects.get()
        self.assertEqual((folder.source, folder.accuracy, folder.priority, folder.duplicate_count),
                         ("kafka", "high", 2, 1))
        self.assertEqual(sorted(IntakeImage.objects.values_list("stage", flat=True)), ["new", "new", "skipped"])

    def test_a_redelivered_message_is_registered_only_once(self):
        self.s.vehicle("low_accuracy/car_1")
        msg = {"folder_id": "car_1", "folder": "low_accuracy/car_1", "accuracy": "low"}
        self.assertTrue(handle(self.s, msg).registered)
        self.assertFalse(handle(self.s, msg).registered)          # Kafka delivered it twice: no harm
        self.assertEqual(IntakeFolder.objects.count(), 1)

    def test_only_the_listed_images_are_taken(self):
        self.s.vehicle("low_accuracy/car_1", contents=(b"a", b"b", b"c"))
        r = handle(self.s, {"folder_id": "car_1", "folder": "low_accuracy/car_1", "accuracy": "low",
                            "images": ["cam_0.jpg", "cam_2.jpg"]})
        self.assertEqual(r.images, 2)

    def test_images_arriving_a_little_late_are_waited_for(self):
        msg = {"folder_id": "car_1", "folder": "low_accuracy/car_1", "accuracy": "low"}
        import threading
        threading.Timer(0.5, lambda: self.s.vehicle("low_accuracy/car_1")).start()
        self.assertTrue(handle(self.s, msg, wait=5).registered)

    def test_preprocessing_switched_off_sends_images_straight_to_ocr(self):
        stage_queue.set_enabled("preprocess", False)
        self.s.vehicle("low_accuracy/car_1")
        handle(self.s, {"folder_id": "car_1", "folder": "low_accuracy/car_1", "accuracy": "low"})
        self.assertEqual(set(IntakeImage.objects.values_list("stage", flat=True)), {"cleaned"})

    def test_bad_messages_are_refused_with_a_reason(self):
        self.s.vehicle("low_accuracy/car_1")
        cases = {
            "not a JSON object": ["a", "list"],
            '"folder_id" is missing': {"folder": "low_accuracy/car_1", "accuracy": "low"},
            '"accuracy" must be': {"folder_id": "c", "folder": "low_accuracy/car_1", "accuracy": "medium"},
            "outside the image storage": {"folder_id": "c", "folder": "../../etc", "accuracy": "low"},
            "file names only": {"folder_id": "c", "folder": "low_accuracy/car_1", "accuracy": "low",
                                "images": ["../secret.jpg"]},
            "still not found": {"folder_id": "c", "folder": "low_accuracy/nope", "accuracy": "low"},
            "its images still not found": {"folder_id": "c", "folder": "low_accuracy/car_1", "accuracy": "low",
                                           "images": ["missing.jpg"]},
        }
        for expected, data in cases.items():
            with self.subTest(expected):
                with self.assertRaisesMessage(BadMessage, expected):
                    handle(self.s, data, wait=0.1)
        self.assertEqual(IntakeFolder.objects.count(), 0)

    def test_priorities_come_from_the_sources(self):
        self.assertEqual(priorities_from({"sources": [{"accuracy": "low", "priority": 5},
                                                      {"accuracy": "high", "priority": 9}]}), {"low": 5, "high": 9})
        self.assertEqual(priorities_from({}), {"low": 1, "high": 2})


class FakeMessage:
    def __init__(self, value, key=b"car_1", partition=3, offset=17):
        self._v, self._k, self._p, self._o = value, key, partition, offset
    def value(self): return self._v
    def key(self): return self._k
    def partition(self): return self._p
    def offset(self): return self._o
    def topic(self): return "anpr.folders"
    def error(self): return None


class KafkaReaderBotTests(TestCase):
    """The bot's handle(): success counts, and bad messages go to the failed topic."""

    def setUp(self):
        from bots.kafka_reader import KafkaReaderBot
        self.s = Storage()
        self.bot = KafkaReaderBot("reader-kafka-test")
        self.bot.root = self.s.root
        self.bot.wait_seconds = 0.1
        self.bot.bot_id = Bot.objects.create(name="reader-kafka-test", stage="reader").id
        self.sent = []

    def tearDown(self):
        self.s.dir.cleanup()

    def run_handle(self, value):
        with patch("bots.kafka_reader.kc.send", side_effect=lambda p, topic, key, data: self.sent.append((topic, data))), \
             patch("bots.kafka_reader.kc.flush"):
            self.bot.handle(FakeMessage(value), producer=None)

    def test_good_message_is_registered_and_counted(self):
        self.s.vehicle("low_accuracy/car_1")
        self.run_handle(json.dumps({"folder_id": "car_1", "folder": "low_accuracy/car_1", "accuracy": "low"}).encode())
        self.assertEqual(IntakeFolder.objects.count(), 1)
        self.assertEqual(Bot.objects.get(pk=self.bot.bot_id).processed, 1)
        self.assertEqual(self.sent, [])

    def test_unreadable_json_goes_to_the_failed_topic_with_its_position(self):
        self.run_handle(b"this is not json")
        topic, data = self.sent[0]
        self.assertEqual(topic, "anpr.failed")
        self.assertEqual((data["partition"], data["offset"], data["message"]), (3, 17, "this is not json"))
        self.assertEqual(Bot.objects.get(pk=self.bot.bot_id).failed, 1)

    def test_bad_message_goes_to_the_failed_topic_with_the_reason(self):
        self.run_handle(json.dumps({"folder_id": "car_1"}).encode())
        self.assertIn('"folder" is missing', self.sent[0][1]["reason"])
        self.assertEqual(IntakeFolder.objects.count(), 0)

class SupervisorKafkaModeTests(TestCase):
    """Kafka mode: Kafka shares the partitions, so the supervisor gives readers no shard
    and never restarts them over one (it used to restart them forever: shard 'kafka' != '1/1')."""

    def test_no_shards_and_no_restarts_in_kafka_mode(self):
        from core.management.commands.bot_supervisor import Command
        reader = Bot.objects.create(name="reader-1", stage="reader", desired_running=True, shard="kafka")

        class AliveProc:
            terminated = False
            def poll(self): return None
            def terminate(self): self.terminated = True

        proc = AliveProc()
        cmd = Command()
        cmd.bcfg = {"stale_after_seconds": 30, "stop_grace_seconds": 60}
        cmd.procs, cmd.stop_asked, cmd.restarting, cmd.shards = {reader.id: proc}, {}, set(), {}
        with patch("core.management.commands.bot_supervisor.reader_mode", return_value="kafka"):
            cmd.reconcile()
        self.assertEqual(cmd.shards, {})
        self.assertFalse(proc.terminated)


class PreprocessSwitchTests(TestCase):
    """Preprocessing can be switched off on the Pipeline page: images then skip it."""

    def test_switching_off_moves_the_waiting_images_to_ocr(self):
        s = Storage()
        self.addCleanup(s.dir.cleanup)
        s.vehicle("car_1")
        handle(s, {"folder_id": "car_1", "folder": "car_1", "accuracy": "low"})
        self.assertEqual(stage_queue.first_image_stage(), "new")
        moved = stage_queue.set_enabled("preprocess", False)
        self.assertEqual(moved, 2)
        self.assertEqual(set(IntakeImage.objects.values_list("stage", flat=True)), {"cleaned"})
        self.assertEqual(stage_queue.first_image_stage(), "cleaned")
        stage_queue.set_enabled("preprocess", True)
        self.assertEqual(stage_queue.first_image_stage(), "new")

    def test_an_image_landing_in_the_old_line_is_moved_by_housekeeping(self):
        from core import housekeeping
        stage_queue.set_enabled("preprocess", False)
        folder = IntakeFolder.objects.create(path="/x/car", name="car", source="t", accuracy="low", priority=1)
        from django.utils import timezone
        IntakeImage.objects.create(folder=folder, file_name="a.jpg", path="/x/car/a.jpg", stage="new",
                                   size_bytes=1, sha256="0" * 64, modified_at=timezone.now())
        self.assertEqual(housekeeping.skip_switched_off(), 1)
        self.assertEqual(IntakeImage.objects.get().stage, "cleaned")

    def test_only_preprocessing_can_be_switched_off(self):
        with self.assertRaisesMessage(ValueError, "can't be switched off"):
            stage_queue.set_enabled("ocr", False)
        with self.assertRaises(ValueError) as e:
            stage_queue.save_settings("ocr", {"enabled": False})
        self.assertIn("enabled", e.exception.args[0])
        with self.assertRaises(ValueError):
            stage_queue.save_settings("preprocess", {"enabled": "no"})

    def test_the_pipeline_api_switches_it_and_reports_it(self):
        from django.contrib.auth.models import User
        from rest_framework.test import APIClient
        client = APIClient()
        client.force_authenticate(User.objects.create_user("u", password="p"))
        r = client.patch("/api/pipeline/preprocess/settings/", {"enabled": False, "batch_size": 7}, format="json")
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual((r.json()["settings"]["enabled"], r.json()["settings"]["batch_size"]), (False, 7))
        stages = {s["stage"]: s for s in client.get("/api/pipeline/").json()["stages"]}
        self.assertEqual((stages["preprocess"]["can_switch_off"], stages["preprocess"]["enabled"]), (True, False))
        self.assertFalse(stages["ocr"]["can_switch_off"])
        self.assertIn(client.get("/api/pipeline/").json()["reader_mode"], ("scan", "kafka"))


class DefaultBotNameTests(TestCase):
    def test_docker_bots_started_with_only_stage_get_a_name(self):
        """docker-compose runs `python -m bots --stage ocr --yes`: the name must default."""
        import sys
        from bots import __main__ as entry
        with patch.object(sys, "argv", ["bots", "--stage", "ocr", "--yes"]), \
             patch("bots.runner.StageBot.run") as run, patch("bots.runner.StageBot.__init__", return_value=None) as init:
            entry.main()
        name, stage = init.call_args.args
        self.assertTrue(name.startswith("ocr-") and stage == "ocr")
        run.assert_called_once()


class RestartAndPrefectTests(TestCase):
    """A restarted bot gives back what it held; Prefect being down never stops a bot."""

    def _image(self, stage, taken_by="", attempts=0, name="a.jpg"):
        from django.utils import timezone
        folder, _ = IntakeFolder.objects.get_or_create(path="/x/car", defaults=dict(name="car", source="t",
                                                                                     accuracy="low", priority=1))
        return IntakeImage.objects.create(folder=folder, file_name=name, path=f"/x/car/{name}", stage=stage,
                                          taken_by=taken_by, attempts=attempts, size_bytes=1, sha256="0" * 64,
                                          modified_at=timezone.now())

    def test_reclaim_gives_back_own_items_and_fails_the_ones_out_of_attempts(self):
        self._image("preprocessing", "pre-1", attempts=1, name="a.jpg")
        self._image("preprocessing", "pre-1", attempts=3, name="b.jpg")
        self._image("preprocessing", "pre-2", attempts=1, name="c.jpg")       # another bot's: untouched
        self.assertEqual(stage_queue.reclaim("preprocess", "pre-1", max_attempts=3), 2)
        stages = dict(IntakeImage.objects.values_list("file_name", "stage"))
        self.assertEqual(stages, {"a.jpg": "new", "b.jpg": "failed", "c.jpg": "preprocessing"})

    def test_batch_runs_without_prefect_when_its_server_is_down(self):
        from bots.runner import StageBot
        self._image("new")
        bot = StageBot("pre-1", "preprocess")
        bot.pipe = {**bot.pipe, "use_prefect": True}
        bot.register()
        items = stage_queue.take("preprocess", "pre-1", 10)
        with patch("bots.prefect_batch.prefect_reachable", return_value=False), \
             patch("bots.prefect_batch.run_batch") as run_batch:
            outcomes = bot.process(items, 1)
        run_batch.assert_not_called()
        self.assertTrue(all(o.ok for o in outcomes))
        from core.models import BotEvent
        self.assertTrue(BotEvent.objects.filter(message__contains="Prefect server not reachable").exists())
        bot._beat_stop.set()

    def test_removing_a_gone_container_gives_back_its_items(self):
        from datetime import timedelta
        from django.utils import timezone
        from core import housekeeping
        Bot.objects.create(name="preprocess-old", stage="preprocess", state="stopped", desired_running=True,
                           last_heartbeat=timezone.now() - timedelta(minutes=2))
        self._image("preprocessing", "preprocess-old", attempts=1)
        self.assertEqual(housekeeping.remove_dead_bots(after_minutes=10), ["preprocess-old"])
        self.assertEqual(IntakeImage.objects.get().stage, "new")
