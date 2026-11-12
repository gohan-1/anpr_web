"""
Assembly-line tests: queue safety, bots, folder routing, and the Pipeline API.
Run: python manage.py test core        (uses a temporary copy of your database)
"""
import threading
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone
from rest_framework.test import APITestCase

from bots.runner import StageBot
from core import stage_queue
from core.models import Bot, BotEvent, IntakeFolder, IntakeImage


def make_folder(name="car", images=3, priority=1, accuracy="low"):
    folder = IntakeFolder.objects.create(path=f"/data/{name}", name=name, source="test",
                                         accuracy=accuracy, priority=priority, image_count=images)
    IntakeImage.objects.bulk_create([
        IntakeImage(folder=folder, file_name=f"cam_{i}.jpg", path=f"/data/{name}/cam_{i}.jpg",
                    size_bytes=1, sha256=f"{name}{i}", modified_at=timezone.now(), priority=priority)
        for i in range(images)])
    return folder


def run_until_line_empty(stage):
    """Runs a real bot in this thread and stops it as soon as its line is empty."""
    bot = StageBot(f"test-{stage}", stage)
    bot.scfg = {**bot.scfg, "placeholder_seconds": 0}
    bot.ctx.api_cfg = {**bot.ctx.api_cfg, "mock": True, "mock_latency_seconds": 0}   # instant fake OCR
    bot.nap = lambda seconds: setattr(bot, "stopping", True)     # empty line -> stop instead of waiting
    bot.run()
    return Bot.objects.get(name=f"test-{stage}")


class ManyBotsTests(TransactionTestCase):
    """Real concurrency: several threads, each with its own database connection."""

    def test_eight_bots_never_take_the_same_image(self):
        for n in range(20):
            make_folder(f"car_{n}", images=20)                      # 400 images waiting
        taken, lock = [], threading.Lock()

        def bot(i):
            try:
                while items := stage_queue.take("preprocess", f"bot-{i}", 15):
                    with lock:
                        taken.extend(item.id for item in items)
            finally:
                connection.close()

        threads = [threading.Thread(target=bot, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(taken), 400)                           # every image taken...
        self.assertEqual(len(set(taken)), 400)                      # ...exactly once
        self.assertGreater(len({i.taken_by for i in IntakeImage.objects.all()}), 1)   # work was shared


class QueueTests(TestCase):
    def test_low_accuracy_comes_out_first(self):
        make_folder("high", images=2, priority=2, accuracy="high")   # arrived first
        make_folder("low", images=2, priority=1)                     # arrived later
        names = [i.folder.name for i in stage_queue.take("preprocess", "b", 2)]
        self.assertEqual(names, ["low", "low"])

    def test_failed_item_retries_then_gives_up(self):
        make_folder(images=1)
        for expected in ("new", "failed"):                            # max_attempts=2
            item = stage_queue.take("preprocess", "b", 1)[0]
            self.assertEqual(stage_queue.fail("preprocess", item, "b", "boom", max_attempts=2), expected)
        self.assertEqual(IntakeImage.objects.get().last_error, "boom")

    def test_release_gives_items_back_without_counting_a_try(self):
        make_folder(images=2)
        items = stage_queue.take("preprocess", "b", 2)
        stage_queue.release("preprocess", items, "b")
        self.assertEqual(list(IntakeImage.objects.values_list("stage", "attempts")), [("new", 0)] * 2)

    def test_stuck_items_return_and_the_old_bot_cannot_overwrite(self):
        make_folder(images=1)
        item = stage_queue.take("preprocess", "dead-bot", 1)[0]
        IntakeImage.objects.update(taken_at=timezone.now() - timedelta(minutes=30))
        self.assertEqual(stage_queue.return_stuck("preprocess", 10, 3), 1)
        again = stage_queue.take("preprocess", "new-bot", 1)[0]
        self.assertFalse(stage_queue.finish("preprocess", item, "dead-bot", "cleaned"))   # too late
        self.assertTrue(stage_queue.finish("preprocess", again, "new-bot", "cleaned"))

    def test_folder_is_routed_exactly_once(self):
        folder = make_folder(images=2)
        IntakeImage.objects.update(stage="read")
        ready = stage_queue.folders_ready([folder.id])
        self.assertEqual(len(ready), 1)
        self.assertTrue(stage_queue.route_folder(ready[0], "needs_trip", "test"))
        self.assertFalse(stage_queue.route_folder(ready[0], "needs_llm", "a second bot, too late"))
        self.assertEqual(IntakeFolder.objects.get().stage, "needs_trip")

    def test_folder_waits_until_every_image_is_finished(self):
        folder = make_folder(images=2)
        IntakeImage.objects.filter(file_name="cam_0.jpg").update(stage="read")
        self.assertEqual(stage_queue.folders_ready([folder.id]), [])


class BotRunTests(TestCase):
    def test_whole_line_from_new_images_to_done(self):
        for n in range(10):
            make_folder(f"car_{n}", images=3)
        for stage in ("preprocess", "ocr", "llm", "trip"):
            bot = run_until_line_empty(stage)
            self.assertEqual(bot.state, Bot.State.STOPPED)
        self.assertEqual(IntakeImage.objects.exclude(stage="read").count(), 0)
        stages = dict(IntakeFolder.objects.values_list("name", "stage"))
        self.assertTrue(set(stages.values()) <= {"done", "manual"})
        self.assertEqual(Bot.objects.get(name="test-preprocess").processed, 30)
        self.assertTrue(IntakeImage.objects.exclude(cleaned_path="").exists())
        # each stage counts its own tries: finished items carry no attempts into the next stage
        self.assertEqual(IntakeImage.objects.exclude(attempts=0).count(), 0)

    def test_backpressure_pauses_when_the_next_line_is_full(self):
        make_folder(images=5)
        bot = StageBot("test-bp", "preprocess")
        bot.scfg = {**bot.scfg, "pause_if_next_line_over": 0}
        IntakeImage.objects.filter(file_name="cam_0.jpg").update(stage="cleaned")   # OCR line not empty
        self.assertTrue(bot.line_too_full())


class PipelineApiTests(APITestCase):
    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")
        token = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"},
                                 format="json").data["token"]
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {token}")

    def test_overview_lists_every_stage_in_order(self):
        make_folder(images=4)
        r = self.client.get("/api/pipeline/")
        self.assertEqual([s["stage"] for s in r.data["stages"]], ["reader", "preprocess", "ocr", "llm", "trip"])
        self.assertEqual(r.data["stages"][1]["waiting"], 4)

    def test_scale_up_and_down(self):
        r = self.client.post("/api/pipeline/ocr/scale/", {"count": 3}, format="json")
        self.assertEqual(r.status_code, 200)                     # the page uses this reply right away
        self.assertEqual({x["stage"]: x["bots_wanted"] for x in r.data["stages"]}["ocr"], 3)
        self.assertEqual(sorted(Bot.objects.filter(stage="ocr").values_list("name", flat=True)),
                         ["ocr-1", "ocr-2", "ocr-3"])
        Bot.objects.filter(name="ocr-3").update(state=Bot.State.RUNNING, pid=123)   # pretend it runs
        self.client.post("/api/pipeline/ocr/scale/", {"count": 1}, format="json")
        bots = {b.name: b for b in Bot.objects.filter(stage="ocr")}
        self.assertTrue(bots["ocr-3"].delete_requested)          # running: finishes its batch first
        self.assertNotIn("ocr-2", bots)                         # never started: removed at once
        self.assertFalse(bots["ocr-1"].delete_requested)

    def test_scale_rejects_bad_input(self):
        self.assertEqual(self.client.post("/api/pipeline/nope/scale/", {"count": 1}, format="json").status_code, 404)
        self.assertEqual(self.client.post("/api/pipeline/ocr/scale/", {"count": 999}, format="json").status_code, 400)

    def test_bots_can_be_filtered_by_stage(self):
        self.client.post("/api/pipeline/llm/scale/", {"count": 2}, format="json")
        self.client.post("/api/pipeline/trip/scale/", {"count": 1}, format="json")
        names = [b["name"] for b in self.client.get("/api/bots/?stage=llm").data["results"]]
        self.assertEqual(names, ["llm-1", "llm-2"])


class MaintenanceTests(APITestCase):
    """What the Pipeline page uses to maintain the line: settings, failed items, retry."""

    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")
        token = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"},
                                 format="json").data["token"]
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {token}")

    def fail_all(self, stage, bot="b1"):
        """Takes every item of the stage's line and fails it for good."""
        items = stage_queue.take(stage, bot, 1000)
        for item in items:
            stage_queue.fail(stage, item, bot, "engine said no", max_attempts=1)
        return items

    def test_settings_change_is_saved_validated_and_reset(self):
        r = self.client.patch("/api/pipeline/ocr/settings/", {"batch_size": 50}, format="json")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(stage_queue.settings_of("ocr")["batch_size"], 50)
        overview = {s["stage"]: s for s in self.client.get("/api/pipeline/").data["stages"]}
        self.assertEqual(overview["ocr"]["editable"]["batch_size"]["value"], 50)
        self.assertEqual(overview["ocr"]["editable"]["batch_size"]["default"], 20)
        bad = self.client.patch("/api/pipeline/ocr/settings/", {"batch_size": 0, "placeholder_seconds": 1}, format="json")
        self.assertEqual(bad.status_code, 400)
        self.assertEqual(set(bad.data), {"batch_size", "placeholder_seconds"})
        self.client.patch("/api/pipeline/ocr/settings/", {"batch_size": None}, format="json")   # back to default
        self.assertEqual(stage_queue.settings_of("ocr")["batch_size"], 20)

    def test_running_bot_picks_up_new_settings_before_its_next_batch(self):
        make_folder(images=7)
        stage_queue.save_settings("preprocess", {"batch_size": 2})
        bot = StageBot("test-live", "preprocess")
        sizes = []
        real_process = bot.process

        def watch(items, n):
            sizes.append(len(items))
            return real_process(items, n)
        bot.process = watch
        bot.nap = lambda seconds: setattr(bot, "stopping", True)
        bot.run()
        self.assertEqual(sizes, [2, 2, 2, 1])

    def test_failed_images_are_listed_and_retried_into_the_line_they_failed_in(self):
        folder = make_folder(images=3)
        IntakeImage.objects.filter(file_name="cam_0.jpg").update(stage="cleaned")   # 1 waits for OCR
        self.fail_all("preprocess")                                                 # 2 fail in preprocessing
        self.fail_all("ocr")                                                        # 1 fails in OCR
        stage_queue.route_folder(folder, "manual", "No image could be read")
        listed = self.client.get("/api/pipeline/failed/?level=image").data
        self.assertEqual(sorted(i["failed_in"] for i in listed), ["ocr", "preprocess", "preprocess"])
        self.assertEqual({s["stage"]: s["failed"] for s in self.client.get("/api/pipeline/").data["stages"]
                          }["preprocess"], 2)
        r = self.client.post("/api/pipeline/retry/", {"level": "image"}, format="json")
        self.assertEqual(r.data, {"moved": 3, "folders_reopened": 1})
        self.assertEqual(sorted(IntakeImage.objects.values_list("stage", flat=True)), ["cleaned", "new", "new"])
        folder.refresh_from_db()
        self.assertEqual(folder.stage, "reading")                 # decided again once they're through
        self.assertEqual(IntakeImage.objects.exclude(attempts=0).count(), 0)

    def test_retry_only_chosen_ids_and_never_reopens_a_done_folder(self):
        done = make_folder("done_car", images=1)
        self.fail_all("preprocess")
        IntakeFolder.objects.filter(pk=done.pk).update(stage="done")
        other = make_folder("other", images=2)
        failed = self.fail_all("preprocess")
        r = self.client.post("/api/pipeline/retry/", {"level": "image", "ids": [failed[0].id]}, format="json")
        self.assertEqual(r.data["moved"], 1)
        self.assertEqual(IntakeImage.objects.filter(folder=other, stage="failed").count(), 1)
        self.client.post("/api/pipeline/retry/", {"level": "image"}, format="json")
        self.assertEqual(IntakeImage.objects.get(folder=done).stage, "failed")   # its trip is already made
        self.assertEqual(self.client.post("/api/pipeline/retry/", {"level": "x"}, format="json").status_code, 400)

    def test_failed_folders_go_back_to_the_llm_or_trip_line(self):
        a, b = make_folder("a", images=1), make_folder("b", images=1)
        IntakeFolder.objects.filter(pk=a.pk).update(stage="needs_llm")
        IntakeFolder.objects.filter(pk=b.pk).update(stage="needs_trip")
        self.fail_all("llm")
        self.fail_all("trip")
        self.client.post("/api/pipeline/retry/", {"level": "folder"}, format="json")
        self.assertEqual(dict(IntakeFolder.objects.values_list("name", "stage")), {"a": "needs_llm", "b": "needs_trip"})


from unittest.mock import patch  # noqa: E402

from core.models import Folder  # noqa: E402
from core.ocr_results import decide  # noqa: E402


def ocr_bot(name="ocr-t", **row):
    """A real OCR bot object, with the fake OCR API answering instantly."""
    Bot.objects.create(name=name, stage="ocr", desired_running=True, **row)
    bot = StageBot(name, "ocr")
    bot.bot_id = Bot.objects.get(name=name).id
    bot.ctx.api_cfg = {**bot.ctx.api_cfg, "mock": True, "mock_latency_seconds": 0, "retries": 1,
                       "retry_delay_seconds": 0}
    bot.nap = lambda seconds: setattr(bot, "stopping", True)
    return bot


class OcrStageTests(TestCase):
    """The OCR stage does what the classic bots did: the real API call, per bot account."""

    def setUp(self):
        self.folder = make_folder("car", images=3)
        IntakeImage.objects.update(stage="cleaned")

    def test_reads_every_image_and_routes_the_folder_with_its_result(self):
        bot = ocr_bot()
        bot.run()
        images = list(IntakeImage.objects.all())
        self.assertTrue(all(i.stage == "read" and i.read_by == "ocr-t" for i in images))
        self.assertTrue(all(i.duration_ms is not None and i.api_attempts == 1 for i in images))
        self.assertTrue(any(i.plate for i in images))
        folder = IntakeFolder.objects.get()
        self.assertIn(folder.stage, ("needs_trip", "needs_llm", "manual"))
        self.assertIsNotNone(folder.read_at)
        result = Folder.objects.get()                              # shown on the Folders page
        self.assertEqual((result.name, result.processed_by, result.images.count()), ("car", "ocr-t", 3))
        self.assertEqual(result.final_plate, folder.plate)

    def test_route_follows_the_confidence(self):
        imgs = list(IntakeImage.objects.order_by("id"))
        for i, (plate, conf) in zip(imgs, [("AB12", 0.95), ("AB12", 0.7), ("", None)]):
            i.stage, i.plate, i.confidence = "read", plate, conf
        cfg = {"trip_if_confidence_at_least": 0.9}
        self.assertEqual(decide(imgs, cfg)[0], "needs_trip")
        self.assertIn("2 image(s) agree", decide(imgs, cfg)[1])
        self.assertEqual(decide(imgs, {"trip_if_confidence_at_least": 0.99})[0], "needs_llm")
        for i in imgs:
            i.plate = ""
        self.assertEqual(decide(imgs, cfg)[0], "manual")
        for i in imgs:
            i.stage = "failed"
        self.assertEqual(decide(imgs, cfg)[:2], ("manual", "No image could be read by the OCR API"))

    def test_api_failure_is_retried_then_failed_and_the_folder_still_decided(self):
        stage_queue.save_settings("ocr", {"max_attempts": 2})      # as the Pipeline page does
        bot = ocr_bot()
        with patch("pipeline.api_client.recognize", side_effect=ConnectionError("API down")) as api:
            bot.run()                       # 1st try fails -> back in the line; 2nd try fails -> given up
        self.assertEqual(api.call_count, 3 * 2 * 2)                 # 3 images x 2 tries x (1 call + 1 retry)
        self.assertEqual(IntakeImage.objects.filter(stage="failed", failed_in="ocr", attempts=2).count(), 3)
        self.assertIn("API down", IntakeImage.objects.first().last_error)
        self.assertEqual(IntakeFolder.objects.get().stage, "manual")
        self.assertEqual(Bot.objects.get(name="ocr-t").failed, 6)

    @override_settings()
    def test_real_api_without_account_waits_instead_of_failing(self):
        bot = ocr_bot()
        bot.ctx.api_cfg["mock"] = False
        self.assertFalse(bot.refresh_account())
        bot.run()
        self.assertEqual(IntakeImage.objects.filter(stage="cleaned").count(), 3)    # nothing taken
        self.assertTrue(BotEvent.objects.filter(message__contains="no API account").exists())
        row = Bot.objects.get(name="ocr-t")
        row.api_username = "acct"
        row.set_password("pw")
        row.rate_limit = 4
        row.save()
        self.assertTrue(bot.refresh_account())                     # edits on the Bots page apply live
        self.assertEqual(bot.ctx.credentials, ("acct", "pw"))
        self.assertEqual(bot.ctx.limiter.per_second, 4)

    def test_supervisor_routes_a_folder_nobody_finished(self):
        from core.management.commands.bot_supervisor import Command
        IntakeImage.objects.update(stage="read", plate="AB12", confidence=0.97, read_by="ocr-1")
        self.assertEqual(Command().route_forgotten_folders(), 1)
        self.assertEqual(IntakeFolder.objects.get().stage, "needs_trip")
        self.assertEqual(Command().route_forgotten_folders(), 0)   # exactly once

