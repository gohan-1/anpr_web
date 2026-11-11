"""Tests for reviewer actions. Run from the web folder: python manage.py test core"""
from django.test import TestCase

from .models import Folder, Job, Route
from .services import ReviewError, apply_review


class ReviewServiceTests(TestCase):
    def setUp(self):
        job = Job.objects.create(input_path="/data")
        self.folder = Folder.objects.create(job=job, path="/data/v1", name="v1", route=Route.REVIEW,
                                            scenario="S7", final_plate="MH12AB1234", final_score=0.7)

    def test_approve(self):
        apply_review(self.folder, "approve", "sam")
        self.folder.refresh_from_db()
        self.assertEqual((self.folder.route, self.folder.reviewed_plate, self.folder.reviewed_by),
                         (Route.ACCEPTED, "MH12AB1234", "sam"))

    def test_correct_cleans_plate(self):
        apply_review(self.folder, "correct", "sam", "mh-12 ab 1284")
        self.folder.refresh_from_db()
        self.assertEqual(self.folder.reviewed_plate, "MH12AB1284")

    def test_correct_rejects_bad_format(self):
        with self.assertRaises(ReviewError):
            apply_review(self.folder, "correct", "sam", "ab")

    def test_reject(self):
        apply_review(self.folder, "reject", "sam")
        self.folder.refresh_from_db()
        self.assertEqual(self.folder.route, Route.NO_PLATE)

    def test_approve_without_plate_fails(self):
        self.folder.final_plate = ""
        with self.assertRaises(ReviewError):
            apply_review(self.folder, "approve", "sam")


# ---------------------------------------------------------------- API tests
from django.contrib.auth import get_user_model  # noqa: E402
from rest_framework.test import APITestCase  # noqa: E402

from .models import Image  # noqa: E402


class ApiTests(APITestCase):
    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")
        job = Job.objects.create(input_path="/data", folder_count=2)
        self.low = Folder.objects.create(job=job, path="/d/a", name="car_a", route=Route.REVIEW,
                                         scenario="S7", final_plate="MH12AB1234", final_score=0.7)
        self.ok = Folder.objects.create(job=job, path="/d/b", name="car_b", route=Route.ACCEPTED,
                                        scenario="S1", final_plate="KA01CD5678", final_score=0.95)
        Image.objects.create(folder=self.low, path="/missing.jpg", file_name="cam_1.jpg", sha256="x",
                             quality=0.8, plate="MH12AB1234", confidence=0.7, final_score=0.73)

    def login(self):
        r = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"}, format="json")
        self.assertEqual(r.status_code, 200)
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {r.data['token']}")

    def test_needs_login(self):
        self.assertEqual(self.client.get("/api/stats/").status_code, 401)

    def test_wrong_password(self):
        r = self.client.post("/api/auth/login/", {"username": "sam", "password": "nope"}, format="json")
        self.assertEqual(r.status_code, 400)

    def test_stats(self):
        self.login()
        r = self.client.get("/api/stats/")
        self.assertEqual(r.data["total"], 2)
        self.assertEqual(r.data["routes"]["review"], 1)
        self.assertIn("accept_score", r.data["thresholds"])

    def test_folder_list_filters(self):
        self.login()
        r = self.client.get("/api/folders/", {"route": "review"})
        self.assertEqual([f["name"] for f in r.data["results"]], ["car_a"])
        r = self.client.get("/api/folders/", {"q": "ka01"})
        self.assertEqual([f["name"] for f in r.data["results"]], ["car_b"])

    def test_review_next_and_approve(self):
        self.login()
        r = self.client.get("/api/review/next/")
        self.assertEqual((r.data["name"], r.data["queue_left"], len(r.data["images"])), ("car_a", 1, 1))
        r = self.client.post(f"/api/folders/{self.low.id}/review/", {"action": "approve"}, format="json")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.data["folder"]["route"], "accepted")
        self.assertEqual(self.client.get("/api/review/next/").status_code, 204)   # queue empty

    def test_bad_correction_returns_400(self):
        self.login()
        r = self.client.post(f"/api/folders/{self.low.id}/review/", {"action": "correct", "plate": "ab"},
                             format="json")
        self.assertEqual(r.status_code, 400)
        self.assertIn("doesn't match", r.data["detail"])

    def test_logout_kills_token(self):
        self.login()
        self.assertEqual(self.client.post("/api/auth/logout/").status_code, 204)
        self.assertEqual(self.client.get("/api/auth/me/").status_code, 401)


# ---------------------------------------------------------------- bots and queue
import tempfile  # noqa: E402
import threading  # noqa: E402
from pathlib import Path  # noqa: E402

from django.db import connection  # noqa: E402

from . import queue  # noqa: E402
from .models import Bot, WorkItem  # noqa: E402


def make_bot(name="bot-1", **kw):
    bot = Bot(name=name, api_username="acct", **kw)
    bot.set_password("s3cret")
    bot.save()
    return bot


class QueueTests(TestCase):
    def test_enqueue_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            for n in range(5):
                (Path(d) / f"car_{n}").mkdir()
            (Path(d) / "not_a_folder.txt").write_text("x")
            self.assertEqual(queue.enqueue_folders(Path(d)), 5)
            self.assertEqual(queue.enqueue_folders(Path(d)), 0)    # nothing queued twice

    def test_two_bots_never_get_the_same_folder(self):
        WorkItem.objects.bulk_create([WorkItem(path=f"/d/{n}") for n in range(10)])
        a, b = make_bot("a"), make_bot("b")
        got_a = {i.path for i in queue.claim(a, 6)}
        got_b = {i.path for i in queue.claim(b, 6)}
        self.assertEqual((len(got_a), len(got_b)), (6, 4))
        self.assertFalse(got_a & got_b)

    def test_fail_retries_then_gives_up(self):
        WorkItem.objects.create(path="/d/x")
        bot = make_bot()
        for attempt in (1, 2, 3):
            item = queue.claim(bot, 1)[0]
            status = queue.fail(item, "boom", max_attempts=3)
        self.assertEqual(status, WorkItem.Status.FAILED)
        self.assertEqual(queue.retry_failed(), 1)

    def test_stale_items_return_to_queue(self):
        WorkItem.objects.create(path="/d/x")
        bot = make_bot()
        queue.claim(bot, 1)
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.CRASHED)   # its bot died
        self.assertEqual(queue.requeue_stale(600, 3), 1)
        self.assertEqual(WorkItem.objects.get().status, WorkItem.Status.PENDING)

    def test_bot_with_input_folder_only_gets_its_own_folders(self):
        with tempfile.TemporaryDirectory() as d:
            gate1 = Path(d) / "gate1"
            gate1.mkdir()
            for n in range(3):
                (gate1 / f"car_g{n}").mkdir()
                (Path(d) / f"car_s{n}").mkdir()
            owner = make_bot("owner", input_dir=str(gate1))
            shared = make_bot("shared")
            added = queue.scan_all(Path(d), 0)
            self.assertEqual(sorted(added.values()), [3, 3])      # gate1 itself is NOT a vehicle folder
            got_owner = {Path(i.path).name for i in queue.claim(owner, 10)}
            got_shared = {Path(i.path).name for i in queue.claim(shared, 10)}
            self.assertEqual(got_owner, {"car_g0", "car_g1", "car_g2"})
            self.assertEqual(got_shared, {"car_s0", "car_s1", "car_s2"})

    def test_bot_given_a_single_vehicle_folder_processes_it(self):
        with tempfile.TemporaryDirectory() as d:
            car = Path(d).resolve() / "vehicle_002"
            car.mkdir()
            (car / "a.jpg").write_bytes(b"x")
            bot = make_bot("solo", input_dir=str(car))
            queue.scan_all(None, 0)
            queue.scan_all(None, 0)                                   # not queued twice
            self.assertEqual([i.path for i in queue.claim(bot, 10)], [str(car)])

    def test_choosing_an_already_queued_folder_moves_it_to_the_bot(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            for n in range(3):
                (root / f"ocr_{n}").mkdir()
                (root / f"ocr_{n}" / "a.jpg").write_bytes(b"x")
            bot = make_bot("b1")
            queue.scan_all(root, 0)                                   # all 3 queued as shared
            self.assertTrue(all(w.assigned_bot is None for w in WorkItem.objects.all()))
            Bot.objects.filter(pk=bot.pk).update(input_dir=str(root / "ocr_1"))
            bot.refresh_from_db()
            queue.scan_all(root, 0)
            self.assertEqual([i.path for i in queue.claim(bot, 10)], [str(root / "ocr_1")])
            # switching to another folder: the old one (if still waiting) goes back to the shared pool
            WorkItem.objects.filter(path=str(root / "ocr_1")).update(status=WorkItem.Status.PENDING, bot=None)
            Bot.objects.filter(pk=bot.pk).update(input_dir=str(root / "ocr_2"))
            queue.scan_all(root, 0)
            self.assertIsNone(WorkItem.objects.get(path=str(root / "ocr_1")).assigned_bot)
            self.assertEqual(WorkItem.objects.get(path=str(root / "ocr_2")).assigned_bot, bot)

    def test_save_folder_result_stores_ocr_without_deciding(self):
        from pipeline.processor import ImageAnalysis
        from .ingest import save_folder_result
        imgs = [ImageAnalysis(Path("/x/a.jpg"), plate="MH12AB1234", confidence=0.62, raw={"r": 1}),
                ImageAnalysis(Path("/x/b.jpg"), plate="MH12AB1284", confidence=0.91),
                ImageAnalysis(Path("/x/c.jpg"), error="HTTPStatusError: 400")]
        f = save_folder_result(Path("/x"), imgs, processed_by="bot-1")
        self.assertEqual((f.route, f.final_plate, f.final_score, f.scenario), ("processed", "MH12AB1284", 0.91, ""))
        self.assertEqual(f.images.count(), 3)
        failed = save_folder_result(Path("/y"), [ImageAnalysis(Path("/y/a.jpg"), error="timeout")])
        self.assertEqual(failed.route, "failed")

    def test_password_is_encrypted(self):
        bot = make_bot()
        self.assertNotIn("s3cret", bot.api_password_encrypted)
        self.assertEqual(Bot.objects.get().get_password(), "s3cret")


class BotApiTests(APITestCase):
    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")
        token = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"}, format="json").data["token"]
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {token}")

    def test_create_never_returns_password(self):
        r = self.client.post("/api/bots/", {"name": "bot-1", "api_username": "acct", "password": "pw",
                                            "concurrency": 8}, format="json")
        self.assertEqual(r.status_code, 201)
        self.assertNotIn("password", r.data)
        self.assertNotIn("api_password_encrypted", r.data)

    def test_create_validates(self):
        r = self.client.post("/api/bots/", {"name": "bad name!", "api_username": "a", "concurrency": 999}, format="json")
        self.assertEqual(r.status_code, 400)
        self.assertIn("name", r.data)          # "bad name!" has a space and a "!"
        self.assertIn("concurrency", r.data)   # 999 is above the limit
        self.assertIn("password", r.data)      # a new bot needs a password

    def test_start_stop_delete(self):
        bot = make_bot()
        self.assertTrue(self.client.post(f"/api/bots/{bot.id}/start/").data["desired_running"])
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.RUNNING)
        r = self.client.delete(f"/api/bots/{bot.id}/")
        self.assertEqual(r.status_code, 202)                 # running: asked to finish, then removed
        self.assertTrue(Bot.objects.get().delete_requested)
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.STOPPED)
        stopped = make_bot("idle")
        self.assertEqual(self.client.delete(f"/api/bots/{stopped.id}/").status_code, 204)   # removed now

    def test_queue_add_and_status(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "car_1").mkdir()
            r = self.client.post("/api/queue/add/", {"path": d}, format="json")
            self.assertEqual(r.data["added"], 1)
        self.assertEqual(self.client.get("/api/queue/").data["counts"]["pending"], 1)
        r = self.client.post("/api/queue/add/", {"path": "/no/such/folder"}, format="json")
        self.assertEqual(r.status_code, 400)


class SpeedTests(TestCase):
    def test_speed_uses_time_actually_covered(self):
        from datetime import timedelta
        from django.utils import timezone
        now = timezone.now()
        # 60 folders finished over the last minute -> about 60/min, not 12/min
        WorkItem.objects.bulk_create([
            WorkItem(path=f"/d/{n}", status=WorkItem.Status.DONE, finished_at=now - timedelta(seconds=n))
            for n in range(60)])
        self.assertAlmostEqual(queue.folders_per_minute(WorkItem.objects.all()), 60, delta=2)


class BrowseTests(TestCase):
    def setUp(self):
        from django.contrib.auth.models import User
        from rest_framework.test import APIClient
        self.client = APIClient()
        self.client.force_authenticate(User.objects.create_user("u", password="p"))
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        (self.root / "gate1" / "car_1").mkdir(parents=True)
        (self.root / ".hidden").mkdir()
        from unittest import mock
        cfg = {"paths": {"browse_roots": [str(self.root)], "input_dir": str(self.root)}}
        self.patch = mock.patch("core.api.load_config", return_value=cfg)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_roots_then_subfolders(self):
        r = self.client.get("/api/browse/").json()
        self.assertEqual(r["dirs"], [{"name": str(self.root), "path": str(self.root)}])
        r = self.client.get("/api/browse/", {"path": str(self.root)}).json()
        self.assertEqual([d["name"] for d in r["dirs"]], ["gate1"])        # hidden folders skipped
        self.assertIsNone(r["parent"])                                     # can't go above the root
        r = self.client.get("/api/browse/", {"path": str(self.root / "gate1")}).json()
        self.assertEqual(r["parent"], str(self.root))

    def test_outside_roots_is_forbidden(self):
        for bad in ["/etc", str(self.root / ".." )]:
            self.assertEqual(self.client.get("/api/browse/", {"path": bad}).status_code, 403)

    def test_needs_login(self):
        from rest_framework.test import APIClient
        self.assertEqual(APIClient().get("/api/browse/").status_code, 401)
