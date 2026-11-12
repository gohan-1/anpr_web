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


# ---------------------------------------------------------------- bots
from pathlib import Path  # noqa: E402

from .models import Bot  # noqa: E402


def make_bot(name="ocr-1", stage="ocr", **kw):
    bot = Bot(name=name, stage=stage, api_username="acct", **kw)
    bot.set_password("s3cret")
    bot.save()
    return bot


class BotApiTests(APITestCase):
    """The Bots page: every bot belongs to a stage; OCR bots carry their own API account."""

    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")
        token = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"}, format="json").data["token"]
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {token}")

    def test_password_is_encrypted(self):
        bot = make_bot()
        self.assertNotIn("s3cret", bot.api_password_encrypted)
        self.assertEqual(Bot.objects.get().get_password(), "s3cret")

    def test_create_ocr_bot_with_account_never_returns_password(self):
        r = self.client.post("/api/bots/", {"name": "ocr-gate1", "stage": "ocr", "api_username": "acct",
                                            "password": "pw", "rate_limit": 2}, format="json")
        self.assertEqual(r.status_code, 201)
        self.assertNotIn("password", r.data)
        self.assertNotIn("api_password_encrypted", r.data)
        self.assertTrue(r.data["has_account"])
        self.assertFalse(r.data["desired_running"])          # created switched off

    def test_create_validates(self):
        r = self.client.post("/api/bots/", {"name": "bad name!", "stage": "nope", "api_username": "a",
                                            "rate_limit": -1}, format="json")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(set(r.data), {"name", "stage", "rate_limit"})
        half = self.client.post("/api/bots/", {"name": "ocr-x", "stage": "ocr", "api_username": "a"}, format="json")
        self.assertIn("password", half.data)                   # a username needs its password
        dup = self.client.post("/api/bots/", {"name": "ocr-x", "stage": "trip"}, format="json")
        self.assertEqual(dup.status_code, 201)
        self.assertIn("name", self.client.post("/api/bots/", {"name": "ocr-x", "stage": "llm"}, format="json").data)

    def test_any_stage_without_account(self):
        for stage in ("reader", "preprocess", "ocr", "llm", "trip"):
            r = self.client.post("/api/bots/", {"name": f"{stage}-a", "stage": stage}, format="json")
            self.assertEqual(r.status_code, 201, r.data)
        self.assertFalse(Bot.objects.get(name="ocr-a").api_password_encrypted)

    def test_edit_account_and_rate_limit_but_never_the_stage(self):
        bot = make_bot()
        r = self.client.patch(f"/api/bots/{bot.id}/", {"rate_limit": 5, "password": "new-pw"}, format="json")
        self.assertEqual(r.data["rate_limit"], 5)
        self.assertEqual(Bot.objects.get().get_password(), "new-pw")
        self.client.patch(f"/api/bots/{bot.id}/", {"rate_limit": 1}, format="json")     # no password: kept
        self.assertEqual(Bot.objects.get().get_password(), "new-pw")
        self.assertIn("stage", self.client.patch(f"/api/bots/{bot.id}/", {"stage": "llm"}, format="json").data)
        r = self.client.patch(f"/api/bots/{bot.id}/", {"api_username": ""}, format="json")   # remove the account
        self.assertFalse(r.data["has_account"])

    def test_list_filters_by_stage(self):
        make_bot("ocr-1")
        make_bot("trip-1", stage="trip")
        names = [b["name"] for b in self.client.get("/api/bots/?stage=trip").data["results"]]
        self.assertEqual(names, ["trip-1"])

    def test_start_stop_delete(self):
        bot = make_bot()
        self.assertTrue(self.client.post(f"/api/bots/{bot.id}/start/").data["desired_running"])
        self.assertFalse(self.client.post(f"/api/bots/{bot.id}/stop/").data["desired_running"])
        self.client.post(f"/api/bots/{bot.id}/start/")
        Bot.objects.filter(pk=bot.pk).update(state=Bot.State.RUNNING)
        r = self.client.delete(f"/api/bots/{bot.id}/")
        self.assertEqual(r.status_code, 202)                 # running: asked to finish, then removed
        self.assertTrue(Bot.objects.get().delete_requested)
        self.assertEqual(self.client.post(f"/api/bots/{bot.id}/start/").status_code, 409)
        stopped = make_bot("idle")
        self.assertEqual(self.client.delete(f"/api/bots/{stopped.id}/").status_code, 204)   # removed now


# ============================================================================ reset_results
from django.contrib.auth import get_user_model as _get_user_model  # noqa: E402
from django.core.management import CommandError, call_command  # noqa: E402
from django.utils import timezone  # noqa: E402

from .models import BotEvent, IntakeFolder, IntakeImage, StageSettings  # noqa: E402
from .ocr_results import save_result  # noqa: E402


def registered(name, stage="done", image_stage="read"):
    f = IntakeFolder.objects.create(path=f"/d/{name}", name=name, source="t", accuracy="high", priority=1,
                                    image_count=1, stage=stage)
    IntakeImage.objects.create(folder=f, file_name="a.jpg", path=f"/d/{name}/a.jpg", size_bytes=1, sha256=name,
                               modified_at=timezone.now(), stage=image_stage, plate="AB12", confidence=0.9,
                               read_by="ocr-1")
    save_result(f, list(f.images.all()), "test")
    return f


class ResetResultsTests(TestCase):
    def setUp(self):
        self.user = _get_user_model().objects.create_user("keep-me", password="x")
        self.bot = make_bot("ocr-1")
        Bot.objects.filter(pk=self.bot.pk).update(processed=2, failed=1)
        BotEvent.objects.create(bot=self.bot, message="hello")
        StageSettings.objects.create(stage="ocr", values={"batch_size": 7})
        registered("ocr_1")
        registered("ocr_2")
        registered("ocr_3", stage="reading", image_stage="ocr_running")     # a bot is reading it now

    def test_reset_all_deletes_results_and_keeps_bots_settings_and_users(self):
        call_command("reset_results", "--yes", stdout=open("/dev/null", "w"))
        self.assertEqual((IntakeFolder.objects.count(), IntakeImage.objects.count(), Folder.objects.count(),
                          Image.objects.count(), BotEvent.objects.count()), (0, 0, 0, 0, 0))
        bot = Bot.objects.get()
        self.assertEqual((bot.name, bot.processed, bot.failed), ("ocr-1", 0, 0))
        self.assertTrue(bot.api_password_encrypted)                   # API account kept
        self.assertEqual(StageSettings.objects.get().values, {"batch_size": 7})
        self.assertTrue(_get_user_model().objects.filter(username="keep-me").exists())

    def test_reprocess_one_folder_deletes_only_that_folder(self):
        call_command("reset_results", "--folder", "ocr_1", "--yes", stdout=open("/dev/null", "w"))
        self.assertEqual(sorted(IntakeFolder.objects.values_list("name", flat=True)), ["ocr_2", "ocr_3"])
        self.assertEqual(sorted(Folder.objects.values_list("name", flat=True)), ["ocr_2", "ocr_3"])
        self.assertEqual(Image.objects.count(), 2)

    def test_folder_a_bot_is_working_on_is_not_touched(self):
        with self.assertRaises(CommandError):                          # only match is being read -> nothing to do
            call_command("reset_results", "--folder", "ocr_3", "--yes", stdout=open("/dev/null", "w"))
        self.assertTrue(IntakeFolder.objects.filter(name="ocr_3").exists())

    def test_reprocessed_folder_is_registered_again_by_the_next_scan(self):
        import tempfile
        from services.reader.scanner import Shard, Source, scan_source
        with tempfile.TemporaryDirectory() as d:
            car = Path(d).resolve() / "ocr_9"
            car.mkdir()
            (car / "a.jpg").write_bytes(b"x")
            src = Source(name="t", path=Path(d).resolve(), accuracy="high", priority=1)
            scan = lambda: scan_source(src, Shard.parse("1/1"), "test", extensions={".jpg"},   # noqa: E731
                                       min_age_seconds=0, workers=1).registered
            self.assertEqual(scan(), 1)
            self.assertEqual(scan(), 0)                                   # known: not again
            call_command("reset_results", "--folder", str(car), "--yes", stdout=open("/dev/null", "w"))
            self.assertEqual(scan(), 1)                                   # registered again, images back at the start
            self.assertEqual(IntakeImage.objects.get(folder__path=str(car)).stage, "new")

    def test_unknown_folder_is_an_error(self):
        with self.assertRaises(CommandError):
            call_command("reset_results", "--folder", "nope", "--yes", stdout=open("/dev/null", "w"))


# ---------------------------------------------------------------- environments and safety
from unittest.mock import patch  # noqa: E402

from django.core.management import call_command  # noqa: E402
from django.core.management.base import CommandError  # noqa: E402
from django.test import override_settings  # noqa: E402


class EnvironmentTests(APITestCase):
    def setUp(self):
        get_user_model().objects.create_user("sam", password="pass12345")

    def login(self):
        r = self.client.post("/api/auth/login/", {"username": "sam", "password": "pass12345"}, format="json")
        self.assertEqual(r.status_code, 200)      # signing in works even when read-only
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {r.data['token']}")

    def test_env_endpoint_is_public_and_hides_password(self):
        r = self.client.get("/api/env/")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(set(r.data), {"env", "read_only", "database"})

    @override_settings(READ_ONLY=True, ANPR_ENV="production")
    def test_read_only_blocks_every_change(self):
        self.login()
        self.assertEqual(self.client.get("/api/stats/").status_code, 200)        # reading is fine
        r = self.client.post("/api/bots/", {"name": "x", "api_username": "a", "password": "p"}, format="json")
        self.assertEqual(r.status_code, 403)
        self.assertIn("read-only", r.json()["detail"])
        self.assertEqual(self.client.post("/admin/core/bot/add/").status_code, 403)   # admin too

    @override_settings(READ_ONLY=True, ANPR_ENV="production")
    def test_read_only_blocks_commands(self):
        with self.assertRaisesMessage(CommandError, "read-only"):
            call_command("reset_results", "--yes")
        with self.assertRaisesMessage(CommandError, "read-only"):
            call_command("bot_supervisor", "--yes")

    @override_settings(IS_LOCAL=False, ANPR_ENV="staging")
    def test_staging_needs_typed_confirmation(self):
        registered("car_1")
        quiet = open("/dev/null", "w")
        with patch("builtins.input", side_effect=["yes"]):               # "yes" is not enough
            with self.assertRaisesMessage(CommandError, "Cancelled"):
                call_command("reset_results", stdout=quiet)
        self.assertEqual(IntakeFolder.objects.count(), 1)
        with patch("builtins.input", side_effect=["staging", "yes"]):    # the name is, then the usual "yes"
            call_command("reset_results", stdout=quiet)
        self.assertEqual(IntakeFolder.objects.count(), 0)

    @override_settings(IS_LOCAL=False, ANPR_ENV="production")
    def test_scripts_need_yes_and_local_only_commands_refuse(self):
        with self.assertRaisesMessage(CommandError, "--yes"):
            call_command("migrate", interactive=False, verbosity=0)
        with self.assertRaisesMessage(CommandError, "only runs on local"):
            call_command("seed_demo")
        with self.assertRaisesMessage(CommandError, "only works on local"):
            call_command("flush", interactive=False)
