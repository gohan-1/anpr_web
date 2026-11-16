"""
Builds the Postman collection in docs/api/ (make postman).

    python scripts/build_postman.py

It starts a SANDBOX (temporary SQLite database, copies of 3 vehicle folders from data/, its own
port 8096, mock OCR), fills it with sample data, sends every request of the collection
in order and saves the real answers as Postman "examples". So every request in the collection
is known to work, and every example is what the API really returns.
Your PostgreSQL database, data/ and running services are not touched.

Written:
  docs/api/ANPR.postman_collection.json         import into Postman (File -> Import)
  docs/api/ANPR-local.postman_environment.json  your server address + login (fill in the password)
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parent.parent           # main/
PY = sys.executable
OUT = ROOT / "docs" / "api"
WEB = "http://127.0.0.1:8096"
USER, PASSWORD = "operator", "operator-pass-123"

# ---------------------------------------------------------------- the collection, in run order
# (folder, name, method, path, body, description, script that saves a variable, auth?)
# {{var}} in a path is filled from the variables saved so far, exactly as Postman does.
REQUESTS = [
    ("Auth", "Log in (saves the token)", "POST", "{{baseUrl}}/api/auth/login/",
     {"username": "{{username}}", "password": "{{password}}"},
     "Gives a token. The collection uses it for every other request "
     "(header `Authorization: Token {{token}}`).",
     'pm.collectionVariables.set("token", pm.response.json().token);', False),
    ("Auth", "Who am I", "GET", "{{baseUrl}}/api/auth/me/", None, "", None, True),

    ("Dashboard", "Environment (no login needed)", "GET", "{{baseUrl}}/api/env/", None,
     "Which environment and database the server uses. Never shows a password.", None, False),
    ("Dashboard", "Overview statistics", "GET", "{{baseUrl}}/api/stats/", None,
     "Everything the Overview page's charts need.", None, True),

    ("Pipeline", "Overview: every stage", "GET", "{{baseUrl}}/api/pipeline/", None,
     "Per stage: waiting, working, done per minute, failed, settings and its bots.", None, True),
    ("Pipeline", "+ / −: give OCR exactly 2 bots", "POST", "{{baseUrl}}/api/pipeline/ocr/scale/", {"count": 2},
     "Stages: reader, preprocess, ocr, llm, trip. More bots: new ones are added, switched on. "
     "Fewer: the newest are removed after their batch.", None, True),
    ("Pipeline", "Change stage settings", "PATCH", "{{baseUrl}}/api/pipeline/ocr/settings/",
     {"batch_size": 50, "parallel": 16, "trip_if_confidence_at_least": 0.9},
     "Bots use the new values from their next batch. Editable: batch_size, parallel, max_attempts, "
     "stuck_after_minutes, pause_if_next_line_over, scan_every_seconds (reader), "
     "trip_if_confidence_at_least (ocr).", None, True),
    ("Pipeline", "Switch preprocessing off", "PATCH", "{{baseUrl}}/api/pipeline/preprocess/settings/",
     {"enabled": False},
     "Only preprocessing can be switched off. Its waiting images move straight to the OCR line, and new "
     "images skip it until it is switched on again.", None, True),
    ("Pipeline", "Switch preprocessing on again", "PATCH", "{{baseUrl}}/api/pipeline/preprocess/settings/",
     {"enabled": True}, "", None, True),
    ("Pipeline", "Reset stage settings to default", "PATCH", "{{baseUrl}}/api/pipeline/ocr/settings/",
     {"batch_size": None, "parallel": None, "trip_if_confidence_at_least": None},
     "null = back to the value in config/settings.yaml.", None, True),
    ("Pipeline", "Failed images", "GET", "{{baseUrl}}/api/pipeline/failed/?level=image", None,
     "level=image or level=folder. The newest 200, with the stage they failed in and the error.", None, True),
    ("Pipeline", "Retry all failed images", "POST", "{{baseUrl}}/api/pipeline/retry/", {"level": "image"},
     'Add "ids": [1, 2] to retry only those. Images go back to the line they failed in.', None, True),

    ("Bots", "All bots", "GET", "{{baseUrl}}/api/bots/?page_size=100", None,
     "Add &stage=ocr (reader, preprocess, ocr, llm, trip) to filter.", None, True),
    ("Bots", "OCR bots only", "GET", "{{baseUrl}}/api/bots/?stage=ocr", None, "", None, True),
    ("Bots", "Create an OCR bot with its API account (saves botId)", "POST", "{{baseUrl}}/api/bots/",
     {"name": "ocr-gate1", "stage": "ocr", "api_username": "gate1-account", "password": "api-password",
      "rate_limit": 2},
     "Created switched OFF; call Start. The password is stored encrypted and never returned. "
     "Other stages need no account: {\"name\": \"trip-2\", \"stage\": \"trip\"}.",
     'pm.collectionVariables.set("botId", pm.response.json().id);', True),
    ("Bots", "One bot", "GET", "{{baseUrl}}/api/bots/{{botId}}/", None, "", None, True),
    ("Bots", "Change rate limit / API account", "PATCH", "{{baseUrl}}/api/bots/{{botId}}/",
     {"rate_limit": 5, "password": "new-api-password"},
     'Applies from the bot\'s next batch. {"api_username": ""} removes the account. The stage can\'t change.',
     None, True),
    ("Bots", "Start", "POST", "{{baseUrl}}/api/bots/{{botId}}/start/", None,
     "The supervisor starts its process within ~2 s.", None, True),
    ("Bots", "Stop", "POST", "{{baseUrl}}/api/bots/{{botId}}/stop/", None,
     "It finishes its current batch, then exits.", None, True),
    ("Bots", "Events (last 50)", "GET", "{{baseUrl}}/api/bots/{{botId}}/events/", None, "", None, True),
    ("Bots", "Remove", "DELETE", "{{baseUrl}}/api/bots/{{botId}}/", None,
     "204 = removed now. 202 = it was running: it finishes its batch, then the supervisor removes it. "
     "Its results are kept.", None, True),

    ("Folders", "Folder results (saves folderId)", "GET", "{{baseUrl}}/api/folders/?page=1", None,
     "Newest first. Filters: ?q=<name or plate>&route=processed,failed&page=2",
     'const r = pm.response.json().results; if (r.length) pm.collectionVariables.set("folderId", r[0].id);', True),
    ("Folders", "Search by plate or name", "GET", "{{baseUrl}}/api/folders/?q=MH12", None, "", None, True),
    ("Folders", "One folder with its images (saves imageId)", "GET", "{{baseUrl}}/api/folders/{{folderId}}/", None,
     "", 'const i = pm.response.json().images; if (i.length) pm.collectionVariables.set("imageId", i[0].id);', True),
    ("Folders", "Image file", "GET", "{{baseUrl}}/api/images/{{imageId}}/file/", None,
     "The image itself (binary). In Postman it shows under Body -> Preview.", None, True),
    ("Folders", "Next folder waiting for review", "GET", "{{baseUrl}}/api/review/next/", None,
     "204 No Content when nothing waits for review.", None, True),
    ("Folders", "Review: correct the plate", "POST", "{{baseUrl}}/api/folders/{{folderId}}/review/",
     {"action": "correct", "plate": "MH12AB1234"},
     'action: "approve", "correct" (needs plate) or "reject".', None, True),

    ("Auth", "Log out (the token stops working)", "POST", "{{baseUrl}}/api/auth/logout/", None,
     "Run Log in again afterwards.", None, True),
]
FOLDER_ORDER = ["Auth", "Dashboard", "Pipeline", "Bots", "Folders"]
FOLDER_INFO = {
    "Auth": "Log in first: it saves the token that every other request uses.",
    "Dashboard": "The Overview page.",
    "Pipeline": "The assembly line (Pipeline page): + / −, settings, failures.",
    "Bots": "Every bot of every stage (Bots page). Run them top to bottom: Create saves {{botId}}.",
    "Folders": "Folder results, review, image files. Run 'Folder results' first: it saves {{folderId}}.",
}


# ---------------------------------------------------------------- sandbox
def wait_for(url: str, seconds: float = 60) -> None:
    end = time.time() + seconds
    while time.time() < end:
        try:
            httpx.get(url, timeout=1)
            return
        except httpx.HTTPError:
            time.sleep(0.3)
    raise SystemExit(f"{url} did not start")


def start_sandbox(sb: Path, procs: list) -> dict:
    src = sb / "data"
    src.mkdir()
    folders = sorted(p for p in (ROOT / "data").iterdir() if p.is_dir())[:3]
    if len(folders) < 3:
        raise SystemExit("Need at least 3 vehicle folders in data/ for the sample data.")
    for f in folders:
        shutil.copytree(f, src / f.name)
    cfg = yaml.safe_load(open(ROOT / "config" / "settings.yaml"))
    cfg["api"].update(mock=True, mock_latency_seconds=0, retry_delay_seconds=0)
    cfg["reader"].update(min_folder_age_seconds=0,
                         sources=[{"name": "data", "path": str(src), "accuracy": "high", "priority": 1}])
    cfg["pipeline"]["use_prefect"] = False
    for st in cfg["stages"].values():
        st["placeholder_seconds"] = 0
    yaml.safe_dump(cfg, open(sb / "settings.yaml", "w"))
    env = {**os.environ, "ANPR_DB_ENGINE": "sqlite", "ANPR_DB_PATH": str(sb / "db.sqlite3"),
           "ANPR_CONFIG": str(sb / "settings.yaml"), "ANPR_ENV": "local", "PYTHONUNBUFFERED": "1"}
    subprocess.run([PY, "manage.py", "migrate", "-v0"], cwd=ROOT / "web", env=env, check=True)
    log = open(sb / "servers.log", "w")
    procs.append(subprocess.Popen([PY, "manage.py", "runserver", "127.0.0.1:8096", "--noreload"],
                                  cwd=ROOT / "web", env=env, stdout=log, stderr=log))   # caller stops it, even on errors
    wait_for(f"{WEB}/api/env/")
    # Sample data: the whole line once, a few bots, one failed image (shown by "Failed images").
    seed = f'''
from django.contrib.auth.models import User
User.objects.create_user("{USER}", password="{PASSWORD}")
from bots.runner import StageBot
from core.models import Bot, IntakeImage
from django.conf import settings
from services.reader.scanner import Shard, load_sources, scan_source
from pipeline.config import load_config
rcfg = load_config()["reader"]
for src in load_sources(rcfg, settings.PROJECT_ROOT):        # what a reader bot does: register the 3 folders
    scan_source(src, Shard(1, 1), "reader-1", extensions=set(rcfg["image_extensions"]), min_age_seconds=0, workers=2)
for stage in ("preprocess", "ocr", "llm", "trip"):
    b = StageBot(stage + "-1", stage)
    b.ctx.api_cfg = {{**b.ctx.api_cfg, "mock": True, "mock_latency_seconds": 0}}
    b.nap = lambda s, b=b: setattr(b, "stopping", True)
    if stage == "ocr":
        first = IntakeImage.objects.filter(stage="cleaned").order_by("id").first()
        IntakeImage.objects.filter(pk=first.pk).update(stage="failed", failed_in="ocr", attempts=3,
                                                       last_error="ConnectError: OCR API unreachable")
    b.run()
Bot.objects.create(name="reader-1", stage="reader", shard="1/1")
'''
    subprocess.run([PY, "manage.py", "shell", "-c", seed], cwd=ROOT / "web", env=env, check=True,
                   stdout=subprocess.DEVNULL)
    return env


# ---------------------------------------------------------------- run + build
def fill(text: str, values: dict) -> str:
    for k, v in values.items():
        text = text.replace("{{" + k + "}}", str(v))
    return text


def run_script(script: str | None, data, values: dict) -> None:
    """The few saving-scripts above, done in Python so the sandbox run follows the same steps."""
    if not script or data is None:
        return
    if "token" in script:
        values["token"] = data["token"]
    elif "botId" in script:
        values["botId"] = data["id"]
    elif "folderId" in script and data["results"]:
        values["folderId"] = data["results"][0]["id"]
    elif "imageId" in script and data["images"]:
        values["imageId"] = data["images"][0]["id"]


def scrub(text: str, sb: Path) -> str:
    """No machine names or temp paths in the shared collection."""
    return (text.replace(str(sb / "data"), "/srv/anpr/data").replace(str(sb), "/srv/anpr")
            .replace(socket.gethostname(), "anpr-server").replace(str(ROOT), "/srv/anpr/main"))


def url_obj(raw: str) -> dict:
    base, _, query = raw.partition("?")
    host_var, _, path = base.partition("}}")
    obj = {"raw": raw, "host": [host_var + "}}"], "path": [p for p in path.split("/") if p]}
    if base.endswith("/"):
        obj["path"].append("")                                    # keep Django's trailing slash
    if query:
        obj["query"] = [{"key": k, "value": v} for k, _, v in (q.partition("=") for q in query.split("&"))]
    return obj


def build(sb: Path) -> dict:
    values = {"baseUrl": WEB, "username": USER, "password": PASSWORD}
    folders: dict[str, list] = {name: [] for name in FOLDER_ORDER}
    client = httpx.Client(timeout=60)
    for folder, name, method, path, body, desc, script, auth in REQUESTS:
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = f"Token {values.get('token', '')}"
        body_json = json.dumps(body, indent=2) if body is not None else None
        r = client.request(method, fill(path, values), headers=headers,
                           content=fill(body_json, values) if body_json else None)
        ok = r.status_code < 400
        print(f"  {'ok  ' if ok else 'FAIL'} {r.status_code} {method:6} {fill(path, values)}")
        if not ok:
            raise SystemExit(f"Request failed: {name}: {r.text[:300]}")
        is_json = r.headers.get("content-type", "").startswith("application/json")
        data = r.json() if is_json and r.content else None
        run_script(script, data, values)

        request = {"method": method, "header": [{"key": "Content-Type", "value": "application/json"}] if body else [],
                   "url": url_obj(path), "description": desc}
        if body is not None:
            request["body"] = {"mode": "raw", "raw": body_json, "options": {"raw": {"language": "json"}}}
        if not auth:
            request["auth"] = {"type": "noauth"}
        if is_json:
            sample = scrub(json.dumps(data, indent=2), sb) if data is not None else ""
        else:
            sample = f"<{r.headers.get('content-type', 'binary')} file, {len(r.content)} bytes>" if r.content else ""
        item = {"name": name, "request": request, "response": [{
            "name": f"{r.status_code} {r.reason_phrase}", "originalRequest": request, "status": r.reason_phrase,
            "code": r.status_code, "_postman_previewlanguage": "json" if is_json else "text",
            "header": [{"key": "Content-Type", "value": r.headers.get("content-type", "")}], "body": sample}]}
        if script:
            item["event"] = [{"listen": "test", "script": {"type": "text/javascript", "exec": [
                "if (pm.response.code < 300) {", "  " + script, "}"]}}]
        folders[folder].append(item)

    return {
        "info": {"_postman_id": str(uuid.uuid5(uuid.NAMESPACE_URL, "anpr-api")), "name": "ANPR API",
                 "description": (
                     "Every endpoint of the ANPR Django API, with sample bodies and real "
                     "sample answers (open a request -> Examples).\n\n"
                     "1. Import this file and ANPR-local.postman_environment.json; pick the environment.\n"
                     "2. Fill in username and password in the environment.\n"
                     "3. Run Auth -> Log in: it saves the token for every other request.\n\n"
                     "Interactive docs: {{baseUrl}}/api/docs/ (Swagger) and {{baseUrl}}/api/redoc/.\n"
                     "Built by scripts/build_postman.py (make postman)."),
                 "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
        "auth": {"type": "apikey", "apikey": [{"key": "key", "value": "Authorization", "type": "string"},
                                              {"key": "value", "value": "Token {{token}}", "type": "string"},
                                              {"key": "in", "value": "header", "type": "string"}]},
        "item": [{"name": n, "description": FOLDER_INFO[n], "item": folders[n]} for n in FOLDER_ORDER],
        "variable": [{"key": k, "value": ""} for k in
                     ("token", "botId", "folderId", "imageId")],
    }


def environment() -> dict:
    return {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, "anpr-local-env")), "name": "ANPR local",
            "values": [{"key": "baseUrl", "value": "http://127.0.0.1:8000", "enabled": True},
                       {"key": "username", "value": "", "enabled": True},
                       {"key": "password", "value": "", "type": "secret", "enabled": True}],
            "_postman_variable_scope": "environment"}


def main() -> None:
    sb = Path(tempfile.mkdtemp(prefix="anpr-postman-"))
    procs = []
    try:
        print(f"Sandbox: {sb}")
        start_sandbox(sb, procs)
        collection = build(sb)
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=10)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "ANPR.postman_collection.json").write_text(json.dumps(collection, indent=2) + "\n")
    (OUT / "ANPR-local.postman_environment.json").write_text(json.dumps(environment(), indent=2) + "\n")
    shutil.rmtree(sb, ignore_errors=True)
    print(f"Written:\n  {OUT / 'ANPR.postman_collection.json'}\n  {OUT / 'ANPR-local.postman_environment.json'}")


if __name__ == "__main__":
    main()
