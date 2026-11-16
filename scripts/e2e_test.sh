#!/usr/bin/env bash
# End-to-end test: the real web API, supervisor and stage bots (and, if Kafka is running,
# a Kafka-mode reader bot), driven the way the browser drives them.   Run with:  make test-e2e
#
# SAFE: uses its own temporary SQLite database, its own copy of 9 vehicle folders, its own
# settings file, its own port (8099) and its own Kafka topics (deleted afterwards). Your
# PostgreSQL database, main/data and running services are not touched. Mock OCR is forced on.
set -u
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV=${VENV:-$(cd "$ROOT/.." && pwd)/.venv}
PY=$VENV/bin/python
SB=$(mktemp -d -t anpr-e2e-XXXXXX)
WEB=""; SUP=""; KR=""; KTOPIC="anpr-e2e-$$"

cleanup() {
  for p in $KR $SUP $WEB; do kill -TERM "$p" 2>/dev/null; done
  sleep 1
  [ -n "${KAFKA_OK:-}" ] && "$PY" - "$KTOPIC" <<'KPY' 2>/dev/null
import sys; from confluent_kafka.admin import AdminClient; import os
t = sys.argv[1]; a = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")})
[f.result(10) for f in a.delete_topics([f"{t}.folders", f"{t}.failed"]).values()]   # wait: the request must really be sent
KPY
  echo "Sandbox (logs, database): $SB"
}
trap cleanup EXIT

mapfile -t F < <(ls -d "$ROOT"/data/*/ 2>/dev/null | head -9)
if [ ${#F[@]} -lt 9 ]; then echo "Need at least 9 vehicle folders in $ROOT/data to copy from."; exit 2; fi
mkdir -p "$SB/src"
for f in "${F[@]:0:6}"; do cp -a "$f" "$SB/src/"; done

"$PY" - "$ROOT" "$SB" "$KTOPIC" <<'PY'
import sys, yaml
root, sb = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(f"{root}/config/settings.yaml"))
cfg["api"].update(mock=True, mock_latency_seconds=0.05, retry_delay_seconds=0)
cfg["reader"].update(mode="scan", min_folder_age_seconds=0, storage_root=f"{sb}/src", wait_for_files_seconds=2,
                     sources=[{"name": "e2e", "path": f"{sb}/src", "accuracy": "high", "priority": 1}])
t = sys.argv[3]
cfg["kafka"]["topics"].update(folders=f"{t}.folders", failed=f"{t}.failed")
cfg["kafka"]["groups"]["reader"] = f"{t}-readers"
cfg["kafka"]["partitions"] = 2
cfg["pipeline"]["use_prefect"] = False
for name, st in cfg["stages"].items():
    st["placeholder_seconds"] = 0.01
cfg["stages"]["reader"]["scan_every_seconds"] = 2
yaml.safe_dump(cfg, open(f"{sb}/settings.yaml", "w"))
PY
export ANPR_DB_ENGINE=sqlite ANPR_DB_PATH="$SB/e2e.sqlite3" ANPR_CONFIG="$SB/settings.yaml" ANPR_READER_MODE=scan ANPR_USE_PREFECT=0

PASS=0; FAILN=0
check() { if eval "$2" >/dev/null 2>&1; then echo "  PASS  $1"; PASS=$((PASS+1)); else echo "  FAIL  $1"; FAILN=$((FAILN+1)); fi; }
q()     { (cd "$ROOT/web" && "$PY" manage.py shell -c "$1" 2>/dev/null | grep -v "imported automatically" | tail -1); }
waitq() { for _ in $(seq "${3:-30}"); do [ "$(q "$1")" = "$2" ] && return 0; sleep 1; done; return 1; }
json()  { "$PY" -c "import sys,json; d=json.load(sys.stdin); print($1)"; }
URL=http://127.0.0.1:8099/api
M="from core.models import *"

echo "== setup (sandbox: $SB)"
(cd "$ROOT/web" && "$PY" manage.py migrate -v0) && echo "  ok    sandbox database migrated"
q "from django.contrib.auth.models import User; User.objects.create_user('e2e','',password='e2e-pass-123')" >/dev/null
(cd "$ROOT/web" && exec "$PY" manage.py runserver 127.0.0.1:8099 --noreload) >"$SB/web.log" 2>&1 & WEB=$!
for _ in $(seq 60); do curl -s -o /dev/null $URL/env/ && break; sleep 0.5; done

echo "== web API: login and security"
TOKEN=$(curl -s -X POST -H 'Content-Type: application/json' -d '{"username":"e2e","password":"e2e-pass-123"}' $URL/auth/login/ | json 'd.get("token","")')
check "login returns a token" '[ -n "$TOKEN" ]'
H=(-H "Authorization: Token $TOKEN" -H 'Content-Type: application/json')
check "wrong password rejected" '[ "$(curl -s -o /dev/null -w %{http_code} -X POST -H "Content-Type: application/json" -d "{\"username\":\"e2e\",\"password\":\"no\"}" $URL/auth/login/)" -ge 400 ]'
check "API refuses requests without login" '[ "$(curl -s -o /dev/null -w %{http_code} $URL/bots/)" = 401 ]'
check "environment endpoint shows the database, never a password" 'curl -s $URL/env/ | grep -q "\"database\":\"SQLite e2e.sqlite3\""'

echo "== Bots page: create bots"
mk() { curl -s "${H[@]}" -X POST -d "$1" $URL/bots/; }
R1=$(mk '{"name":"ocr-acct","stage":"ocr","api_username":"u1","password":"secret-1","rate_limit":0}')
OID=$(echo "$R1" | json 'd["id"]')
check "create an OCR bot with its own API account" '[ -n "$OID" ] && echo "$R1" | grep -q "\"has_account\":true"'
check "API never returns the password" '! echo "$R1" | grep -q secret-'
check "password stored encrypted" '[ "$(q "$M;print(\"secret-1\" in Bot.objects.get(name=\"ocr-acct\").api_password_encrypted)")" = False ]'
check "duplicate name rejected" 'mk "{\"name\":\"ocr-acct\",\"stage\":\"trip\"}" | grep -q name'
check "username without password rejected" 'mk "{\"name\":\"ocr-half\",\"stage\":\"ocr\",\"api_username\":\"u\"}" | grep -q password'
check "new bots start switched off" '[ "$(q "$M;print(Bot.objects.filter(desired_running=True).count())")" = 0 ]'
check "overview lists the 5 stages in order" '[ "$(curl -s "${H[@]}" $URL/pipeline/ | json "\",\".join(s[\"stage\"] for s in d[\"stages\"])")" = "reader,preprocess,ocr,llm,trip" ]'

echo "== supervisor + stage bots: the whole line"
(cd "$ROOT/web" && exec "$PY" manage.py bot_supervisor) >"$SB/supervisor.log" 2>&1 & SUP=$!
sleep 4
check "nothing happens while every bot is off" '[ "$(q "$M;print(IntakeFolder.objects.count())")" = 0 ]'
scale() { curl -s "${H[@]}" -X POST -d "{\"count\":$2}" $URL/pipeline/$1/scale/; }
check "+ answers with the updated overview" '[ "$(scale preprocess 2 | json "[x[\"bots_wanted\"] for x in d[\"stages\"] if x[\"stage\"]==\"preprocess\"][0]")" = 2 ]'
curl -s "${H[@]}" -X POST $URL/bots/$OID/start/ >/dev/null
scale reader 2 >/dev/null; scale llm 1 >/dev/null; scale trip 1 >/dev/null
check "the supervisor starts all 7 bots (2 readers, 2 preprocess, OCR, LLM, trip)" 'waitq "$M;print(Bot.objects.filter(state=\"running\").count())" 7 40'
check "reader bots got shards 1/2 and 2/2" '[ "$(q "$M;print(\",\".join(sorted(Bot.objects.filter(stage=\"reader\").values_list(\"shard\",flat=True))))")" = "1/2,2/2" ]'
check "every folder goes down the whole line (done or manual)" 'waitq "$M;print(IntakeFolder.objects.exclude(stage__in=(\"done\",\"manual\")).count())" 0 90'
check "every image read by the OCR bot, with plate result and timing" '[ "$(q "$M;print(IntakeImage.objects.filter(stage=\"read\").exclude(read_by=\"ocr-acct\").count() + IntakeImage.objects.filter(stage=\"read\", duration_ms__isnull=True).count())")" = 0 ] && [ "$(q "$M;print(IntakeImage.objects.exclude(plate=\"\").exists())")" = True ]'
check "no image left in a line" '[ "$(q "$M;print(IntakeImage.objects.exclude(stage__in=(\"read\",\"skipped\")).count())")" = 0 ]'
check "no image handled twice (preprocess counts = images)" '[ "$(q "$M;from django.db.models import Sum;print(Bot.objects.filter(stage=\"preprocess\").aggregate(n=Sum(\"processed\"))[\"n\"]==IntakeImage.objects.exclude(stage=\"skipped\").count())")" = True ]'
NF=$(q "$M;print(IntakeFolder.objects.exclude(read_at=None).count())")
check "each read folder has its result on the Folders page" '[ "$NF" -ge 6 ] && [ "$(curl -s "${H[@]}" $URL/folders/ | json "d[\"count\"]")" = "$NF" ]'
check "folders were routed by OCR confidence (reasons recorded)" '[ "$(q "$M;print(IntakeFolder.objects.filter(route_reason=\"\").exclude(read_at=None).count())")" = 0 ]'
check "speeds recorded per stage" '[ "$(q "$M;print(StageMinute.objects.filter(stage=\"ocr\").exists())")" = True ]'
(cd "$ROOT/web" && "$PY" manage.py report --since all --out "$SB/report") >"$SB/report.txt" 2>&1
check "timing report shows folder and image timing" 'grep -q "^  Folder (found -> all images read)" "$SB/report.txt" && grep -qE "^  One image .* +[1-9][0-9]* +[0-9]" "$SB/report.txt"'
check "report files written (HTML + 2 CSV)" '[ -s "$SB/report.html" ] && [ -s "$SB/report_folders.csv" ] && [ -s "$SB/report_images.csv" ]'

echo "== settings, rate limit, automation"
check "stage setting saved from the API" '[ "$(curl -s "${H[@]}" -X PATCH -d "{\"batch_size\":3}" $URL/pipeline/ocr/settings/ | json "d[\"settings\"][\"batch_size\"]")" = 3 ]'
check "invalid setting rejected" '[ "$(curl -s -o /dev/null -w %{http_code} "${H[@]}" -X PATCH -d "{\"batch_size\":0}" $URL/pipeline/ocr/settings/)" = 400 ]'
curl -s "${H[@]}" -X PATCH -d '{"rate_limit":20}' $URL/bots/$OID/ >/dev/null
cp -a "${F[7]}" "$SB/src/new_arrival"
check "folder arriving later is found and processed" 'waitq "$M;print(IntakeFolder.objects.filter(name=\"new_arrival\",stage__in=(\"done\",\"manual\")).exists())" True 60'
check "rate-limit change reached the running OCR bot" '[ "$(q "$M;print(BotEvent.objects.filter(bot__name=\"ocr-acct\",message__contains=\"Rate limit is now 20\").exists())")" = True ]'
check "retry with nothing failed moves nothing" '[ "$(curl -s "${H[@]}" -X POST -d "{\"level\":\"image\"}" $URL/pipeline/retry/ | json "d[\"moved\"]")" = 0 ]'
PID=$(q "$M;print(Bot.objects.get(name=\"ocr-acct\").pid)")
kill -9 "$PID" 2>/dev/null
NEWPID=""
for _ in $(seq 30); do
  NEWPID=$(q "$M;b=Bot.objects.get(name=\"ocr-acct\");print(b.pid if b.state==\"running\" else \"\")")
  [ -n "$NEWPID" ] && [ "$NEWPID" != "$PID" ] && break; sleep 1
done
check "killed bot is restarted by the supervisor" '[ -n "$NEWPID" ] && [ "$NEWPID" != "$PID" ]'
check "restart is counted" '[ "$(q "$M;print(Bot.objects.get(name=\"ocr-acct\").restarts)")" -ge 1 ]'
REDO=$(basename "${F[0]}")
(cd "$ROOT/web" && "$PY" manage.py reset_results --folder "$REDO" --yes) >/dev/null 2>&1
check "reprocess: the folder is registered and processed again" 'waitq "$M;print(IntakeFolder.objects.filter(name=\"$REDO\",stage__in=(\"done\",\"manual\")).exists() and Folder.objects.filter(name=\"$REDO\").count()==1)" True 60'

echo "== preprocessing switched off"
check "switch preprocessing off from the API" '[ "$(curl -s "${H[@]}" -X PATCH -d "{\"enabled\":false}" $URL/pipeline/preprocess/settings/ | json "d[\"settings\"][\"enabled\"]")" = False ]'
cp -a "${F[8]}" "$SB/src/no_preprocess"
check "a new folder skips preprocessing and is still processed" 'waitq "$M;print(IntakeFolder.objects.filter(name=\"no_preprocess\",stage__in=(\"done\",\"manual\")).exists())" True 60'
check "its images never went through a preprocess bot" '[ "$(q "$M;print(IntakeImage.objects.filter(folder__name=\"no_preprocess\",cleaned_path=\"\").exclude(stage=\"skipped\").count()==IntakeImage.objects.filter(folder__name=\"no_preprocess\").exclude(stage=\"skipped\").count())")" = True ]'
curl -s "${H[@]}" -X PATCH -d '{"enabled":true}' $URL/pipeline/preprocess/settings/ >/dev/null

echo "== stop, remove, scale down"
curl -s "${H[@]}" -X POST $URL/bots/$OID/stop/ >/dev/null
check "Stop: bot state becomes stopped" 'waitq "$M;print(Bot.objects.get(name=\"ocr-acct\").state)" stopped'
gone() { for _ in $(seq 10); do ps -p "$1" >/dev/null || return 0; sleep 1; done; return 1; }
check "Stop: bot process is gone" 'gone "$NEWPID"'
curl -s "${H[@]}" -X DELETE $URL/bots/$OID/ >/dev/null
check "Remove: bot deleted, its results kept" 'waitq "$M;print(Bot.objects.filter(name=\"ocr-acct\").exists())" False && [ "$(q "$M;print(Folder.objects.count())")" -ge 6 ]'
scale reader 1 >/dev/null
check "- on readers: the remaining reader restarts as shard 1/1" 'waitq "$M;print(\",\".join(Bot.objects.filter(stage=\"reader\",state=\"running\").values_list(\"shard\",flat=True)))" "1/1" 40'
scale preprocess 0 >/dev/null
check "- to 0: the stage's bots stop and are removed" 'waitq "$M;print(Bot.objects.filter(stage=\"preprocess\").count())" 0 40'
KIDS=$(pgrep -P $SUP | tr '\n' ' ')               # only THIS sandbox supervisor's bots
kill -TERM $SUP; wait $SUP 2>/dev/null; SUP=""
check "SIGTERM to the supervisor stops its bots too" '[ -n "$KIDS" ] && ! ps -p ${KIDS// /,} >/dev/null'

echo "== Kafka mode reader (needs Kafka: make kafka-up; runs alone, after the supervisor)"
if "$PY" -c "from confluent_kafka.admin import AdminClient; import os; AdminClient({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS','localhost:9094')}).list_topics(timeout=3)" >/dev/null 2>&1; then
  KAFKA_OK=1
  KB=$(mk '{"name":"kafka-e2e","stage":"reader"}' | json 'd["id"]')
  curl -s "${H[@]}" -X POST $URL/bots/$KB/start/ >/dev/null
  (cd "$ROOT" && ANPR_READER_MODE=kafka exec "$PY" -m bots --stage reader --name kafka-e2e) >"$SB/kafka-reader.log" 2>&1 & KR=$!
  check "Kafka reader bot starts (no shard: Kafka shares the partitions)" 'waitq "$M;print(Bot.objects.filter(name=\"kafka-e2e\",state=\"running\",shard=\"kafka\").exists())" True 30'
  mkdir -p "$SB/src/kafka_in" && cp -a "${F[0]}" "$SB/src/kafka_in/kafka_car"
  "$PY" - "$KTOPIC" <<'KPY'
import json, os, sys
from confluent_kafka import Producer
t = sys.argv[1]
p = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")})
p.produce(f"{t}.folders", key=b"kafka_car", value=json.dumps({"folder_id": "kafka_car", "folder": "kafka_in/kafka_car", "accuracy": "low"}).encode())
p.produce(f"{t}.folders", key=b"kafka_car", value=json.dumps({"folder_id": "kafka_car", "folder": "kafka_in/kafka_car", "accuracy": "low"}).encode())
p.produce(f"{t}.folders", key=b"bad", value=b"not json")
p.produce(f"{t}.folders", key=b"evil", value=json.dumps({"folder_id": "x", "folder": "../../etc", "accuracy": "low"}).encode())
p.flush(10)
KPY
  check "a folder message is registered: source kafka, low accuracy, images in the first line" 'waitq "$M;print(IntakeFolder.objects.filter(name=\"kafka_car\",source=\"kafka\",accuracy=\"low\",priority=1).exists() and IntakeImage.objects.filter(folder__name=\"kafka_car\",stage=\"new\").exists())" True 40'
  check "bad messages are refused and counted (sent to the failed topic, not retried)" 'waitq "$M;print(Bot.objects.get(name=\"kafka-e2e\").failed)" 2 30'
  check "the same message delivered twice registers the folder once" '[ "$(q "$M;print(IntakeFolder.objects.filter(name=\"kafka_car\").count())")" = 1 ]'
  curl -s "${H[@]}" -X POST $URL/bots/$KB/stop/ >/dev/null
  check "Stop in the web app: the Kafka reader leaves its group and exits" 'gone "$KR" && waitq "$M;print(Bot.objects.get(name=\"kafka-e2e\").state)" stopped'
  KR=""
else
  echo "  skip  Kafka not reachable at ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094} (start it: make kafka-up)"
fi

echo
echo "RESULT: $PASS passed, $FAILN failed"
[ "$FAILN" = 0 ]
