#!/usr/bin/env bash
# End-to-end test: the real web API, supervisor, bot processes and reader service,
# driven the way the browser drives them.   Run with:  make test-e2e
#
# SAFE: uses its own temporary SQLite database, its own copy of 9 vehicle folders and
# its own settings file, and its own ports (8099, 8198). Your PostgreSQL database,
# main/data and running services are not touched. Mock OCR is forced on.
set -u
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV=${VENV:-$(cd "$ROOT/.." && pwd)/.venv}
PY=$VENV/bin/python
SB=$(mktemp -d -t anpr-e2e-XXXXXX)
WEB=""; SUP=""; RD=""; RO=""

cleanup() {
  for p in $RO $RD $SUP $WEB; do kill -TERM "$p" 2>/dev/null; done
  sleep 1
  pkill -f "run_bot (e2e-shared|e2e-own)" 2>/dev/null
  echo "Sandbox (logs, database): $SB"
}
trap cleanup EXIT

mapfile -t F < <(ls -d "$ROOT"/data/*/ 2>/dev/null | head -9)
if [ ${#F[@]} -lt 9 ]; then echo "Need at least 9 vehicle folders in $ROOT/data to copy from."; exit 2; fi
mkdir -p "$SB/data/shared" "$SB/data/own" "$SB/reader/low" "$SB/reader/high"
for f in "${F[@]:0:4}"; do cp -a "$f" "$SB/data/shared/"; done
for f in "${F[@]:4:2}"; do cp -a "$f" "$SB/data/own/"; done
for f in "${F[@]:6:2}"; do cp -a "$f" "$SB/reader/low/"; done

"$PY" - "$ROOT" "$SB" <<'PY'
import sys, yaml
root, sb = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(f"{root}/config/settings.yaml"))
cfg["paths"].update(input_dir=f"{sb}/data/shared", browse_roots=[f"{sb}/data"])
cfg["api"].update(mock=True, mock_latency_seconds=0.05)
cfg["processing"]["min_folder_age_seconds"] = 0
cfg["bots"].update(scan_every_seconds=2, use_prefect=False)
cfg["reader"].update(min_folder_age_seconds=0, instances=["http://127.0.0.1:8198"], sources=[
    {"name": "low-accuracy", "path": f"{sb}/reader/low", "accuracy": "low", "priority": 1},
    {"name": "high-accuracy", "path": f"{sb}/reader/high", "accuracy": "high", "priority": 2}])
yaml.safe_dump(cfg, open(f"{sb}/settings.yaml", "w"))
PY
export ANPR_DB_ENGINE=sqlite ANPR_DB_PATH="$SB/e2e.sqlite3" ANPR_CONFIG="$SB/settings.yaml"

PASS=0; FAILN=0
check() { if eval "$2" >/dev/null 2>&1; then echo "  PASS  $1"; PASS=$((PASS+1)); else echo "  FAIL  $1"; FAILN=$((FAILN+1)); fi; }
q()     { (cd "$ROOT/web" && "$PY" manage.py shell -c "$1" 2>/dev/null | grep -v "imported automatically" | tail -1); }
waitq() { for _ in $(seq "${3:-30}"); do [ "$(q "$1")" = "$2" ] && return 0; sleep 1; done; return 1; }
json()  { "$PY" -c "import sys,json; d=json.load(sys.stdin); print($1)"; }
URL=http://127.0.0.1:8099/api

echo "== setup (sandbox: $SB)"
(cd "$ROOT/web" && "$PY" manage.py migrate -v0) && echo "  ok    sandbox database migrated"
q "from django.contrib.auth.models import User; User.objects.create_user('e2e','',password='e2e-pass-123')" >/dev/null
(cd "$ROOT/web" && exec "$PY" manage.py runserver 127.0.0.1:8099 --noreload) >"$SB/web.log" 2>&1 & WEB=$!
for _ in $(seq 60); do curl -s -o /dev/null $URL/auth/me/ && break; sleep 0.5; done

echo "== web API: login, browse, create bots"
TOKEN=$(curl -s -X POST -H 'Content-Type: application/json' -d '{"username":"e2e","password":"e2e-pass-123"}' $URL/auth/login/ | json 'd.get("token","")')
check "login returns a token" '[ -n "$TOKEN" ]'
H=(-H "Authorization: Token $TOKEN" -H 'Content-Type: application/json')
check "wrong password rejected" '[ "$(curl -s -o /dev/null -w %{http_code} -X POST -H "Content-Type: application/json" -d "{\"username\":\"e2e\",\"password\":\"no\"}" $URL/auth/login/)" -ge 400 ]'
check "API refuses requests without login" '[ "$(curl -s -o /dev/null -w %{http_code} $URL/bots/)" = 401 ]'
check "Browse lists sandbox folders" 'curl -s "${H[@]}" "$URL/browse/?path=$SB/data" | grep -q own'
check "Browse refuses folders outside browse_roots" '[ "$(curl -s -o /dev/null -w %{http_code} "${H[@]}" "$URL/browse/?path=/etc")" = 403 ]'

mk() { curl -s "${H[@]}" -X POST -d "$1" $URL/bots/; }
R1=$(mk '{"name":"e2e-shared","api_username":"u1","password":"secret-1","concurrency":2}')
R2=$(mk "{\"name\":\"e2e-own\",\"api_username\":\"u2\",\"password\":\"secret-2\",\"concurrency\":2,\"input_dir\":\"$SB/data/own\"}")
SID=$(echo "$R1" | json 'd["id"]'); OID=$(echo "$R2" | json 'd["id"]')
check "create bot without input folder (takes shared folders)" '[ -n "$SID" ]'
check "create bot with its own input folder" '[ -n "$OID" ]'
check "API never returns the password" '! echo "$R1$R2" | grep -q secret-'
check "password stored encrypted" '[ "$(q "from core.models import Bot;print(\"secret-1\" in Bot.objects.get(name=\"e2e-shared\").api_password_encrypted)")" = False ]'
check "second bot on the same folder rejected" 'mk "{\"name\":\"e2e-dup\",\"api_username\":\"u\",\"password\":\"p\",\"input_dir\":\"$SB/data/own\"}" | grep -qi input_dir'
check "new bots start switched off" '[ "$(q "from core.models import Bot;print(Bot.objects.filter(desired_running=True).count())")" = 0 ]'

echo "== supervisor: queue, start, process"
(cd "$ROOT/web" && exec "$PY" manage.py bot_supervisor) >"$SB/supervisor.log" 2>&1 & SUP=$!
check "supervisor queues 6 folders (4 shared + 2 own)" 'waitq "from core.models import WorkItem;print(WorkItem.objects.count())" 6'
check "nothing processed while bots are off" '[ "$(q "from core.models import WorkItem;print(WorkItem.objects.filter(status=\"done\").count())")" = 0 ]'
curl -s "${H[@]}" -X POST $URL/bots/$SID/start/ >/dev/null
curl -s "${H[@]}" -X POST $URL/bots/$OID/start/ >/dev/null
check "Start: all 6 folders processed" 'waitq "from core.models import WorkItem;print(WorkItem.objects.filter(status=\"done\").count())" 6 60'
check "a bot's own folders are processed only by that bot" '[ "$(q "from core.models import WorkItem;print(WorkItem.objects.filter(assigned_bot__name=\"e2e-own\").exclude(bot_name=\"e2e-own\").count())")" = 0 ]'
check "shared bot never took the other bot's folders" '[ "$(q "from core.models import WorkItem;print(WorkItem.objects.filter(bot_name=\"e2e-shared\", assigned_bot__isnull=False).count())")" = 0 ]'
check "no folder processed twice" '[ "$(q "from core.models import Folder;from django.db.models import Count;print(Folder.objects.values(\"path\").annotate(n=Count(\"id\")).filter(n__gt=1).count())")" = 0 ]'
check "Image rows saved with plate and confidence" '[ "$(q "from core.models import Image;print(Image.objects.exclude(plate=\"\").filter(confidence__gt=0).exists())")" = True ]'
check "both bots report state=running" '[ "$(q "from core.models import Bot;print(Bot.objects.filter(state=\"running\").count())")" = 2 ]'
check "results visible in the web API" '[ "$(curl -s "${H[@]}" $URL/folders/ | json "d[\"count\"] if isinstance(d,dict) else len(d)")" = 6 ]'

echo "== timing report"
check "every image has its time and OCR call count" '[ "$(q "from core.models import Image;print(Image.objects.filter(duration_ms__isnull=True).count() + Image.objects.filter(api_attempts__isnull=True).count())")" = 0 ]'
(cd "$ROOT/web" && "$PY" manage.py report --since all --out "$SB/report") >"$SB/report.txt" 2>&1
check "report shows folder and image timing" 'grep -q "^  Folder (bot start" "$SB/report.txt" && grep -qE "^  One image .* +[1-9][0-9]* +[0-9]" "$SB/report.txt"'
check "report files written (HTML + 2 CSV)" '[ -s "$SB/report.html" ] && [ -s "$SB/report_folders.csv" ] && [ -s "$SB/report_images.csv" ]'
check "folders CSV has one row per folder" '[ "$(($(wc -l < "$SB/report_folders.csv") - 1))" = 6 ]'

echo "== automation"
cp -a "${F[8]}" "$SB/data/shared/new_arrival"
check "folder added later is found and processed" 'waitq "from core.models import WorkItem;print(WorkItem.objects.filter(status=\"done\").count())" 7 30'
PID=$(q "from core.models import Bot;print(Bot.objects.get(name=\"e2e-shared\").pid)")
kill -9 "$PID" 2>/dev/null
NEWPID=""
for _ in $(seq 30); do
  NEWPID=$(q "from core.models import Bot;b=Bot.objects.get(name=\"e2e-shared\");print(b.pid if b.state==\"running\" else \"\")")
  [ -n "$NEWPID" ] && [ "$NEWPID" != "$PID" ] && break; sleep 1
done
check "killed bot is restarted by the supervisor" '[ -n "$NEWPID" ] && [ "$NEWPID" != "$PID" ]'
check "restart is counted" '[ "$(q "from core.models import Bot;print(Bot.objects.get(name=\"e2e-shared\").restarts)")" -ge 1 ]'

echo "== stop and delete"
curl -s "${H[@]}" -X POST $URL/bots/$OID/stop/ >/dev/null
check "Stop: bot state becomes stopped" 'waitq "from core.models import Bot;print(Bot.objects.get(name=\"e2e-own\").state)" stopped'
check "Stop: bot process is gone" '! pgrep -f "run_bot e2e-own"'
curl -s "${H[@]}" -X DELETE $URL/bots/$SID/ >/dev/null
check "Delete: bot removed once it has stopped" 'waitq "from core.models import Bot;print(Bot.objects.filter(name=\"e2e-shared\").exists())" False'
check "Delete: its results are kept" '[ "$(q "from core.models import Folder;print(Folder.objects.count())")" = 7 ]'
curl -s "${H[@]}" -X POST $URL/bots/$OID/start/ >/dev/null
check "restarted bot is running again" 'waitq "from core.models import Bot;print(Bot.objects.get(name=\"e2e-own\").state)" running'
cp -a "${F[1]}" "$SB/data/shared/no_shared_bot_left"
check "shared folder still processed when only a bot with its own folder runs" 'waitq "from core.models import WorkItem;print(WorkItem.objects.get(path__endswith=\"no_shared_bot_left\").bot_name)" e2e-own 30'
kill -TERM $SUP; wait $SUP 2>/dev/null; SUP=""
check "SIGTERM to supervisor stops its bots too" '! pgrep -f "run_bot e2e-own"'

echo "== reader service"
(cd "$ROOT" && exec "$PY" -m services.reader --port 8198) >"$SB/reader.log" 2>&1 & RD=$!
for _ in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:8198/health && break; sleep 0.5; done
check "/health ok" 'curl -s http://127.0.0.1:8198/health | grep -q "\"status\":\"ok\""'
check "/scan registers the 2 folders" 'curl -s -X POST http://127.0.0.1:8198/scan | grep -q "\"registered\":2"'
check "second /scan registers nothing (no duplicates)" 'curl -s -X POST http://127.0.0.1:8198/scan | grep -q "\"registered\":0"'
check "/folders lists them" '[ "$(curl -s http://127.0.0.1:8198/folders | json "len(d)")" = 2 ]'
FID=$(curl -s http://127.0.0.1:8198/folders | json 'd[0]["id"]')
IMG=$(curl -s http://127.0.0.1:8198/folders/$FID | json 'd["images"][0]["id"]')
check "/images/{id}/file sends the image" '[ "$(curl -s -o /dev/null -w %{http_code} http://127.0.0.1:8198/images/$IMG/file)" = 200 ]'
check "/stats counts them" 'curl -s http://127.0.0.1:8198/stats | grep -q "\"total_folders\":2"'
if curl -s -o /dev/null http://127.0.0.1:4200/api/health; then
  cp -a "${F[0]}" "$SB/reader/high/"
  check "Prefect flow scan-sources asks the reader to scan" '(cd "$ROOT" && "$PY" -m pipeline.intake_flow) 2>&1 | grep -q "Scan finished: 1 new folders"'
else
  echo "  skip  Prefect flow scan-sources (Prefect server not running; make start prefect)"
fi
kill $RD; wait $RD 2>/dev/null; RD=""
(cd "$ROOT" && ANPR_READ_ONLY=1 exec "$PY" -m services.reader --port 8198) >/dev/null 2>&1 & RO=$!
for _ in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:8198/health && break; sleep 0.5; done
check "read-only mode blocks /scan" '[ "$(curl -s -o /dev/null -w %{http_code} -X POST http://127.0.0.1:8198/scan)" = 403 ]'

echo
echo "RESULT: $PASS passed, $FAILN failed"
[ "$FAILN" = 0 ]
