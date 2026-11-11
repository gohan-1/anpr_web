#!/usr/bin/env bash
# Starts, stops and shows the ANPR background services. The Makefile calls this;
# you normally run `make start`, `make stop`, `make status` instead.
#
#   scripts/services.sh start [service...]     (no service = all, in start order)
#   scripts/services.sh stop  [service...]     (no service = all, in reverse order)
#   scripts/services.sh status
#
# Each service runs in its own process group, so stopping it also stops its children
# (bots started by the supervisor, the Prefect server's workers, Vite's node process).
# Pid files and logs are in main/.run/.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV=${VENV:-$(cd "$ROOT/.." && pwd)/.venv}
FRONTEND=${FRONTEND:-$(cd "$ROOT/.." && pwd)/Frontend}
RUN=$ROOT/.run
PY=$VENV/bin/python
mkdir -p "$RUN"
export PYTHONUNBUFFERED=1                       # write log lines immediately, not in blocks
export PREFECT_SERVER_ANALYTICS_ENABLED=false   # local server: don't send usage telemetry

ALL=(prefect web reader supervisor scan frontend)        # start order

describe() {
  case $1 in
    prefect)    echo "Prefect server + dashboard   http://127.0.0.1:4200" ;;
    web)        echo "Django API                   http://127.0.0.1:8000/api/" ;;
    reader)     echo "Reader service (FastAPI)     http://127.0.0.1:8101/docs" ;;
    supervisor) echo "Bot supervisor (starts the bots)" ;;
    scan)       echo "Scheduled reader scan (Prefect flow scan-sources)" ;;
    frontend)   echo "Web app                      http://127.0.0.1:5173" ;;
  esac
}

command_for() {
  case $1 in
    prefect)    echo "cd '$ROOT' && exec '$VENV/bin/prefect' server start --host 127.0.0.1 --port 4200" ;;
    web)        echo "cd '$ROOT/web' && exec '$PY' manage.py runserver 127.0.0.1:8000" ;;
    reader)     echo "cd '$ROOT' && exec '$PY' -m services.reader --port 8101" ;;
    supervisor) echo "cd '$ROOT/web' && exec '$PY' manage.py bot_supervisor" ;;
    scan)       echo "cd '$ROOT' && exec '$PY' -m pipeline.intake_flow --serve" ;;
    frontend)   echo "cd '$FRONTEND' && exec npm run dev -- --host 127.0.0.1 --port 5173 --strictPort" ;;
  esac
}

port_for() {
  case $1 in prefect) echo 4200 ;; web) echo 8000 ;; reader) echo 8101 ;; frontend) echo 5173 ;; *) echo "" ;; esac
}

# How long a service gets to shut down cleanly. Bots finish their current folders first.
grace_for() {
  case $1 in supervisor) echo 90 ;; *) echo 15 ;; esac
}

pid_of()      { cat "$RUN/$1.pid" 2>/dev/null; }
is_running()  { local p; p=$(pid_of "$1"); [ -n "$p" ] && kill -0 "$p" 2>/dev/null; }
port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

start_one() {
  local name=$1 port
  port=$(port_for "$name")
  if is_running "$name"; then
    echo "  =  $name already running (pid $(pid_of "$name"))"; return 0
  fi
  # Something we didn't start is already using the port, e.g. a runserver in another terminal.
  if [ -n "$port" ] && port_in_use "$port"; then
    echo "  !  $name: port $port is already in use by another program; not started."
    echo "     Stop that program (or use it), then run: make start"
    return 1
  fi
  # Two supervisors would start two copies of every bot.
  if [ "$name" = supervisor ] && pgrep -f "manage.py bot_supervisor" >/dev/null; then
    echo "  !  supervisor: one is already running outside make (pid $(pgrep -f 'manage.py bot_supervisor' | head -1)); not started."
    echo "     Only one supervisor may run. Stop the other one (Ctrl+C in its terminal) first."
    return 1
  fi
  if [ "$name" = frontend ] && [ ! -d "$FRONTEND/node_modules" ]; then
    echo "  !  frontend: $FRONTEND/node_modules missing. Run: make install"; return 1
  fi

  echo "--- $(date '+%F %T') starting $name" >>"$RUN/$name.log"
  setsid bash -c "$(command_for "$name")" >>"$RUN/$name.log" 2>&1 </dev/null &
  echo $! >"$RUN/$name.pid"

  if [ -n "$port" ]; then                              # wait until it answers
    for _ in $(seq 120); do
      port_in_use "$port" && break
      is_running "$name" || break
      sleep 0.5
    done
  else
    sleep 2
  fi
  if is_running "$name"; then
    echo "  +  $(describe "$name")"
  else
    echo "  x  $name failed to start. Last lines of $RUN/$name.log:"
    tail -15 "$RUN/$name.log" | sed 's/^/       /'
    rm -f "$RUN/$name.pid"
    return 1
  fi
}

stop_one() {
  local name=$1 pid grace
  if ! is_running "$name"; then rm -f "$RUN/$name.pid"; return 0; fi
  pid=$(pid_of "$name"); grace=$(grace_for "$name")
  # The leader process is the supervisor itself: signal it alone and let it stop its bots.
  # For everything else, signal the whole process group.
  if [ "$name" = supervisor ]; then
    echo "  …  stopping supervisor: bots finish their current folders (up to ${grace}s)"
    kill -TERM "$pid" 2>/dev/null
  else
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
  fi
  for _ in $(seq $((grace * 2))); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "  !  $name did not stop in ${grace}s; forcing it"
  fi
  kill -KILL -- "-$pid" 2>/dev/null                    # anything left in the group
  rm -f "$RUN/$name.pid"
  echo "  -  $name stopped"
}

status() {
  local name port state
  for name in "${ALL[@]}"; do
    port=$(port_for "$name")
    if is_running "$name"; then
      state="running (pid $(pid_of "$name"))"
    elif [ -n "$port" ] && port_in_use "$port"; then
      state="port $port in use, but not started by make"
    elif [ "$name" = supervisor ] && pgrep -f "manage.py bot_supervisor" >/dev/null; then
      state="running outside make (pid $(pgrep -f 'manage.py bot_supervisor' | head -1))"
    else
      state="stopped"
    fi
    printf "  %-11s %-44s %s\n" "$name" "$state" "$(describe "$name" | grep -o 'http[^ ]*')"
  done
}

valid() { local n; for n in "${ALL[@]}"; do [ "$n" = "$1" ] && return 0; done; return 1; }

action=${1:-status}; shift || true
names=("$@")
for n in "${names[@]}"; do valid "$n" || { echo "Unknown service '$n'. Services: ${ALL[*]}"; exit 2; }; done

case $action in
  start)
    [ ${#names[@]} -eq 0 ] && names=("${ALL[@]}")
    failed=0
    for n in "${names[@]}"; do start_one "$n" || failed=1; done
    echo "Logs: $RUN/<service>.log   (make logs)"
    exit $failed ;;
  stop)
    if [ ${#names[@]} -eq 0 ]; then
      for ((i=${#ALL[@]}-1; i>=0; i--)); do stop_one "${ALL[$i]}"; done
    else
      for n in "${names[@]}"; do stop_one "$n"; done
    fi ;;
  status) status ;;
  *) echo "Usage: $0 start|stop|status [service...]"; exit 2 ;;
esac
