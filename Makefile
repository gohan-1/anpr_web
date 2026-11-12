# ANPR: one place to install, start, stop and test everything.
# Run `make` (or `make help`) to see the commands. Run them from ~/Anpr/main.

SHELL    := /bin/bash
ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
VENV     ?= $(abspath $(ROOT)/../.venv)
FRONTEND ?= $(abspath $(ROOT)/../Frontend)
PY       := $(VENV)/bin/python
MANAGE   := cd $(ROOT)/web && $(PY) manage.py
SERVICES := $(ROOT)/scripts/services.sh
export VENV FRONTEND

.DEFAULT_GOAL := help
.PHONY: help install migrate superuser start stop restart status logs bots report report-files reset reprocess \
        test test-unit test-e2e check prune-prefect \
        start-% stop-% restart-% logs-%

help:  ## Show this list
	@echo "ANPR commands (run from $(ROOT)):"
	@echo
	@grep -hE '^[a-zA-Z%_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-16s %s\n", $$1, $$2}'
	@echo
	@echo "Services: prefect web reader supervisor scan frontend"
	@echo "  e.g. make start-web, make stop-supervisor, make logs-supervisor"

# ---------------------------------------------------------------- setup
install:  ## Install Python packages (into ../.venv) and the web app's packages
	@test -x $(PY) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r $(ROOT)/requirements.txt
	cd $(FRONTEND) && npm install
	@test -f $(ROOT)/.env || { cp $(ROOT)/.env.example $(ROOT)/.env; echo "Created .env from .env.example: fill in the PostgreSQL password."; }

migrate:  ## Create or update the database tables
	$(MANAGE) migrate

superuser:  ## Create a login for the web app
	$(MANAGE) createsuperuser

# ---------------------------------------------------------------- run
start: migrate  ## Start everything in the background (Prefect, API, reader, supervisor, scan, web app)
	@$(SERVICES) start
	@echo
	@echo "Open the web app:  http://127.0.0.1:5173"

stop:  ## Stop everything (bots finish their current folders first)
	@$(SERVICES) stop

restart: stop start  ## Stop, then start everything (needed after code or settings.yaml changes)

status:  ## Which services are running
	@$(SERVICES) status

bots:  ## List the bots: wanted state, real state, input folder, counters
	@$(MANAGE) bots

logs:  ## Follow all service logs (Ctrl+C to leave)
	@tail -n 20 -F $(ROOT)/.run/*.log

start-%:  ## Start one service, e.g. make start-web
	@$(SERVICES) start $*

stop-%:  ## Stop one service, e.g. make stop-supervisor
	@$(SERVICES) stop $*

restart-%:  ## Restart one service, e.g. make restart-supervisor
	@$(SERVICES) stop $* && $(SERVICES) start $*

logs-%:  ## Follow one service's log, e.g. make logs-supervisor
	@tail -n 50 -F $(ROOT)/.run/$*.log

# ---------------------------------------------------------------- test
test: check test-unit test-e2e  ## Everything below: checks, unit tests, end-to-end test

check:  ## Django system check + models and migrations agree
	$(MANAGE) check
	$(MANAGE) makemigrations --check --dry-run

test-unit:  ## Unit tests (Django + pytest), on throwaway SQLite databases
	cd $(ROOT)/web && ANPR_DB_ENGINE=sqlite $(PY) manage.py test core
	cd $(ROOT) && ANPR_DB_ENGINE=sqlite $(PY) -m pytest -q tests/

test-e2e:  ## Real API + supervisor + bots + reader in a sandbox (~2 min; real data untouched)
	@bash $(ROOT)/scripts/e2e_test.sh

# ---------------------------------------------------------------- reports
REPORT_ARGS = --since $(or $(SINCE),24h) $(if $(BOT),--bot $(BOT))

report:  ## Timing report on screen. Options: SINCE=24h|7d|30m|all  BOT=ocr-1
	@$(MANAGE) report $(REPORT_ARGS)

report-files:  ## Same report, also saved as HTML + 2 CSV files in main/reports/
	@$(MANAGE) report $(REPORT_ARGS) --out $(ROOT)/reports/report_$$(date +%Y-%m-%d_%H%M)

# ---------------------------------------------------------------- start over
reset:  ## Delete ALL results + Prefect history, then restart: every folder is processed again (bots, logins kept)
	@echo "This deletes every processed result (queue, folders, images, intake, bot logs) and the Prefect history."
	@echo "Kept: bots with their API logins, user accounts, and all image files."
	@read -r -p "Type 'yes' to continue: " answer && [ "$$answer" = yes ] || { echo "Cancelled, nothing changed."; exit 1; }
	@$(SERVICES) stop
	@$(MANAGE) reset_results --yes
	rm -f $(HOME)/.prefect/prefect.db $(HOME)/.prefect/prefect.db-shm $(HOME)/.prefect/prefect.db-wal
	@$(MAKE) --no-print-directory start

reprocess:  ## Process some folders again, e.g. make reprocess FOLDER="ocr_4393072 ocr_4393075" (no restart needed)
	@test -n "$(FOLDER)" || { echo 'Usage: make reprocess FOLDER="ocr_4393072 ocr_4393075"'; exit 2; }
	@$(MANAGE) reset_results $(foreach f,$(FOLDER),--folder $(f))

# ---------------------------------------------------------------- maintenance
prune-prefect:  ## Delete Prefect run history older than DAYS (default 30), e.g. make prune-prefect DAYS=7
	cd $(ROOT) && $(PY) scripts/prune_prefect_runs.py --days $(or $(DAYS),30)
