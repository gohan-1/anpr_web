# ANPR: one place to install, start, stop and test everything.
# Run `make` (or `make help`) to see the commands. Run them from ~/Anpr/main.
#
# Two ways to run the system; every common task has a command for each:
#   development  make start / stop / bots / ...             (services on this machine, the supervisor runs the bots)
#   Docker       make docker-up / docker-down / docker-bots  (containers, one bot per container)

SHELL    := /bin/bash
ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
VENV     ?= $(abspath $(ROOT)/../.venv)
FRONTEND ?= $(abspath $(ROOT)/../Frontend)
PY       := $(VENV)/bin/python
MANAGE   := cd $(ROOT)/web && $(PY) manage.py
SERVICES := $(ROOT)/scripts/services.sh
export VENV FRONTEND

# Docker: `docker compose` from main/, the API on DOCKER_API_PORT (8000; use 8095 if `make start` runs too)
DOCKER_API_PORT ?= 8000
# PREFECT=1 adds the optional Prefect server (only useful with pipeline.use_prefect: true)
COMPOSE  := cd $(ROOT) && ANPR_API_PORT=$(DOCKER_API_PORT) docker compose $(if $(PREFECT),--profile prefect)
DMANAGE  := $(COMPOSE) exec django-api python web/manage.py
DMANAGE_IT := $(COMPOSE) exec -it django-api python web/manage.py

REPORT_ARGS = --since $(or $(SINCE),24h) $(if $(BOT),--bot $(BOT))

.DEFAULT_GOAL := help
.PHONY: help install migrate superuser start stop restart status logs bots housekeep manage shell \
        report report-files reset reprocess api-docs test test-unit test-e2e check prune-prefect \
        docker-up docker-down docker-restart docker-rebuild docker-ps docker-logs docker-scale docker-bots \
        docker-housekeep docker-migrate docker-superuser docker-manage docker-shell docker-psql \
        docker-report docker-reprocess docker-reset \
        kafka-up kafka-down kafka-check kafka-publish kafka-producer kafka-lag kafka-failed docs \
        start-% stop-% restart-% logs-%

help:  ## Show this list
	@echo "ANPR commands (run from $(ROOT)):"
	@awk 'BEGIN {FS = ":.*## "} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
	      /^[a-zA-Z0-9%_-]+:.*## / {printf "  make %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo
	@echo "Development services: prefect web supervisor producer frontend (make start-web, make logs-producer)"
	@echo "Docker services: postgres migrate django-api housekeeper producer frontend bot-reader bot-preprocess bot-ocr bot-llm bot-trip kafka kafka-ui (prefect: PREFECT=1)"

##@ Setup (development)
install:  ## Install Python packages (into ../.venv) and the web app's packages
	@test -x $(PY) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r $(ROOT)/requirements.txt
	cd $(FRONTEND) && npm install
	@test -f $(ROOT)/.env || { cp $(ROOT)/.env.example $(ROOT)/.env; echo "Created .env from .env.example: fill in the PostgreSQL password."; }

migrate:  ## Create or update the database tables
	$(MANAGE) migrate

superuser:  ## Create a login for the web app
	$(MANAGE) createsuperuser

##@ Run (development: services on this machine)
start: migrate  ## Start everything in the background (Prefect, API, supervisor + its bots, web app)
	@$(SERVICES) start
	@echo
	@echo "Open the web app:  http://127.0.0.1:5173"

stop:  ## Stop everything (bots finish their current batch first)
	@$(SERVICES) stop

restart: stop start  ## Stop, then start everything (needed after code or settings.yaml changes)

status:  ## Which services are running
	@$(SERVICES) status

bots:  ## Every bot (stage, wanted vs real state, counters) and every stage's line
	@$(MANAGE) bots

housekeep:  ## One housekeeping round now: stuck items back to their line, forgotten folders routed
	@$(MANAGE) housekeeper --once

logs:  ## Follow all service logs (Ctrl+C to leave)
	@tail -n 20 -F $(ROOT)/.run/*.log

manage:  ## Any Django command, e.g. make manage CMD="showmigrations core"
	@test -n "$(CMD)" || { echo 'Usage: make manage CMD="showmigrations core"'; exit 2; }
	$(MANAGE) $(CMD)

shell:  ## Django shell (Python with the models loaded)
	$(MANAGE) shell

start-%:  ## Start one service, e.g. make start-web
	@$(SERVICES) start $*

stop-%:  ## Stop one service, e.g. make stop-supervisor
	@$(SERVICES) stop $*

restart-%:  ## Restart one service, e.g. make restart-supervisor
	@$(SERVICES) stop $* && $(SERVICES) start $*

logs-%:  ## Follow one service's log, e.g. make logs-supervisor
	@tail -n 50 -F $(ROOT)/.run/$*.log

##@ Run (Docker: one bot = one container)
docker-up:  ## Build and start everything in containers (1 bot per stage). PREFECT=1 also starts Prefect
	@test -f $(ROOT)/envs/.env.docker || { echo "Missing envs/.env.docker: cp envs/.env.docker.example envs/.env.docker and set the password."; exit 2; }
	$(COMPOSE) up -d --build
	@echo
	@echo "Web app: http://127.0.0.1:8080   API: http://127.0.0.1:8000/api/   Swagger: http://127.0.0.1:8000/api/docs/"
	@$(if $(PREFECT),echo "Prefect: http://127.0.0.1:4200",true)
	@echo "Add bots:  make docker-scale STAGE=ocr N=3"

docker-down:  ## Stop and remove the containers, Prefect too (the database volume is kept)
	cd $(ROOT) && docker compose --profile prefect down

docker-restart:  ## Restart the containers (after editing config/settings.yaml)
	$(COMPOSE) restart

docker-rebuild:  ## Rebuild the image and recreate the containers (after changing code)
	$(COMPOSE) up -d --build --force-recreate

docker-ps:  ## Which containers run, and how many per bot stage
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Name}}\t{{.Status}}'

docker-logs:  ## Follow logs: all, or one service with SERVICE=bot-ocr
	$(COMPOSE) logs -f --tail 50 $(SERVICE)

docker-scale:  ## How many containers (bots) one stage has: make docker-scale STAGE=ocr N=3
	@bash $(ROOT)/scripts/docker_scale.sh "$(STAGE)" "$(N)"

docker-bots:  ## Every bot container (stage, state, counters) and every stage's line
	@$(DMANAGE) bots

docker-housekeep:  ## One housekeeping round now (the housekeeper container also does it every 30 s)
	@$(DMANAGE) housekeeper --once

docker-migrate:  ## Create or update the tables in the Docker database
	$(COMPOSE) run --rm migrate

docker-superuser:  ## Create a login for the web app in the Docker database
	$(DMANAGE_IT) createsuperuser

docker-manage:  ## Any Django command inside Docker, e.g. make docker-manage CMD="showmigrations core"
	@test -n "$(CMD)" || { echo 'Usage: make docker-manage CMD="showmigrations core"'; exit 2; }
	$(DMANAGE) $(CMD)

docker-shell:  ## Django shell inside Docker
	$(DMANAGE_IT) shell

docker-psql:  ## psql into the Docker database
	$(COMPOSE) exec -it postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

##@ Kafka (reader bots listen to Kafka when ANPR_READER_MODE=kafka)
kafka-up:  ## Start only Kafka + Kafka UI (for development; docker-up starts them too). UI: http://127.0.0.1:8081
	$(COMPOSE) up -d kafka kafka-ui

kafka-down:  ## Stop Kafka + Kafka UI (messages are kept in the kafkadata volume)
	$(COMPOSE) stop kafka kafka-ui

kafka-check:  ## Can this machine send to and read from Kafka? (uses a test topic, creates the real topics)
	@cd $(ROOT) && $(PY) scripts/kafka_roundtrip.py

kafka-publish:  ## Send ALL folders once (sent before or not). Default: every source; or FOLDER=x ACCURACY=low|high [LIMIT=10]
	@cd $(ROOT) && $(PY) scripts/publish_folders.py $(if $(FOLDER),"$(FOLDER)" --accuracy $(or $(ACCURACY),high)) $(if $(LIMIT),--limit $(LIMIT))

kafka-producer:  ## Start the producer (watches data/, sends NEW folders to Kafka); make start does it in Kafka mode
	@$(SERVICES) start producer

kafka-lag:  ## Per partition: which reader has it and how many messages wait (LAG)
	@$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group anpr-readers

kafka-failed:  ## Show the messages the readers refused (anpr.failed), with the reason
	@$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic anpr.failed --from-beginning --timeout-ms 5000 2>/dev/null || true

##@ Reports and starting over (development | Docker)
report:  ## Timing report on screen. Options: SINCE=24h|7d|30m|all  BOT=ocr-1
	@$(MANAGE) report $(REPORT_ARGS)

report-files:  ## Same report, also saved as HTML + 2 CSV files in main/reports/
	@$(MANAGE) report $(REPORT_ARGS) --out $(ROOT)/reports/report_$$(date +%Y-%m-%d_%H%M)

docker-report:  ## Timing report from the Docker database. Options: SINCE=  BOT=
	@$(DMANAGE) report $(REPORT_ARGS)

reprocess:  ## Process some folders again, e.g. make reprocess FOLDER="ocr_4393072 ocr_4393075" (no restart needed)
	@test -n "$(FOLDER)" || { echo 'Usage: make reprocess FOLDER="ocr_4393072 ocr_4393075"'; exit 2; }
	@$(MANAGE) reset_results $(foreach f,$(FOLDER),--folder $(f))
	@cd $(ROOT) && $(PY) scripts/publish_folders.py --forget $(FOLDER)   # Kafka mode: the producer sends them again

docker-reprocess:  ## The same in Docker: make docker-reprocess FOLDER="ocr_4393072"
	@test -n "$(FOLDER)" || { echo 'Usage: make docker-reprocess FOLDER="ocr_4393072 ocr_4393075"'; exit 2; }
	@$(DMANAGE_IT) reset_results $(foreach f,$(FOLDER),--folder $(f))
	-@$(COMPOSE) exec -T producer python scripts/publish_folders.py --forget $(FOLDER) 2>/dev/null

reset:  ## Delete ALL results + Prefect history, then restart: every folder is processed again (bots, logins kept)
	@echo "This deletes every processed result (lines, folders, images, bot logs) and the Prefect history."
	@echo "Kept: bots with their API accounts, stage settings, user accounts, and all image files."
	@read -r -p "Type 'yes' to continue: " answer && [ "$$answer" = yes ] || { echo "Cancelled, nothing changed."; exit 1; }
	@$(SERVICES) stop
	@$(MANAGE) reset_results --yes
	@cd $(ROOT) && $(PY) scripts/publish_folders.py --forget-all    # Kafka mode: the producer sends every folder again
	rm -f $(HOME)/.prefect/prefect.db $(HOME)/.prefect/prefect.db-shm $(HOME)/.prefect/prefect.db-wal
	@$(MAKE) --no-print-directory start

docker-reset:  ## Delete ALL results in the Docker database (bots, logins kept); folders are processed again
	@echo "This deletes every processed result in the DOCKER database. Kept: bots, settings, logins, image files."
	@read -r -p "Type 'yes' to continue: " answer && [ "$$answer" = yes ] || { echo "Cancelled, nothing changed."; exit 1; }
	@$(DMANAGE) reset_results --yes
	-@$(COMPOSE) exec -T producer python scripts/publish_folders.py --forget-all 2>/dev/null

##@ Test (development; your data is never touched)
test: check test-unit test-e2e  ## Everything below: checks, unit tests, end-to-end test

check:  ## Django system check + models and migrations agree
	$(MANAGE) check
	$(MANAGE) makemigrations --check --dry-run

test-unit:  ## Unit tests (Django + pytest), on throwaway SQLite databases
	cd $(ROOT)/web && ANPR_DB_ENGINE=sqlite $(PY) manage.py test core
	cd $(ROOT) && ANPR_DB_ENGINE=sqlite $(PY) -m pytest -q tests/

test-e2e:  ## Real API + supervisor + bots + reader in a sandbox (~2 min; real data untouched)
	@bash $(ROOT)/scripts/e2e_test.sh

##@ Maintenance
prune-prefect:  ## Delete Prefect run history older than DAYS (default 30), e.g. make prune-prefect DAYS=7
	cd $(ROOT) && $(PY) scripts/prune_prefect_runs.py --days $(or $(DAYS),30)

docs:  ## Rebuild docs/ANPR-Guide.pdf from docs/GUIDE.md and docs/figures/ (needs Google Chrome)
	cd $(ROOT) && $(PY) scripts/build_docs.py

api-docs:  ## Regenerate docs/api/: openapi.yaml + Postman collection (tested against a sandbox)
	cd $(ROOT)/web && ANPR_DB_ENGINE=sqlite $(PY) manage.py spectacular --validate --fail-on-warn --file $(ROOT)/docs/api/openapi.yaml
	cd $(ROOT) && $(PY) scripts/build_postman.py
