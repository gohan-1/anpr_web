# ANPR: Developer and Operator Guide

This guide explains the whole system: what it does, how the parts fit together, how to run it in each setup, how to configure and tune it, and how to change the code safely. The PDF version (`docs/ANPR-Guide.pdf`) is built from this file with `make docs`.

**Contents**

1. [What the system does](#1-what-the-system-does)
2. [Architecture](#2-architecture)
3. [The assembly line](#3-the-assembly-line)
4. [Bots](#4-bots)
5. [Finding new folders: scan mode and Kafka mode](#5-finding-new-folders-scan-mode-and-kafka-mode)
6. [The database](#6-the-database)
7. [Running it: the setups](#7-running-it-the-setups)
8. [Configuration reference](#8-configuration-reference)
9. [Operating the system](#9-operating-the-system)
10. [Scaling and tuning](#10-scaling-and-tuning)
11. [Kafka in practice](#11-kafka-in-practice)
12. [Changing the code](#12-changing-the-code)
13. [Testing](#13-testing)
14. [API documentation](#14-api-documentation)
15. [Troubleshooting](#15-troubleshooting)
16. [Security notes](#16-security-notes)
17. [File map](#17-file-map)
18. [Glossary](#18-glossary)

---

## 1. What the system does

Cameras save one **vehicle folder** per vehicle, each holding a few images (entry, exit, different angles). The system:

1. **finds** each new folder and registers its images (fingerprinting duplicates);
2. **cleans** each image for OCR (preprocessing; placeholder for now);
3. **reads the plate** in each image with an outside OCR API (real, or a mock for testing);
4. **decides** per folder: a confident plate goes straight to trip creation, an unsure one to an LLM, no plate to a person;
5. **creates the trip** (placeholder for now) or puts the folder on the **Review** page for a person.

Every step is done by **bots**: small worker programs, each assigned to one step (stage). You make a step faster by giving it more bots. Everything is controlled and watched from a web app.

What is real today and what is a placeholder:

| Stage | Today | Later |
|---|---|---|
| reader | real: scans `data/` or listens to Kafka | low/high accuracy sources |
| preprocess | placeholder: waits `placeholder_seconds`, passes the image on | a preprocessing service |
| ocr | **real**: calls the OCR API with the bot's own account and rate limit (`api.mock: true` gives fake answers) | 4 OCR engines voting |
| llm | placeholder: 4 of 5 folders become "decided" | an LLM service |
| trip | placeholder: marks the folder done | the trip service |

---

## 2. Architecture

![Figure 1](figures/architecture.svg)

| Part | Code | What it does |
|---|---|---|
| Web app | `~/Anpr/Frontend` (React 19, Vite, TanStack Query) | Pages: Overview, Pipeline, Folders (+ Review), Bots. Talks only to the API under `/api/` |
| API | `main/web` (Django 5+/6, Django REST Framework) | Token login, results, review, the Pipeline and Bots controls, Swagger. Admin at `/admin/` |
| Database | PostgreSQL `anpr` | Everything: the lines (queue), results, bots, settings, users |
| Bots | `main/bots` | One process (or container) per bot. Each takes work from its stage's line |
| Supervisor | `web/core/management/commands/bot_supervisor.py` | Development only: keeps one process running per switched-on Bot row; housekeeping |
| Housekeeper | `web/core/management/commands/housekeeper.py` | Docker only: the supervisor's chores, without starting processes |
| Kafka | `docker-compose.yaml` service `kafka` | Optional input: one message per new vehicle folder |
| Prefect | optional | Records every bot batch as a flow run, for inspection (`pipeline.use_prefect`) |
| OCR API | outside | Reads plates. `api.mock: true` replaces it with fake answers |

**One database is also the queue.** A "line" is simply every row with a given value in its `stage` column. A bot takes rows from its line with `SELECT … FOR UPDATE SKIP LOCKED`, which lets any number of bots take work from the same line at once, without waiting and without ever getting the same row twice. All queue code lives in one file, `web/core/stage_queue.py`, so it can later move to Kafka or Redis by changing only that file.

**Bots never call each other.** They only move rows from one line to the next. This is what makes it an assembly line: each stage can have its own number of bots, can be stopped, and can fail without breaking the others.

---

## 3. The assembly line

![Figure 2](figures/assembly-line.svg)

### Two levels: images and folders

* **Image stages** (`preprocess`, `ocr`) work on single images (`IntakeImage`).
* **Folder stages** (`llm`, `trip`) work on whole folders (`IntakeFolder`).
* The **reader** creates the folder (`stage = reading`) and its images (`stage = new`). Duplicate images (same SHA-256 inside one folder) are marked `skipped` and never enter a line. A folder without a usable image goes straight to `manual`.

Every stage is described once in `pipeline/stages.py`:

| Stage | Takes from (its line) | While working | Puts on |
|---|---|---|---|
| preprocess | `new` | `preprocessing` | `cleaned` |
| ocr | `cleaned` | `ocr_running` | `read` |
| llm | `needs_llm` | `llm_running` | `needs_trip` or `manual` |
| trip | `needs_trip` | `trip_running` | `done` |

### Priorities

Every folder has a priority (from its source, or from the Kafka message's `accuracy`). Its images copy it. Every line is taken in order `priority, id`: lower numbers first, oldest first within the same priority. This is how "low accuracy before high accuracy" works.

### Routing a folder

When the **last** image of a folder is finished (read, skipped or failed), the OCR bot that finished it routes the folder (`web/core/ocr_results.py`, `decide()`):

| Best result among the folder's images | Next line | Reason recorded |
|---|---|---|
| a plate with confidence ≥ `stages.ocr.trip_if_confidence_at_least` (0.85) | `needs_trip` | `MH12AB1234 read with confidence 0.93 (3 image(s) agree)` |
| a plate, but less sure | `needs_llm` | `Best read … has confidence 0.71, below 0.85` |
| no plate in any image / no image read | `manual` | `No plate found in 4 image(s)` |

Routing is done **exactly once**: it is an `UPDATE … WHERE stage='reading'`, so if two OCR bots finish a folder's last two images at the same moment, only one wins. At the same time the result is written to the `Folder` / `Image` tables that the Folders page shows. If a folder's last image failed somewhere else, the housekeeping round routes it ("forgotten folders").

### Failures and retries

* A handler error puts the item **back in its line** for another try (`attempts` counts tries).
* After `max_attempts` tries it becomes `failed`, with `failed_in` = the stage and `last_error` = the message.
* **Pipeline page → a stage → Failed tab → Retry** (or `POST /api/pipeline/retry/`) puts failed items back in the line they failed in, with fresh attempts. A retried image re-opens its folder so the folder is decided again.
* An item held by a bot that went silent for `stuck_after_minutes` is returned to its line by the housekeeping round. A bot that restarts gives back its own held items at once (`reclaim`).

### Backpressure

`pause_if_next_line_over` (preprocess: 50 000) makes a stage wait while the line it feeds is longer than that, so a fast stage can't flood a slow one.

### Switching preprocessing off

Pipeline page → Preprocessing → Settings → switch **Off** (or `PATCH /api/pipeline/preprocess/settings/ {"enabled": false}`):

* images already waiting in `new` move straight to `cleaned` (the OCR line);
* new folders register their images directly as `cleaned`;
* images a preprocess bot is working on finish normally;
* the housekeeping round catches an image registered at the very moment of switching.

Only preprocessing can be switched off (`CAN_SWITCH_OFF` in `stage_queue.py`).

---

## 4. Bots

![Figure 3](figures/bot-loop.svg)

### What a bot is

A bot is one row in the `Bot` table (name, stage, wanted on/off, API account, rate limit) plus one running process `python -m bots --stage <stage> --name <name>`. Every bot of a stage runs the same loop; the stage's **handler** (`bots/handlers.py`) does the actual work on one item and returns an `Outcome` (ok + next line + fields to save, or an error). Handlers never touch the database; the loop saves the outcomes.

* **Wanted state** (`desired_running`, `delete_requested`) is set by the web app.
* **Real state** (`state`, `pid`, `last_heartbeat`, `current_task`, counters) is written by the bot itself.
* The web app shows both, so you can see "switched on, but not responding".

### Who starts bots: the two modes

`ANPR_BOTS_MANAGED_BY` decides:

| | `supervisor` (development, default) | `docker` |
|---|---|---|
| A bot is | a process started by the supervisor | a container: `bot-<stage>` service |
| Add / remove bots | Pipeline page **+ / −**, Bots page **Add bot / Remove** | `make docker-scale STAGE=ocr N=3` (the web app hides + / −) |
| Stop / Start in the web app | finishes its batch and **exits** / is started again | **Pause** (stays alive, takes no work) / **Resume** |
| Bot name | what you typed (or `<stage>-<n>` from +) | `<stage>-<container id>`, set automatically |
| API account | Bots page | Bots page, or `BOT_API_USERNAME` / `BOT_API_PASSWORD` / `BOT_RATE_LIMIT` in the container's env |
| Crash | the supervisor restarts it (and counts restarts) | Docker restarts the container (`restart: unless-stopped`) |
| Housekeeping | the supervisor, every 30 s | the `housekeeper` container, every 30 s |

### Heartbeat, stuck items and housekeeping

* Each bot writes `last_heartbeat` every `pipeline.heartbeat_seconds` (3 s) from a separate thread, even during a long batch.
* No heartbeat for `bots.stale_after_seconds` (30 s) = dead. The supervisor kills and restarts it.
* Every 30 s the housekeeping round (`web/core/housekeeping.py`):
    1. returns items held longer than `stuck_after_minutes` to their line (or fails them after `max_attempts`);
    2. moves images past a switched-off preprocessing;
    3. routes folders whose images are all finished but which nobody routed;
    4. Docker only: numbers the alive reader bots (shards 1/3, 2/3, 3/3) in scan mode;
    5. Docker only: removes the rows of containers that are gone (stopped cleanly for 60 s, or silent for `remove_dead_after_minutes`), giving back what they held.
* Run one round by hand: `make housekeep` / `make docker-housekeep`.

### OCR bots: account and rate limit

Each OCR bot calls the OCR API with **its own** account (username + password, stored encrypted with Fernet; the API never returns the password) and its own **rate limit** in calls per second (0 = no limit), shared by its parallel threads. Both are re-read before every batch, so a change on the Bots page applies without a restart. With a real API (`api.mock: false`) an OCR bot without an account waits and says so in its events.

### Prefect (optional)

With `pipeline.use_prefect: true` each batch runs as a Prefect flow run named `<bot> batch <n>` (flow `stage-batch`), with one task run per item, so the Prefect dashboard shows what each bot did. Only the bot's **name** goes to Prefect, never its API account. Prefect is a record, not a requirement: if its server can't be reached, the bot runs the batch without it and logs one warning ("Prefect server not reachable"); it switches back automatically when Prefect returns.

---

## 5. Finding new folders: scan mode and Kafka mode

`ANPR_READER_MODE` (or `reader.mode` in `settings.yaml`) chooses how reader bots find new folders. Everything after registration is identical.

### Scan mode (`scan`, the default)

Each reader bot lists the sub-folders of every `reader.sources` path every `stages.reader.scan_every_seconds` (30 s):

* only folders in **its shard**: the folder name is hashed; with 3 readers each owns about a third (`1/3`, `2/3`, `3/3`), renumbered automatically when readers are added or removed;
* skips folders already registered, and folders modified in the last `min_folder_age_seconds` (the camera may still be writing);
* reads the rest with `read_workers` threads (size, SHA-256, duplicates) and registers each folder with its images in one transaction. The unique `path` guarantees a folder is registered once even if two readers race.

### Kafka mode (`kafka`)

![Figure 4](figures/kafka.svg)

### The producer: files → Kafka

Something must turn new folders on disk into messages. Today that is **the producer**, `scripts/publish_folders.py --watch`, which plays the part of the future upstream system (cameras):

* every `stages.reader.scan_every_seconds` (30 s) it looks in every `reader.sources` path (`data/`);
* each vehicle folder that is **new** (not sent before) and **finished** (unchanged for `reader.min_folder_age_seconds`) becomes one message; the source's `accuracy` becomes the message's accuracy (priority);
* it remembers what it sent in a state file (development: `main/.run/kafka_producer_sent.txt`; Docker: the `producerstate` volume), so a restart doesn't send everything again;
* it only remembers a folder after Kafka confirmed the message (`acks=all`); if Kafka is down, the round is retried.

It runs as a service: `make start` starts it when the reader mode is `kafka` (`make start-producer` / `stop-producer` / `logs-producer` by hand), and in Docker it is the `producer` container. `make reprocess` and `make reset` tell it to forget the folders they reset, so they are sent again; `scripts/publish_folders.py --forget NAME` / `--forget-all` do the same by hand. Sending a folder twice is harmless: the reader registers a folder path only once.

When the real upstream system is connected, stop the producer (and remove the `producer` service); upstream then copies each folder and sends the same message itself.

### The message

The producer (later: the upstream system) sends **one message** per vehicle folder to the topic `anpr.folders`:

```json
{
  "folder_id":   "car_001",
  "folder":      "low_accuracy/car_001",
  "accuracy":    "low",
  "images":      ["cam_1.jpg", "cam_2.jpg"],
  "camera":      "gate-2",
  "captured_at": "2026-11-16T09:15:02Z"
}
```

| Field | Required | Meaning |
|---|---|---|
| `folder_id` | yes | the vehicle's id; also the message **key** (same key → same partition) |
| `folder` | yes | path **relative** to `reader.storage_root` (`./data`): the same message works on a laptop (`main/data/…`) and in Docker (`/app/data/…`). `..` outside the storage is refused |
| `accuracy` | yes | `low` or `high`; gives the priority (from `reader.sources`; default low = 1, high = 2) |
| `images` | no | file names only; without it, every image in the folder |
| `camera`, `captured_at` | no | informational |

All reader bots join the consumer group `anpr-readers`, and Kafka gives each a share of the topic's partitions (6 by default). For every message the bot checks it, waits up to `wait_for_files_seconds` for the files, reads and registers them, and only then **commits**. The rules:

* a message that can never work (bad JSON, missing field, unknown accuracy, files not there in time) goes to `anpr.failed` with the reason and its position, then is committed: it never blocks the partition;
* any other error (database down) stops the bot **without** committing; after the restart Kafka delivers the same message again;
* a message delivered twice is harmless: the folder's unique path registers it once.

The topics `anpr.preprocess`, `anpr.ocr`, `anpr.llm`, `anpr.trip` exist for a later step in which the lines themselves move from PostgreSQL to Kafka; nothing uses them yet.

---

## 6. The database

![Figure 6](figures/data-model.svg)

**Every table, every column, why it matters and how it grows: [DATABASE.md](DATABASE.md)** (in the PDF: Appendix A).

* **Assembly line:** `IntakeFolder` and `IntakeImage`. Their `stage` column is the line. `taken_by` / `taken_at` / `attempts` belong to the queue.
* **Results:** `Folder` and `Image`, written when a folder is routed (`ocr_results.save_result`, matched by path) and updated by the Review page. These feed the Folders, Review and Overview pages.
* **Bots:** `Bot` and `BotEvent` (each bot's log, shown on the Bots page).
* **Settings and speed:** `StageSettings` (values saved on the Pipeline page) and `StageMinute` (items done/failed per stage per minute).
* `Job` is history from the first pipeline version only.

Migrations are in `web/core/migrations/` (`0008`–`0012` built the assembly line). Always create them with `make manage CMD="makemigrations core"`, check with `make check`, and apply with `make migrate` / `make docker-migrate`. Back up before migrating a database with real data: `pg_dump -Fc anpr > backups/anpr_$(date +%Y%m%d_%H%M).dump`.

---

## 7. Running it: the setups

| Setup | Where things run | Use it for |
|---|---|---|
| **Development** | services on your machine (`make start`), bots started by the supervisor | daily development, debugging |
| **Docker** | everything in containers (`make docker-up`), one bot per container | a server, or a production-like test |
| **Kafka** | an option of either setup: reader bots listen to Kafka | when upstream sends folder messages |
| **staging / production** | either setup, with `envs/.env.staging` / `.env.production` | shared servers |

### 7.1 Prerequisites

* Python 3.12+ (development uses `~/Anpr/.venv`), Node.js 20+, `make`.
* PostgreSQL 14+ for development (Docker brings its own PostgreSQL 16).
* Docker with Compose v2 for the Docker setup and for Kafka.
* Google Chrome only to rebuild this PDF (`make docs`).

### 7.2 Development setup

```bash
cd ~/Anpr/main
make install                 # Python packages into ../.venv, web app packages into ../Frontend
```

Create the database once, as the PostgreSQL admin:

```bash
sudo -u postgres psql -c "CREATE USER anpr WITH PASSWORD 'choose-a-password' CREATEDB;"
sudo -u postgres psql -c "CREATE DATABASE anpr OWNER anpr;"
```

`make install` created `main/.env` from `.env.example`. Fill it in (it holds a password and is git-ignored):

```ini
ANPR_DB_ENGINE=postgres
POSTGRES_DB=anpr
POSTGRES_USER=anpr
POSTGRES_PASSWORD=choose-a-password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

Then:

```bash
make migrate                 # create the tables
make superuser               # your login for the web app
make start                   # Prefect :4200, API :8000, supervisor, web app :5173
```

Open **http://127.0.0.1:5173**, log in, open **Pipeline** and press **+** on each stage (or add named bots on the **Bots** page). Put vehicle folders in `main/data/`; they are found within `scan_every_seconds`.

The development services (`scripts/services.sh`; logs and pid files in `main/.run/`):

| Service | Port | What it is |
|---|---|---|
| `prefect` | 4200 | Prefect server and dashboard |
| `web` | 8000 | Django API (auto-reloads on code changes) |
| `supervisor` | – | starts/stops the bots, housekeeping |
| `producer` | – | Kafka mode only: sends each new folder in `data/` to Kafka ([section 5](#5-finding-new-folders-scan-mode-and-kafka-mode)) |
| `frontend` | 5173 | the web app (Vite, hot reload) |

After changing **bot or supervisor code** or `settings.yaml`: `make restart` (or `make restart-supervisor`). The API and the web app reload by themselves.

### 7.3 Docker setup

![Figure 5](figures/docker.svg)

```bash
cd ~/Anpr/main
cp envs/.env.docker.example envs/.env.docker   # set POSTGRES_PASSWORD, DATABASE_URL, DJANGO_SECRET_KEY
touch envs/.env.ocr                              # optional OCR account for bot-ocr (may stay empty)
make docker-up                                   # build + start; PREFECT=1 adds Prefect
make docker-superuser                            # a login in the Docker database
```

| Address | What |
|---|---|
| http://127.0.0.1:8080 | web app (nginx, proxies `/api`, `/admin`, `/static` to django-api) |
| http://127.0.0.1:8000/api/docs/ | API + Swagger |
| http://127.0.0.1:8081 | Kafka UI |
| http://127.0.0.1:4200 | Prefect (only with `PREFECT=1`) |
| 127.0.0.1:5433 | PostgreSQL (`make docker-psql`) |
| localhost:9094 | Kafka from this machine |

Docker and development use **different databases** (the Docker one lives in the `pgdata` volume). Both can't use port 8000 at the same time: stop the development API (`make stop-web`) before `make docker-up`, or stop Docker (`make docker-down`) before `make start`.

Images in `./data` on the host are visible to the bots at once (bind mount, read-only). After changing code: `make docker-rebuild`. After changing `settings.yaml`: `make docker-restart`. Scaling: [section 10](#10-scaling-and-tuning).

### 7.4 Kafka mode in each setup

**Docker:** in `envs/.env.docker` set

```ini
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ANPR_READER_MODE=kafka
```

then `make docker-restart`. Kafka starts with `make docker-up`.

**Development:** start only Kafka in Docker, and tell the local bots where it is, in `envs/.env.local`:

```bash
make kafka-up                # Kafka on localhost:9094 + Kafka UI on :8081
make kafka-check             # this machine can send and receive
```

```ini
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
ANPR_READER_MODE=kafka
```

then `make restart`: the reader bots now consume Kafka, and the **producer** starts too and sends every new folder in `data/` to Kafka (`make logs-producer`). The whole chain is: files in `data/` → producer → `anpr.folders` → reader bots → the line. More in [section 11](#11-kafka-in-practice).

### 7.5 Environments: local, staging, production

The environment is chosen by `--env NAME` on any command, else `ANPR_ENV`, else `local`. Its settings come from `envs/.env.<name>` (copy the matching `.example`).

* `local` may fall back to SQLite and a development secret key; `staging` and `production` require `DATABASE_URL` and `DJANGO_SECRET_KEY`.
* Commands that change data on staging or production ask you to type the environment name (`--yes` skips this, for scripts).
* `ANPR_READ_ONLY=true` makes the API refuse every change and stops bots and the supervisor from starting: safe for looking at production data.
* The web app's sidebar shows the environment and database it is connected to.

---

## 8. Configuration reference

![Figure 7](figures/configuration.svg)

### 8.1 Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `ANPR_ENV` | `local` | which `envs/.env.<name>` is read |
| `DATABASE_URL` | – | `postgres://user:password@host:5432/name` (required on staging/production) |
| `ANPR_DB_ENGINE` + `POSTGRES_*` | sqlite | the older `main/.env` way to choose PostgreSQL (local) |
| `DJANGO_SECRET_KEY` | dev key on local | required outside local |
| `DJANGO_DEBUG` | on for local | |
| `DJANGO_ALLOWED_HOSTS` | localhost | comma-separated |
| `ANPR_READ_ONLY` | false | refuse every change |
| `ANPR_API_DOCS` | = DEBUG | serve Swagger/ReDoc at `/api/docs/`, `/api/redoc/` |
| `ANPR_BOTS_MANAGED_BY` | `supervisor` | `docker` when containers are the bots |
| `ANPR_READER_MODE` | `reader.mode` | `scan` or `kafka` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | `kafka:9092` inside Docker |
| `PREFECT_API_URL` | – | where bots record batches (`http://prefect:4200/api` in Docker) |
| `ANPR_USE_PREFECT` | `pipeline.use_prefect` | `0`/`1`; tests set `0` |
| `ANPR_API_KEY` | `api.api_key` | the OCR API key |
| `ANPR_CONFIG` | `config/settings.yaml` | use another settings file (tests, sandboxes) |
| `BOT_API_USERNAME`, `BOT_API_PASSWORD`, `BOT_RATE_LIMIT` | – | Docker: the bot container's OCR account and rate limit (`envs/.env.ocr`) |

### 8.2 `config/settings.yaml`

Read at start (restart after editing). Values marked **(P)** can also be changed live on the Pipeline page.

| Key | Default | Meaning |
|---|---|---|
| `paths.output_dir` | `./output` | approved / corrected folders are copied here by the Review page |
| `api.url`, `api.api_key` | | the OCR API |
| `api.timeout_seconds`, `retries`, `retry_delay_seconds` | 15, 3, 5 | per call; failed calls are retried inside one try |
| `api.mock`, `mock_latency_seconds` | true, 0.3 | fake answers for testing |
| `decision.plate_regex` | `^[A-Z0-9]{4,10}$` | what a valid plate looks like (Review page) |
| `bots.stale_after_seconds` | 30 | no heartbeat this long = dead |
| `bots.stop_grace_seconds` | 60 | time to finish a batch when stopped |
| `bots.max_bots_per_stage` | 50 | upper limit of **+** |
| `bots.remove_dead_after_minutes` | 10 | Docker: silent container rows are removed after this |
| `reader.mode` | `scan` | `scan` or `kafka` |
| `reader.storage_root` | `./data` | Kafka: messages' `folder` is relative to this |
| `reader.wait_for_files_seconds` | 30 | Kafka: wait for late files |
| `reader.sources` | `./data`, high, 1 | scan: where folders arrive, with accuracy and priority |
| `reader.image_extensions` | jpg jpeg png bmp | |
| `reader.min_folder_age_seconds` | 30 | scan: skip folders still being written |
| `reader.read_workers` | 8 | scan: folders read at the same time per reader |
| `pipeline.heartbeat_seconds` | 3 | |
| `pipeline.idle_sleep_seconds` | 2 | wait when a line is empty |
| `pipeline.use_prefect` | true | record batches in Prefect |
| `stages.reader.scan_every_seconds` **(P)** | 30 | scan interval |
| `stages.<stage>.batch_size` **(P)** | 20 (llm 5) | items taken at once |
| `stages.<stage>.parallel` **(P)** | 8 (llm 3, trip 4) | items of a batch worked on at the same time |
| `stages.<stage>.max_attempts` **(P)** | 3 (llm 2) | then failed |
| `stages.<stage>.stuck_after_minutes` **(P)** | 10 (llm 15) | then returned by housekeeping |
| `stages.preprocess.pause_if_next_line_over` **(P)** | 50000 | backpressure |
| `stages.ocr.trip_if_confidence_at_least` **(P)** | 0.85 | routing threshold |
| `stages.<stage>.placeholder_seconds` | | pretend work of placeholder stages |
| `stages.preprocess.enabled` (P, switch) | true | preprocessing on/off |
| `kafka.partitions` | 6 | partitions per topic (when created) = most readers that get work |
| `kafka.groups.reader` | `anpr-readers` | the readers' consumer group |
| `kafka.topics.folders` / `failed` | `anpr.folders` / `anpr.failed` | |

### 8.3 Pipeline page and Bots page

* **Pipeline → stage → Settings:** the (P) values above, validated (e.g. batch size 1–1000). Saved in `StageSettings`; bots use them from their next batch. **reset** returns to the file's value.
* **Bots page:** per bot: stage (fixed after creation), name, API username/password, rate limit, start/stop, remove.

---

## 9. Operating the system

### 9.1 The web app

| Page | What you do there |
|---|---|
| **Overview** | the line at a glance, counts and charts of processed folders |
| **Pipeline** | per stage: waiting / working / done per minute / failed, a trend line, its bots. **+ / −** bots (development), stage settings, the preprocessing switch, failed items + retry. Warnings: "line growing", "items waiting, no bots" |
| **Folders** | every routed folder: best plate, confidence, reason; open one for each image and the OCR API's answer. **Review**: approve, correct or reject a plate |
| **Bots** | every bot: add (stage, name, account, rate limit), start/stop (pause/resume in Docker), edit, remove, events, last error |

The sidebar shows the environment and database; a yellow banner appears on a read-only connection.

### 9.2 Everyday commands

Run from `~/Anpr/main`; `make` lists them all.

| Development | Docker | What |
|---|---|---|
| `make start` / `stop` / `restart` | `make docker-up` / `docker-down` / `docker-restart` | start / stop everything |
| `make status` | `make docker-ps` | what runs |
| `make bots` | `make docker-bots` | every bot and every line, in the terminal |
| `make logs`, `make logs-supervisor` | `make docker-logs SERVICE=bot-ocr` | logs |
| `make housekeep` | `make docker-housekeep` | one housekeeping round now |
| `make report SINCE=24h BOT=ocr-1` | `make docker-report` | timing report (`report-files`: HTML + CSV in `reports/`) |
| `make reprocess FOLDER="a b"` | `make docker-reprocess FOLDER=a` | process some folders again |
| `make reset` | `make docker-reset` | delete all results, start over (bots, settings, logins kept) |
| `make manage CMD="…"`, `make shell` | `make docker-manage CMD="…"`, `docker-shell`, `docker-psql` | Django / database access |
| – | `make docker-scale STAGE=ocr N=3` | bots per stage in Docker |
| `make kafka-up` / `kafka-check` / `kafka-producer` / `kafka-publish` / `kafka-lag` / `kafka-failed` | same (producer: `make docker-logs SERVICE=producer`) | Kafka ([section 11](#11-kafka-in-practice)) |

### 9.3 Reprocessing and starting over

A folder is processed once; its `IntakeFolder` row remembers it. `make reprocess FOLDER="…"` deletes the folder's registration and results; in **scan mode** the next scan registers it again. In **Kafka mode** the command also tells the producer to forget the folder, so its next round sends it again (a folder whose message comes from a real upstream system must be sent again by that system). Folders a bot is working on right now are skipped. `make reset` deletes all results (asks for `yes`). There is no undo: `pg_dump` first if you may want the old results. Don't delete rows by hand in SQL; the two commands delete exactly the rows that belong together.

### 9.4 Timing report

`make report` prints how long a folder takes (found → all images read), how long one image takes (rate-limit wait + read + OCR call + retries), per OCR bot, and the slowest items. `make report-files` also writes an HTML page and two CSV files to `main/reports/`.

---

## 10. Scaling and tuning

**Find the bottleneck first.** On the Pipeline page the slow stage is the one whose line keeps growing ("Line growing: add bots"). Its *done/min* is lower than the stage before it.

| Symptom | Change |
|---|---|
| a line keeps growing | more bots for that stage (+, or `make docker-scale STAGE=… N=…`) |
| OCR bots idle but OCR line long | they wait for the OCR API: raise `rate_limit` per bot or add bots with **their own accounts** |
| OCR API returns "too many requests" | lower each bot's rate limit; the sum over all OCR bots is what the API sees |
| CPU busy, little done | lower `parallel`; for I/O-bound stages (OCR calls) raise it |
| a stage floods the next | lower `pause_if_next_line_over` |
| items fail on hiccups | raise `max_attempts`, or the API's `retries` |
| items held too long after a crash | lower `stuck_after_minutes` (but above your slowest batch) |
| too many folders go to the LLM | lower `trip_if_confidence_at_least` (more go straight to trip; check accuracy) |
| new folders appear late | lower `scan_every_seconds` / `min_folder_age_seconds` (scan) |

**How many bots?** Throughput of a stage ≈ bots × parallel ÷ seconds per item (for OCR also capped by bots × rate limit). Example: one OCR call takes 0.5 s, `parallel` 8 → one bot ≈ 16 images/s if the rate limit allows it.

**Scaling in Docker:** `make docker-scale STAGE=ocr N=3` keeps every other stage's count (plain `docker compose --scale` would reset them to 1). New containers register themselves within seconds; removed ones get 60 s to finish their batch and their rows disappear about a minute later. Reader bots: in scan mode the housekeeper renumbers shards; in Kafka mode Kafka rebalances partitions, and at most `kafka.partitions` readers get work.

**Database:** every bot and the API hold connections. PostgreSQL's default `max_connections` (100) is enough for a few dozen bots; raise it before running hundreds.

---

## 11. Kafka in practice

| Task | Command |
|---|---|
| start Kafka (development) | `make kafka-up` (UI: http://127.0.0.1:8081) |
| check this machine can use it | `make kafka-check` (uses the topic `anpr.selftest`, creates the real topics) |
| the producer (new folders in `data/` → Kafka, continuously) | started by `make start` in Kafka mode; `make kafka-producer`, `make logs-producer`; Docker: container `producer` |
| send **all** folders once more (sent before or not) | `make kafka-publish` (every source), or `make kafka-publish FOLDER=low_accuracy ACCURACY=low LIMIT=10` |
| who reads which partition, and how many messages wait | `make kafka-lag` (column LAG) |
| refused messages and why | `make kafka-failed`, or Kafka UI → Topics → anpr.failed |
| switch modes | `ANPR_READER_MODE=kafka` or `scan` in the env file, then restart |

**Connecting the real upstream:** it must (1) copy the folder completely into the shared storage, then (2) send the JSON message above to `anpr.folders` with key = `folder_id`, using `acks=all`. From another machine use the address advertised for the external listener (`KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yaml`, today `localhost:9094`; change it to the server's name).

**Partitions:** the number of partitions is the most reader bots that can work at the same time. Set `kafka.partitions` before the topics are created; to grow an existing topic: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic anpr.folders --partitions 12`.

**Replaying:** to read a topic again from the start, stop the readers, then `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group anpr-readers --reset-offsets --to-earliest --topic anpr.folders --execute`. Already-registered folders are skipped (unique path).

**Single broker:** the compose file runs one broker (replication 1). For production use three brokers and replication factor 3, or a managed Kafka, and change `replication_factor` in `pipeline/kafka_client.py`.

---

## 12. Changing the code

### 12.1 Where things are

| You want to change… | File |
|---|---|
| what a stage does to one item | `bots/handlers.py` |
| the bot loop (batches, saving, heartbeat, pause) | `bots/runner.py`; Kafka reader: `bots/kafka_reader.py` |
| the stages and their lines | `pipeline/stages.py` |
| taking / finishing / failing items, settings, retry | `web/core/stage_queue.py` |
| how a folder is routed after OCR, and its saved result | `web/core/ocr_results.py` |
| finding folders on disk / Kafka message handling | `services/reader/scanner.py` / `services/reader/kafka_intake.py` |
| housekeeping chores | `web/core/housekeeping.py` |
| supervisor (development) | `web/core/management/commands/bot_supervisor.py` |
| API endpoints | `web/core/api.py` (results, review), `web/core/bot_api.py` (pipeline, bots), `urls.py`, `serializers.py`, `schema.py` (docs) |
| tables | `web/core/models.py` + a migration |
| the OCR call | `pipeline/processor.py`, `pipeline/api_client.py`, `pipeline/ratelimit.py` |
| web pages | `~/Anpr/Frontend/src/pages/*.jsx`, shared parts in `src/components/` |

### 12.2 Recipe: make a placeholder stage real (e.g. preprocessing)

1. In `bots/handlers.py`, replace the body of `preprocess()`: call your service with `item["path"]`, return `Outcome(item["id"], True, "cleaned", {"cleaned_path": new_path})`, or raise / return `Outcome(..., False, error=...)` on failure. Don't touch the database there.
2. Credentials or a rate limit: use `ctx.credentials` and `ctx.limiter` like `ocr()` does, and enable the account fields for that stage in the Bots page form if needed.
3. New settings: add them under `stages.preprocess` in `settings.yaml` (they arrive in `cfg`). To make one editable on the Pipeline page, add it with its range to `EDITABLE_SETTINGS` in `stage_queue.py`, and a label in `SETTING_LABELS` in `Pipeline.jsx`.
4. Remove `placeholder_seconds` for that stage.
5. Add a test in `web/core/test_pipeline.py` (mock the service), run `make test`.

### 12.3 Recipe: change how folders are routed

Change `decide()` in `web/core/ocr_results.py`. It receives the folder's finished images and the OCR stage settings, and returns `(next line, reason, best image)`. The next line must be one of `needs_trip`, `needs_llm`, `manual`. Add tests next to the existing routing tests in `test_pipeline.py`.

### 12.4 Recipe: add a new stage

1. `pipeline/stages.py`: add a `Stage(name, level, waits_in, working, goes_to, label)` and put its name in `BOT_STAGES` in line order.
2. Add the new status values to the `stage` choices of `IntakeImage` or `IntakeFolder` and to `Bot.Stage`; create a migration.
3. Make the stage before it put items on the new line (its handler's `Outcome.to`, or its `goes_to`).
4. `bots/handlers.py`: write the handler and register it in `HANDLERS`.
5. `settings.yaml`: a `stages.<name>` block (batch_size, parallel, max_attempts, stuck_after_minutes).
6. Frontend: add it to `STAGES` in `src/components/BotParts.jsx` (the Pipeline page draws stages from the API).
7. Docker: a `bot-<name>` service in `docker-compose.yaml` and the name in `STAGES` of `scripts/docker_scale.sh`.
8. Tests, `make test`, `make api-docs`.

### 12.5 Recipe: add an API endpoint

1. Write the view in `api.py` or `bot_api.py` (DRF `@api_view` or a view class). Use `require_writable` semantics: the `ReadOnlyMiddleware` already refuses writes in read-only mode.
2. Add the URL in `web/core/urls.py`.
3. Describe it for Swagger in `web/core/schema.py` (`extend_schema` with an example) and decorate the view.
4. Test it in `web/core/tests.py` with `APIClient`.
5. `make api-docs` regenerates `docs/api/openapi.yaml` and the Postman collection (it fails if the schema has warnings).
6. Call it from the web app with `api(path, {method, body})` from `src/api.js`.

### 12.6 Recipe: move a line from PostgreSQL to Kafka (later)

All queue operations are in `web/core/stage_queue.py` (`take`, `finish`, `fail`, `release`, `reclaim`, `return_stuck`, `line_stats`). Bots only call those. A Kafka version would publish an item to the next stage's topic in `finish()` and consume from the stage's topic in `take()`, committing after the save, using `pipeline/kafka_client.py` and the reserved topics in `settings.yaml`. Keep PostgreSQL as the record of status and results so the web pages keep working.

### 12.7 Rules that keep the system correct

* A handler never writes to the database; the loop does, through `stage_queue`.
* Every move between lines is a conditional `UPDATE … WHERE stage = <expected> AND taken_by = <me>`, so a bot that lost an item (returned as stuck) can't overwrite someone else's work. Keep that pattern.
* A folder and its images are registered in one transaction; its route is decided by one conditional update.
* Never send secrets (API passwords) to Prefect or put them in logs or API answers.
* Times are stored in UTC (`TIME_ZONE = "UTC"`).

### 12.8 Frontend development

`cd ~/Anpr/Frontend && npm run dev` (or `make start`) serves on :5173 and proxies `/api` to `ANPR_API` (default `http://127.0.0.1:8000`). Data comes through TanStack Query hooks that poll every few seconds. Check with `npx eslint src` and `npx vite build`. In Docker the app is built into the nginx image (`Frontend/Dockerfile`); `make docker-rebuild` after changes.

---

## 13. Testing

| Command | What it covers | Time | Touches your data? |
|---|---|---|---|
| `make check` | Django system check; models and migrations agree | 5 s | no |
| `make test-unit` | 83 Django tests (API, bots, the assembly line with many bots, routing, settings, retry, environments, Docker mode, Kafka intake, supervisor, preprocessing switch, Prefect fallback, admin, API docs) + 32 pytest tests (scanner, shards, producer, rate limiter, decision rules) | ~20 s | no: throwaway SQLite |
| `make test-e2e` | the real API, supervisor, stage bots and (if Kafka runs) a Kafka reader, driven like the browser: 46+ checks | ~3 min | no: own SQLite, own copies of 9 folders, own port and topics |
| `make test` | all of the above | | no |

Unit tests never contact a running Prefect server (`ANPR_USE_PREFECT=0` is set for test runs). Write new tests next to similar ones: queue and bots in `web/core/test_pipeline.py`, Kafka in `web/core/test_kafka_intake.py`, API in `web/core/tests.py`, pure functions in `tests/`.

---

## 14. API documentation

* **Swagger UI:** http://127.0.0.1:8000/api/docs/ (shown when `DEBUG` or `ANPR_API_DOCS` is on). Click **Authorize**, enter `Token <your token>` (from `POST /api/auth/login/`).
* **ReDoc:** `/api/redoc/`; the raw schema: `/api/schema/`.
* **Postman:** import `docs/api/ANPR.postman_collection.json` and `ANPR-local.postman_environment.json`, fill in the username and password, run **Auth → Log in**. Every request has a real example answer.
* `make api-docs` rebuilds both after API changes (it runs every request against a sandbox).

Main endpoints: `auth/login|logout|me`, `env`, `stats`, `folders`, `folders/{id}`, `folders/{id}/review`, `review/next`, `images/{id}/file`, `pipeline`, `pipeline/{stage}/scale`, `pipeline/{stage}/settings`, `pipeline/failed`, `pipeline/retry`, `bots` (+ `start`, `stop`, `events`).

---

## 15. Troubleshooting

| Problem | Cause and fix |
|---|---|
| Pipeline shows items waiting, "no bots" | press **+** (development) or `make docker-scale` (Docker); check `make bots` |
| bots "switched on" but not running (development) | the supervisor isn't running: `make status`, `make start-supervisor`, `make logs-supervisor` |
| OCR bot "Waiting: no API account" | `api.mock` is false: set its username/password on the Bots page (Docker: `envs/.env.ocr`) |
| items stay "working" | their bot died; housekeeping returns them after `stuck_after_minutes`, a restarted bot at once. `make housekeep` |
| "Prefect server not reachable" in bot events | start Prefect (`make start-prefect`, or `make docker-up PREFECT=1`) or set `use_prefect: false`; work continues either way |
| Prefect shows nothing | `pipeline.use_prefect` is false, or bots point at another server (`PREFECT_API_URL`) |
| new folders not found (scan) | they are younger than `min_folder_age_seconds`, or outside `reader.sources`; see the reader bot's events |
| Kafka messages not processed | `make kafka-lag` (are readers in the group? LAG?), `make kafka-failed` (refused, with reason), the reader bot's events; `ANPR_READER_MODE` really `kafka`? |
| `make kafka-check` fails | Kafka not running (`make kafka-up`), or wrong `KAFKA_BOOTSTRAP_SERVERS` (laptop: `localhost:9094`, Docker: `kafka:9092`) |
| port 8000 in use | development and Docker both want it: stop one of them |
| Docker: duplicate bot rows | old containers' rows; the housekeeper removes them within a minute |
| everything refuses changes | read-only mode (`ANPR_READ_ONLY`), shown as a yellow banner |
| `admin.E108` or migration errors after a model change | `make check`; create the migration with `make manage CMD="makemigrations core"` |

---

## 16. Security notes

* Secrets live only in `main/.env` and `envs/.env.*` (git-ignored, mode 600, excluded from Docker images by `.dockerignore`). Never commit them.
* Bot API passwords are encrypted (Fernet, key derived from `DJANGO_SECRET_KEY`); the API never returns them. Changing the secret key makes stored bot passwords unreadable: set them again.
* All ports are published on 127.0.0.1 only. Put a TLS reverse proxy in front for remote access, set `DJANGO_DEBUG=false`, real `DJANGO_ALLOWED_HOSTS`, and switch off API docs (`ANPR_API_DOCS=false`) in production.
* Kafka in the compose file has no authentication: keep it on a private network.
* `sqlite_dump.json` (the old SQLite export) contains password hashes: don't share it; delete it once you no longer need it.

---

## 17. File map

```
~/Anpr/
├── Frontend/                     React web app (pages/, components/, api.js, styles.css; Dockerfile + nginx.conf)
└── main/
    ├── Makefile                  every command (make help)
    ├── docker-compose.yaml       the Docker setup        Dockerfile  .dockerignore
    ├── config/settings.yaml      tunable values
    ├── envs/                     .env.<environment>(.example), .env.docker, .env.ocr
    ├── data/                     incoming vehicle folders (scan mode / storage_root)
    ├── bots/                     __main__.py (start one bot), runner.py (the loop), handlers.py (stage work),
    │                             kafka_reader.py, prefect_batch.py
    ├── pipeline/                 stages.py, config.py, processor.py + api_client.py + ratelimit.py (OCR call),
    │                             decision.py (plate rules), kafka_client.py
    ├── services/reader/          scanner.py (scan mode), kafka_intake.py (Kafka messages)
    ├── services/common/          django_setup.py (Django outside manage.py)
    ├── web/                      Django project: anpr_web/ (settings, env, urls), core/ (models, API, queue,
    │                             housekeeping, admin, management/commands, migrations, tests)
    ├── scripts/                  services.sh, docker_scale.sh, e2e_test.sh, build_postman.py, build_docs.py,
    │                             kafka_roundtrip.py, publish_folders.py (the producer), prune_prefect_runs.py
    ├── tests/                    pytest tests
    ├── docs/                     GUIDE.md, DATABASE.md, ANPR-Guide.pdf, figures/, api/ (openapi.yaml, Postman)
    ├── backups/                  database dumps made before migrations
    └── unused/                   retired code, kept for reference only (safe to delete)
```

---

## 18. Glossary

| Term | Meaning |
|---|---|
| **vehicle folder** | one folder of images of one vehicle |
| **stage** | one step of the line: reader, preprocess, ocr, llm, trip |
| **line** | the items waiting for a stage: rows with that stage's waiting status |
| **bot** | a worker assigned to one stage; one process or one container |
| **batch** | the items a bot takes at once (`batch_size`) |
| **shard** | scan mode: a reader's share of the folders (`2/3`) |
| **partition** | Kafka: a slice of a topic; each is read by one reader of the group |
| **consumer group** | Kafka: the readers that share a topic (`anpr-readers`) |
| **commit** | Kafka: "this message is done"; done only after saving |
| **heartbeat** | a bot's regular "I'm alive" timestamp |
| **housekeeping** | the 30-second round of repair chores |
| **backpressure** | a stage waiting because the next line is full |
| **route** | where a folder goes after OCR: trip, LLM or a person |
| **mock** | fake OCR answers for testing (`api.mock: true`) |
