# ANPR

Automatic number-plate reading. Cameras drop one folder of images per vehicle; **bots** send each image to a private **OCR API**, and the plate text and confidence are stored in **PostgreSQL** and shown in a **web app**.

- **How to use it**: this file.
- **How it works inside** (queue, bots, Prefect, reader service, storage, growth): [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

![How a vehicle folder gets processed](docs/diagrams/process.png)

---

## Contents

1. [Quick start](#1-quick-start)
2. [Everyday commands](#2-everyday-commands)
3. [Using the web app](#3-using-the-web-app)
4. [Bots: create, start, stop, delete](#4-bots-create-start-stop-delete)
5. [The reader service](#5-the-reader-service)
6. [Settings](#6-settings)
7. [Timing report](#7-timing-report)
8. [Testing](#8-testing)
9. [Troubleshooting](#9-troubleshooting)
10. [Folder layout](#10-folder-layout)

---

## 1. Quick start

You need: Python 3.12+, Node.js 20+, PostgreSQL running locally, and `make`.

```bash
cd ~/Anpr/main
make install            # Python packages into ~/Anpr/.venv, web app packages into ~/Anpr/Frontend
```

**Database.** Once, as the PostgreSQL admin:

```bash
sudo -u postgres psql -c "CREATE USER anpr WITH PASSWORD 'choose-a-password' CREATEDB;"
sudo -u postgres psql -c "CREATE DATABASE anpr OWNER anpr;"
```

`make install` created `main/.env` from `.env.example`. Put the same password in it:

```ini
ANPR_DB_ENGINE=postgres
POSTGRES_DB=anpr
POSTGRES_USER=anpr
POSTGRES_PASSWORD=choose-a-password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

`.env` holds a password: it is in `.gitignore`, never commit it.

**Tables and a login:**

```bash
make migrate            # create the tables
make superuser          # your login for the web app
```

**Start everything:**

```bash
make start
```

```
  +  Prefect server + dashboard   http://127.0.0.1:4200
  +  Django API                   http://127.0.0.1:8000/api/
  +  Reader service (FastAPI)     http://127.0.0.1:8101/docs
  +  Bot supervisor (starts the bots)
  +  Scheduled reader scan (Prefect flow scan-sources)
  +  Web app                      http://127.0.0.1:5173
```

Open **http://127.0.0.1:5173**, log in, go to **Bots**, add a bot and press **Start**.

> **Mock mode is on by default** (`api.mock: true` in `config/settings.yaml`): plates are fake. See [Settings](#6-settings) to connect the real OCR API.

---

## 2. Everyday commands

Run them from `~/Anpr/main`. `make` alone lists them all.

| Command | What it does |
|---|---|
| `make start` | Start every service in the background (and apply any new migrations first) |
| `make stop` | Stop every service. Bots finish the folders they are working on first (up to 90 s) |
| `make restart` | Stop + start. **Needed after changing Python code or `settings.yaml`** |
| `make status` | Which services are running |
| `make bots` | Every bot: switched on or off, really running or not, input folder, counters; plus the queue |
| `make logs` | Follow all logs live (Ctrl+C to leave) |
| `make start-<service>` / `stop-<service>` / `restart-<service>` / `logs-<service>` | The same for one service |
| `make test` | All checks and tests (see [Testing](#7-testing)) |
| `make report` | How long a folder and one image take, per bot, slowest ones (see [Timing report](#7-timing-report)) |
| `make prune-prefect DAYS=30` | Delete Prefect run history older than 30 days |
| `make reprocess FOLDER="ocr_4393072 …"` | Process some folders again (see [Starting over](#starting-over)) |
| `make reset` | Delete **all** results and start again from scratch (see [Starting over](#starting-over)) |

The services, in start order:

| Service | Port | What it is |
|---|---|---|
| `prefect` | 4200 | Prefect server and dashboard. Records every folder a bot processes |
| `web` | 8000 | Django REST API, used by the web app |
| `reader` | 8101 | Reader service (FastAPI): registers new folders in the intake tables |
| `supervisor` | – | Finds new folders, queues them, starts / stops / restarts the bots |
| `scan` | – | Prefect flow that asks the reader service to scan every 60 s |
| `frontend` | 5173 | The web app (React, Vite) |

Logs and pid files are in `main/.run/` (e.g. `main/.run/supervisor.log`).

### Starting over

A folder is processed only once: its row in the queue (`WorkItem`) remembers it. To process folders again, their queue row and old results must be deleted, and these commands do exactly that:

| You want to… | Run | What happens |
|---|---|---|
| process **some folders** again (e.g. after fixing the OCR API) | `make reprocess FOLDER="ocr_4393072 ocr_4393075"` | Their queue rows and results are deleted; the running supervisor queues them again within 30 s. No restart. Folders a bot is working on right now are skipped |
| start **everything** from scratch | `make reset` | Asks for `yes`, stops all services, deletes all results, empties the Prefect history, starts everything; all folders are processed again |

What is deleted and what is kept:

| Deleted | Kept |
|---|---|
| queue (`WorkItem`), results (`Folder`, `Image`), bot logs (`BotEvent`), reader intake tables, old jobs, Prefect history (`make reset` only) | bots with their API logins and input folders (counters reset to 0), user accounts, **all image files** |

There is no undo. Back up first if you may want the old results: `pg_dump -h localhost -U anpr -Fc anpr > backup.dump`. Without make: `cd web && python manage.py reset_results [--folder NAME …] [--yes]`.

Don't delete rows by hand (DBeaver, SQL): deleting only `Folder` leaves the queue row, so nothing is processed again; deleting only `WorkItem` processes the folder again but keeps the old results next to the new ones.

You can still run any service by hand in a terminal instead, for example `cd web && python manage.py runserver`. `make start` notices a port that's already taken and won't start a second copy. It also refuses to start a second supervisor, because two supervisors would run every bot twice.

---

## 3. Using the web app

**http://127.0.0.1:5173**: log in with the account from `make superuser`.

| Page | What you do there |
|---|---|
| **Overview** | Counts and charts of processed folders |
| **Folders** | Every processed vehicle folder: plate, confidence, bot. Click one to see each image and what the OCR API returned for it |
| **Bots** | Add, start, stop, delete bots; choose a bot's input folder; see the queue; retry failed folders |

To add more logins: `make superuser`, or the Django admin at http://127.0.0.1:8000/admin/.

To check a username or password from the command line:

```bash
cd ~/Anpr/main/web
python manage.py shell -c "from django.contrib.auth import authenticate; print(authenticate(username='NAME', password='PASSWORD'))"
python manage.py changepassword NAME      # set a new password
```

---

## 4. Bots: create, start, stop, delete

A **bot** is one worker process with its own OCR API account. Several bots work in parallel; the database guarantees two bots never get the same folder.

**Bots are managed from the Bots page; the supervisor does the rest automatically.** You never start a bot process by hand.

| You do (Bots page) | What happens |
|---|---|
| **Add bot**: name, API username and password, concurrency, optional input folder | A row in the `Bot` table, switched **off**. The password is stored encrypted and never shown again |
| **Start** | The bot is marked "should run". Within 2 s the supervisor starts its process; the state goes *Starting* → *Running* |
| **Stop** | The bot finishes its current folders, then exits. State → *Stopped*. Its unfinished folders go back to the queue |
| **Delete** | The bot stops as above, then its row is removed. **Its results are kept** |
| **Input folder → Browse…** | The bot only processes folders inside that folder. See below |

What runs automatically, without you doing anything:

- **New folders** are found every 30 s and queued.
- **A bot that crashes** is restarted. If it crashes more than 5 times in 10 minutes it is switched off, and its last error is shown.
- **A bot that hangs** (no heartbeat for 30 s) is killed and restarted.
- **A folder whose bot died** goes back to the queue. After 3 failed attempts it is marked *failed*; use **Retry failed** on the Bots page.
- **Whenever the supervisor starts** (`make start`, `make restart`), every bot that was switched on is started again. After a reboot, run `make start`.

**How many bots are running?** Run `make bots`:

```
BOT           WANTED  STATE     ALIVE  PID        DONE  FAILED  RESTARTS  INPUT FOLDER
bot-1         on      running   yes    17118         1       0         0  /home/…/data/gate1
bot-2         on      running   yes    17117         2       0         0  (shared queue)

2 bot(s): 2 switched on, 2 actually running (heartbeat in the last 30s).
```

*WANTED* is what you chose on the Bots page; *STATE* and *ALIVE* are what is really happening. "Switched on but not running" means the supervisor isn't running: `make start-supervisor`.

### Which bot gets which folder

| Bot's input folder | It processes |
|---|---|
| empty | the **shared queue**: vehicle folders in `paths.input_dir` (default `main/data/`) |
| a folder of vehicle folders (`data/gate1/`) | every vehicle folder in it, now and when new ones arrive; **plus shared folders when it has spare room** |
| one vehicle folder (`data/gate1/ocr_4393075/`) | that folder; **plus shared folders when it has spare room** |

- **Every folder gets processed as long as at least one bot is switched on.** A bot always takes its own folders first; with spare room it helps with the shared queue (`bots.help_shared_queue: true`). Set it to `false` if a bot must only ever touch its own folder; shared folders then need a bot *without* an input folder, and `make bots` warns when there is none.
- A bot never takes **another bot's** folders.
- Two bots can't have the same input folder.
- A folder is processed **once**, however many bots run in parallel. Choosing a folder that was already processed does nothing.

---

## 5. The reader service

A separate FastAPI service that **registers** new vehicle folders: it lists each folder's images, their sizes and a fingerprint (sha256), marks duplicate images, and saves this in the intake tables `IntakeFolder` and `IntakeImage`. It does **not** call the OCR API, and the bots don't use the intake tables yet.

- It watches the folders in `reader.sources` in `config/settings.yaml`. By default these are `data/low_accuracy` (priority 1) and `data/high_accuracy` (priority 2). **Create these folders** (or change the paths), otherwise every scan reports "Source folder not found".
- `make start` runs it on port 8101, plus the `scan` flow that asks it to scan every 60 s (visible in Prefect as flow **scan-sources**).
- Try it in the browser: **http://127.0.0.1:8101/docs** → choose an endpoint → **Try it out** → **Execute**.

| Endpoint | Does |
|---|---|
| `GET /health` | Alive? Database reachable? Which source folders exist? |
| `POST /scan` | Register new folders now |
| `GET /folders?status=new` | Registered folders in pipeline order (priority, then oldest) |
| `GET /folders/{id}` | One folder with its images |
| `GET /images/{id}/file` | The image file |
| `GET /stats` | Counts by status, accuracy and source |

Run it alone: `cd ~/Anpr/main && python -m services.reader --port 8101`. Options: `--shard 2/3` to split the folders between three copies, `--env NAME`. Set `ANPR_READ_ONLY=1` to disable `/scan`.

---

## 6. Settings

Two files:

- **`main/.env`**: secrets and the database connection (see Quick start). Template: `.env.example`.
- **`main/config/settings.yaml`**: everything else. Run `make restart` after changing it.

The settings you're most likely to change:

| Setting | Default | Meaning |
|---|---|---|
| `api.mock` | `true` | `true` = fake OCR replies for testing. **Set `false` for real results** |
| `api.url` | placeholder | The OCR API address |
| `api.timeout_seconds`, `api.retries`, `api.retry_delay_seconds` | 15, 3, 5 | Per image |
| `paths.input_dir` | `./data` | Folder for the shared queue |
| `paths.browse_roots` | `input_dir` | Folders the **Browse…** dialog may open. Add camera mounts here |
| `bots.use_prefect` | `true` | Record every folder as a Prefect flow run. `false` = no Prefect history (saves disk) |
| `bots.help_shared_queue` | `true` | Bots with their own input folder also take shared folders when they have spare room |
| `bots.scan_every_seconds` | 30 | How often new folders are looked for |
| `bots.max_attempts` | 3 | Tries before a folder is marked failed |
| `bots.stale_after_seconds` | 30 | No heartbeat this long = bot considered dead |
| `processing.min_folder_age_seconds` | 30 | Skip folders changed this recently (camera still writing) |
| `reader.sources` | 2 folders | Folders the reader service watches, with accuracy and priority |

Each bot logs in to the OCR API with **its own** username and password (set on the Bots page). Relative paths in `settings.yaml` start from `main/`.

---

## 7. Timing report

How long does one folder take? One image? Which bot is slower? Which images are slowest?

```bash
make report                      # last 24 hours, all bots, on screen
make report SINCE=7d             # last 7 days   (also 30m, 6h, all)
make report BOT=bot-1            # one bot
make report-files                # same, and saved to main/reports/: .html (to share) + 2 .csv (Excel)
```

Without make: `cd web && python manage.py report --since 24h --bot bot-1 --top 10 --out /some/folder/name`.

What it contains:

| Section | Shows |
|---|---|
| **Summary** | Folders finished (done/failed), images sent, API errors, retries, busy time, **throughput** (folders per hour while at least one folder was running, so idle time doesn't count), queue right now |
| **Timing** | count, average, min, median, p95 and max for: **folder** time, **queue wait**, **one image**, and **all images of a folder added up** |
| **Per bot** | Folders, images, folder and image average and p95, failures, per bot |
| **Slowest folders / images** | The 5 slowest (`--top N` for more), with bot, OCR calls and result |

What each time means:

| Time | From → to | Stored in |
|---|---|---|
| **Folder** | a bot starts the folder → its results are saved | `WorkItem.claimed_at` → `finished_at` |
| **Queue wait** | the folder is found → a bot starts it | `WorkItem.created_at` → `claimed_at` |
| **One image** | rate-limit wait + reading the file + OCR call + retries | `Image.duration_ms` (and `Image.api_attempts`) |

How to read it:

- **p95** = 95 % of folders/images were faster than this. Use it, not the max, to judge normal speed.
- **Folder ≈ images added up**: the images were sent one after another and nothing else costs time (Prefect off).
- **Folder < images added up**: the images were sent in parallel (Prefect on).
- **Folder much bigger than both**: time is lost outside the OCR calls, e.g. Prefect bookkeeping under load. Compare `bots.use_prefect: true` and `false`.
- **Image time high, OCR calls > 1**: the API failed and was retried (each retry waits `api.retry_delay_seconds`).
- **Long queue wait**: not enough bots, or no switched-on bot for those folders.

The HTML file is a self-contained page you can e-mail or open in a browser. The CSV files have one row per folder (`_folders.csv`) and one row per image (`_images.csv`) for your own analysis. Images processed before timing was added (11 Nov 2026) have no image time; they still count in the totals.

---

## 8. Testing

```bash
make test          # all of the below
```

| Command | Tests | Time | Touches your data? |
|---|---|---|---|
| `make check` | Django configuration; models and migrations agree | 2 s | No |
| `make test-unit` | 36 Django tests (queue, claims, bot folders, saving results, browse, reset) and 31 pytest tests (reader service, rate limiter, old decision rules) | 10 s | No: throwaway SQLite databases |
| `make test-e2e` | The real API, supervisor, bot processes, timing report and reader service, driven like the browser drives them | ~2 min | No: own SQLite file, copies of 9 folders, own ports, mock OCR |

What `make test-e2e` proves (42 checks):

- **Login and security:** wrong password rejected, API closed without login, Browse refuses `/etc`, passwords never returned and stored encrypted.
- **Bots:** create a shared bot and a bot with its own folder; they start switched off; a second bot on the same folder is rejected.
- **Processing:** the supervisor queues folders; nothing is processed until **Start**; then every folder is processed exactly once; a bot's own folders only by that bot; shared folders still processed when only a bot with its own folder is running; plate and confidence saved and visible in the API.
- **Timing report:** every image has its time and OCR call count; the report shows folder and image timing; HTML and CSV files are written with one row per folder.
- **Automation:** a folder added later is processed automatically; a bot killed with `kill -9` is restarted and the restart is counted.
- **Stop and delete:** **Stop** ends the process; **Delete** removes the bot and keeps its results; stopping the supervisor stops its bots.
- **Reader service:** health, scan, no double registration, folders, image file, stats, read-only mode; and the Prefect `scan-sources` flow, when the Prefect server is running.

The unit tests use SQLite because the `anpr` PostgreSQL user may not be allowed to create the temporary test database. To run the Django tests on PostgreSQL instead, give it that right once (`ALTER USER anpr CREATEDB;`), then: `cd web && python manage.py test core`.

---

## 9. Troubleshooting

| Problem | Cause and fix |
|---|---|
| Bot says **Waiting to start** / `make bots` says "switched on but not running" | The supervisor isn't running: `make start-supervisor` |
| Folders stay **pending** | No bot is switched on, or the supervisor isn't running. `make bots` shows which queue they're in and warns about this |
| A folder I added isn't picked up | It must be untouched for 30 s first, and inside a watched folder. It is never processed twice; if it was done before, nothing happens |
| Bots process every folder twice | Two supervisors are running. `make status`; stop the one in another terminal |
| Bots don't appear in the Prefect dashboard | `bots.use_prefect: true` in `settings.yaml`, Prefect running (`make status`), then `make restart-supervisor` |
| `make start`: *port … already in use* | That service is already running in another terminal. Use it, or stop it and `make start` again |
| A service shows **failed to start** | Its log is printed; full log in `main/.run/<service>.log` |
| `CommandError: Conflicting migrations` | Two migration files with the same number. Don't copy `web/core/migrations/` between machines on its own: it must match your `models.py` and the migrations already applied. `make check` shows mismatches |
| `permission denied to create database` when testing | Use `make test-unit` (SQLite), or `ALTER USER anpr CREATEDB;` |
| Plates look random | Mock mode: set `api.mock: false` and `api.url`, then `make restart` |
| DBeaver doesn't show the `anpr` database | Connection settings → PostgreSQL tab → tick **Show all databases** |
| Reader: *Source folder not found* | Create `data/low_accuracy` and `data/high_accuracy`, or change `reader.sources` |

---

## 10. Folder layout

```
Anpr/
├── .venv/                       Python environment
├── Frontend/                    web app (React + Vite)
└── main/                        ← run make here
    ├── Makefile                 install / start / stop / test
    ├── .env                     database password (not in git)
    ├── config/settings.yaml     all other settings
    ├── data/                    vehicle folders (not in git)
    ├── docs/                    ARCHITECTURE.md + diagrams
    ├── pipeline/                OCR calls, Prefect flows
    ├── services/reader/         reader service (FastAPI)
    ├── reports/                 make report-files output (not in git)
    ├── scripts/                 services.sh, e2e_test.sh, prune_prefect_runs.py
    ├── tests/                   pytest tests
    └── web/                     Django project: API, models, supervisor, bots
```

For what each file does, see the file map in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#11-file-map).
