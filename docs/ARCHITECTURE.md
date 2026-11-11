# ANPR: how the system works

This document explains the whole process: how vehicle folders are found, processed by bots through Prefect and the OCR API, and stored. It also covers how the databases grow and how to keep them small.

![How a vehicle folder gets processed](diagrams/process.png)

---

## 1. What the system does

Cameras write one **folder per vehicle**, each containing a few images (`entry_image.jpg`, `exit_image.jpg`, `license_plate_crop.jpg`, …). The system:

1. finds new vehicle folders,
2. sends every image in a folder to a private **OCR API**, which returns the plate text and a confidence,
3. stores the results so people can browse them in the **web app**.

Several **bots** do the work in parallel. Each bot has its own API account, speed limit and, optionally, its own input folder. Every folder a bot processes is recorded as a **Prefect flow run**, so it can be watched in the Prefect dashboard.

No decision rules are applied: the system saves what the OCR API returned. The highest-confidence plate is shown as the folder's plate, for display only.

---

## 2. The parts

| Part | Started with | Code | Job |
|---|---|---|---|
| **Input folders** | (disk) | `main/data/` | One sub-folder per vehicle. Read only. |
| **Bot supervisor** | `python3 manage.py bot_supervisor` | `web/core/management/commands/bot_supervisor.py` | Every 30 s, finds new folders and adds them to the queue. Starts, stops and restarts bots to match the Bots page. Returns folders of crashed bots to the queue. |
| **Bot** (one process per bot) | started by the supervisor (`run_bot <name>`) | `web/core/management/commands/run_bot.py` | Claims folders from the queue, processes them, saves results. Sends a heartbeat every 3 s. |
| **Prefect flow** | runs inside each bot | `pipeline/prefect_bot.py` | One flow run per folder, one task per image. Calls the OCR API. |
| **OCR API client** | — | `pipeline/api_client.py`, `pipeline/processor.py` | Sends one image, retries 3×, respects the bot's rate limit. |
| **Queue** | — | `web/core/queue.py` | Add, claim, complete and fail folders. Stored in the `WorkItem` table. |
| **App database** | — | `web/db.sqlite3` | Queue, bots, results, users. |
| **Prefect server** | `prefect server start` | `~/.prefect/prefect.db` | Records flow and task runs for the Prefect dashboard (`http://127.0.0.1:4200`). |
| **Django API** | `python3 manage.py runserver` | `web/core/api.py`, `bot_api.py` | REST API at `:8000/api/…` for the web app. |
| **Web app** | `npm run dev` in `Frontend/` | `Frontend/src/` | Pages: Overview, Folders, Bots. |
| **Settings** | — | `config/settings.yaml` | All tunable values: paths, API, bots, Prefect on or off. |

---

## 3. The life of one folder

Follow `data/ocr_4393075/` from disk to screen. The numbers match the diagram.

1. **Scan.** The supervisor looks at each bot's input folder and the default `data/` folder every 30 s (`bots.scan_every_seconds`). Folders changed less than 30 s ago are skipped, because the camera may still be writing them (`processing.min_folder_age_seconds`).
2. **Queue.** Each new folder becomes one `WorkItem` row with status `pending`. If it is in a bot's own input folder, the row is **assigned** to that bot; otherwise it is **shared**.
3. **Claim.** A bot asks for up to `concurrency` folders (default 4). The claim is one atomic database update, so two bots can never get the same folder. The row becomes `in_progress`.
4. **Read images.** The bot lists the image files in the folder (`.jpg .jpeg .png .bmp`). No local checks are made (no hashing, duplicate or quality checks).
5. **OCR.** Inside a Prefect flow run called `bot-1: ocr_4393075`, each image is a task (up to 8 in parallel). Each task sends the image to the OCR API with the bot's own login, waiting for the bot's rate limit and retrying up to 3 times on errors.
6. **Save.** The bot's main thread writes one `Folder` row and one `Image` row per image (plate, confidence, full API reply or error). The folder is marked `processed`, or `failed` if every image got an API error. The `WorkItem` becomes `done`.
   If the whole folder crashes (for example the folder was deleted), the `WorkItem` goes back to `pending` and is retried. After 3 attempts it becomes `failed`. Failed folders can be retried from the Bots page.
7. **Show.** The web app reads `Folder` and `Image` through the API. Images are streamed from disk when you open them; they are never copied.

---

## 4. Which bot gets which folder

Each `WorkItem` row has an `assigned_bot`:

| `assigned_bot` | Who may take it |
|---|---|
| a bot's name | only that bot |
| empty ("shared") | any bot **without** its own input folder |

How the input folder you choose on the Bots page (**Browse…**) is used:

| You choose… | What gets queued |
|---|---|
| a folder **with images directly inside** (`data/ocr_4393075`) | that one folder |
| a folder **of vehicle folders** (`data/gate1/`) | every sub-folder, now and when new ones appear |
| nothing (Clear) | the bot takes shared folders from the default `data/` |

Rules the supervisor applies on every scan:

- A folder is in the queue **only once**, so it is processed only once. Choosing a folder that is already `done` does nothing.
- A **waiting** folder you choose for a bot is moved to that bot, even if it was already queued as shared.
- When you change or clear a bot's folder, its old **waiting** folders go back to the shared pool.
- Folders `in_progress` or `done` are never moved.
- Two bots cannot watch the same folder.
- Shared folders wait forever if every bot has its own input folder. Keep at least one bot without a folder, or give a bot `data` itself.

The Browse dialog only shows folders inside `paths.browse_roots` in `settings.yaml` (default `./data`). Add camera mounts there.

---

## 5. Bots and Prefect

- The **bot** owns the work: queue, Start/Stop, input folder, saving results, counting done and failed.
- **Prefect** runs and records the OCR part. In the dashboard, flow `anpr-bot-folder` has one run per folder, named `bot-name: folder-name` and tagged `bot:<name>`. Task runs are named `ocr <image file>`.
- API passwords are never flow parameters, so they never appear in Prefect.
- The Prefect server must be running while bots run.
- Turn Prefect off with `bots.use_prefect: false`; bots then call the same code directly.
- `pipeline/flows.py` is the older, separate Prefect pipeline (`python -m pipeline.flows`). It is kept but **not used** by the bots. Do not run it on the same folders.

---

## 6. Running it

Four terminals, each with `source ~/Anpr/.venv/bin/activate`:

```bash
prefect server start                                        # 1. Prefect (port 4200)
cd ~/Anpr/main/web && python3 manage.py runserver           # 2. API (port 8000)
cd ~/Anpr/Frontend && npm run dev                           # 3. web app (port 5173)
cd ~/Anpr/main/web && python3 manage.py bot_supervisor      # 4. bots
```

Then log in to the web app, open **Bots**, add or start bots, and choose input folders.

- Pressing **Start** only records that a bot should run. **The supervisor must be running** for the bot to actually start ("Waiting to start" means it isn't).
- Run **one** supervisor only. Two supervisors start two copies of every bot.
- After changing Python code or `settings.yaml`, restart the supervisor (Ctrl+C, run again). Bots finish their current folders before stopping.

Key settings in `config/settings.yaml`:

| Setting | Meaning |
|---|---|
| `paths.input_dir` | Default folder for shared work (`./data`) |
| `paths.browse_roots` | Folders the Browse dialog may open |
| `api.mock` | `true` = fake OCR replies for testing. **Set `false` and `api.url` for real results.** |
| `api.retries`, `api.timeout_seconds` | Per-image retry and timeout |
| `bots.use_prefect` | Record each folder as a Prefect flow run |
| `bots.max_attempts` | Folder tries before `failed` (3) |
| `bots.scan_every_seconds` | How often the supervisor looks for new folders (30) |
| `processing.min_folder_age_seconds` | Skip folders still being written (30) |

---

## 7. How data is stored

![Where data is stored and how it grows](diagrams/storage.png)

### App database: `main/web/db.sqlite3`

The source of truth. SQLite, one file.

| Table | One row per | What's in it | Cleaned up? |
|---|---|---|---|
| `WorkItem` | folder found | path, status, assigned bot, claiming bot, attempts, last error | **Never.** Also what stops a folder being processed twice. |
| `Folder` | folder processed | name, path, status, best plate and confidence, which bot | **Never.** |
| `Image` | image sent | file path, plate, confidence, **full API JSON reply**, API error | **Never.** Largest table. |
| `Bot` | bot | settings, encrypted API password, state, heartbeat, counters | When you remove the bot |
| `BotEvent` | bot log line | time, level, message | Last 200 per bot are kept |
| `User`, `Token` | login | Django users, API tokens | Token deleted on logout |
| `Job` | old `flows.py` run | — | Not used by bots |

How several processes share one SQLite file safely:

- **WAL mode**: readers (the web app) don't block the writer (a bot), and the writer doesn't block readers.
- **One write at a time**: SQLite allows a single writer. Each write is short, bots write only from their main thread, and a busy database is waited on for up to 20 s instead of failing.
- **Atomic claims**: a folder is claimed with `UPDATE … WHERE status='pending'`; if another bot got it first, nothing changes and the bot skips it.
- **Images are not stored**: only their paths. Deleting or moving image files breaks the previews in the web app, but not the stored results.

### Prefect database: `~/.prefect/prefect.db`

History for the Prefect dashboard only; the app never reads it. Per folder: 1 flow run, 1 task run per image, their state changes, log lines and events. Measured at **about 30–60 KB per folder**, roughly 10× the app database. Events are deleted automatically after 7 days (`PREFECT_SERVER_EVENTS_RETENTION_PERIOD`); runs, states and logs are kept until deleted.

### Growth

Assuming 5 images per folder and ~1 KB of JSON per OCR reply:

| Folders | App database | Prefect database |
|---|---|---|
| 10,000 | ~70 MB | ~450 MB |
| 100,000 | ~700 MB | ~4.5 GB |
| 1,000,000 | ~7 GB | ~45 GB |

App database per folder ≈ 1 KB + images × (0.3 KB + API reply size). These are planning estimates: check real sizes after the first few thousand folders with `ls -lh ~/Anpr/main/web/db.sqlite3 ~/.prefect/prefect.db`.

### Memory (RAM) of the running processes

- **Supervisor**: small and constant. It reads folder names with `os.scandir` and inserts them in batches of 5,000, so even millions of folders don't load into memory at once.
- **Each bot**: works on at most `concurrency` folders (default 4) with up to 8 images per folder in flight. Each image is read from disk while it is sent. Worst case ≈ concurrency × 8 × largest image (4 × 8 × 3 MB ≈ 100 MB), usually much less. Results are held only until saved; Prefect results are not persisted (`persist_result=False`).
- **Web app lists** are paged (50 rows per page).

---

## 8. Keeping the databases small

**Prefect history** (the biggest). Delete old runs whenever you like; results in the app are not affected:

```bash
cd ~/Anpr/main
python3 scripts/prune_prefect_runs.py --days 30 --dry-run   # count
python3 scripts/prune_prefect_runs.py --days 30             # delete
```

Run it weekly (later: cron or a systemd timer). If you don't need the Prefect dashboard, `bots.use_prefect: false` stops this growth completely.

**App database.** Do **not** delete `WorkItem` rows: their folders would be processed again on the next scan. Options, from least to most effort:

1. Keep everything. Fine up to a few hundred thousand folders.
2. Clear the stored API replies of old images (keeps plate and confidence):
   ```bash
   cd ~/Anpr/main/web
   python3 manage.py shell -c "from core.models import Image; from django.utils import timezone; from datetime import timedelta; print(Image.objects.filter(processed_at__lt=timezone.now()-timedelta(days=90)).update(api_response={}))"
   ```
3. Move to **PostgreSQL** at around 1 million folders, or when several machines run bots. The code is ready for it (the claim already uses `SKIP LOCKED` on PostgreSQL). Set `ANPR_DB_ENGINE=postgres` and the `POSTGRES_*` values in `main/.env` (template: `main/.env.example`), install `psycopg[binary]`, then run `migrate`.

SQLite files don't shrink after deleting data. To give space back to the disk, stop the bots and run:
```bash
cd ~/Anpr/main/web && python3 manage.py shell -c "from django.db import connection; connection.cursor().execute('VACUUM')"
```

**Backups.** Copy `web/db.sqlite3` while bots are stopped, or use SQLite's online backup:
```bash
python3 -c "import sqlite3; s=sqlite3.connect('web/db.sqlite3'); d=sqlite3.connect('backup.sqlite3'); s.backup(d)"
```

---

## 9. Known limits

- **Mock mode is on** (`api.mock: true`): plates are fake until the real API URL is set.
- Each folder is processed **once**. Images added to a folder after it was processed are ignored. There is no "Reprocess" button yet.
- The supervisor runs in a terminal; it should become a systemd service so it starts at boot.
- One machine, one SQLite file. For several machines, switch to PostgreSQL first.
- The Overview page still has charts from the old decision rules (accepted / review / no plate).

---

## 10. File map

```
Anpr/
├── Frontend/                     React web app (Vite)
│   └── src/pages/Bots.jsx        bots, queue, input folder
│   └── src/components/FolderPicker.jsx   Browse… dialog
└── main/
    ├── config/settings.yaml      all settings
    ├── data/                     input vehicle folders
    ├── docs/                     this document + diagrams
    ├── scripts/prune_prefect_runs.py
    ├── pipeline/
    │   ├── prefect_bot.py        Prefect flow used by bots
    │   ├── processor.py          list images, call API with retries
    │   ├── api_client.py         OCR API request / reply parsing
    │   ├── ratelimit.py          calls-per-second limit
    │   └── flows.py              old standalone Prefect pipeline (unused)
    └── web/
        ├── anpr_web/settings.py  Django settings (database)
        └── core/
            ├── models.py         tables
            ├── queue.py          queue + folder scanning
            ├── ingest.py         save results
            ├── api.py, bot_api.py   REST API
            └── management/commands/  bot_supervisor, run_bot, enqueue_folders
```
