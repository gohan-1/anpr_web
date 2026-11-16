# unused/

Code that is no longer part of the project, kept only so it can be looked at.
Nothing imports it, Docker never copies it (.dockerignore), and it can be deleted:
`rm -rf unused`.

| File | Why it's no longer used |
|---|---|
| `settings.py`, `asgi.py`, `wsgi.py` | an older copy of the Django project settings |
| `pipeline/flows.py` | the first pipeline (one Prefect flow did everything); replaced by the stage bots |
| `pipeline/quality.py` | image quality scoring, used only by `flows.py` |
| `pipeline/intake_flow.py` | Prefect flow `scan-sources` that asked the reader service to scan; reader bots scan themselves now |
| `services_reader/app.py`, `__main__.py` | the FastAPI reader service on port 8101; replaced by reader bots (scan or Kafka mode). Its scanning code lives on in `services/reader/scanner.py` |
| `scripts/kafka_check.py` | an early Kafka check; use `make kafka-check` (scripts/kafka_roundtrip.py) |
