"""
Optional: record every batch in Prefect (pipeline.use_prefect: true in settings.yaml).

Each batch becomes one flow run named after its bot ("ocr-2 batch 17"), and every item
in it one task run, so the Prefect dashboard shows what each bot did, item by item.
The work itself is the same handler the plain mode uses.

If the Prefect server can't be reached (stopped, or Docker started without PREFECT=1),
the bot does NOT stop: runner.py checks prefect_reachable() before each batch and runs
the batch without Prefect instead. Prefect is a record of the work, never a requirement.
"""
import time

import httpx
from prefect import flow, task, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from .handlers import CONTEXTS, HANDLERS, Outcome

_CHECK_EVERY = 30.0                     # seconds between reachability checks
_last_check, _last_answer = 0.0, True


def prefect_reachable() -> bool:
    """True if the Prefect API answers (checked at most every 30 s). Without PREFECT_API_URL
    Prefect runs a temporary server inside the process, which is always "reachable"."""
    global _last_check, _last_answer
    if time.monotonic() - _last_check < _CHECK_EVERY:
        return _last_answer
    from prefect.settings import PREFECT_API_URL
    url = PREFECT_API_URL.value()
    try:
        _last_answer = not url or httpx.get(f"{url.rstrip('/')}/health", timeout=3).status_code == 200
    except httpx.HTTPError:
        _last_answer = False
    _last_check = time.monotonic()
    return _last_answer


@task(task_run_name="{stage} item {payload[id]}")
def handle_item(stage: str, payload: dict, scfg: dict, bot: str) -> Outcome:
    # Only the bot's NAME travels through Prefect; its API account stays in this process.
    try:
        return HANDLERS[stage](payload, scfg, CONTEXTS.get(bot))
    except Exception as exc:
        return Outcome(payload["id"], False, error=f"{type(exc).__name__}: {exc}")


@flow(name="stage-batch")
def stage_batch(stage: str, payloads: list[dict], scfg: dict, bot: str) -> list[Outcome]:
    futures = handle_item.map(stage, payloads, unmapped(scfg), unmapped(bot))
    return [f.result() for f in futures]


def run_batch(stage: str, bot: str, batch_no: int, payloads: list[dict], scfg: dict) -> list[Outcome]:
    # the stage's "parallel" setting (Pipeline page) = how many items run at the same time
    runner = ThreadPoolTaskRunner(max_workers=int(scfg.get("parallel", 4)))
    return stage_batch.with_options(flow_run_name=f"{bot} batch {batch_no}", task_runner=runner)(
        stage, payloads, scfg, bot)
