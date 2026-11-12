"""
Optional: record every batch in Prefect (pipeline.use_prefect: true in settings.yaml).

Each batch becomes one flow run named after its bot ("ocr-2 batch 17"), and every item
in it one task run, so the Prefect dashboard shows what each bot did, item by item.
The work itself is the same handler the plain mode uses.
"""
from prefect import flow, task, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

from .handlers import CONTEXTS, HANDLERS, Outcome


@task(task_run_name="{stage} item {payload[id]}")
def handle_item(stage: str, payload: dict, scfg: dict, bot: str) -> Outcome:
    # Only the bot's NAME travels through Prefect; its API account stays in this process.
    try:
        return HANDLERS[stage](payload, scfg, CONTEXTS.get(bot))
    except Exception as exc:
        return Outcome(payload["id"], False, error=f"{type(exc).__name__}: {exc}")


@flow(name="stage-batch", task_runner=ThreadPoolTaskRunner(max_workers=8))
def stage_batch(stage: str, payloads: list[dict], scfg: dict, bot: str) -> list[Outcome]:
    futures = handle_item.map(stage, payloads, unmapped(scfg), unmapped(bot))
    return [f.result() for f in futures]


def run_batch(stage: str, bot: str, batch_no: int, payloads: list[dict], scfg: dict) -> list[Outcome]:
    return stage_batch.with_options(flow_run_name=f"{bot} batch {batch_no}")(stage, payloads, scfg, bot)
