"""
Deletes old flow runs (and their task runs, states and logs) from the Prefect database.
The app database (results, queue) is NOT touched: this only shortens Prefect's history.

    python scripts/prune_prefect_runs.py --days 30            # delete runs older than 30 days
    python scripts/prune_prefect_runs.py --days 30 --dry-run  # only count them

Needs the Prefect server running (it talks to PREFECT_API_URL).
"""
import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from prefect import get_client
from prefect.client.schemas.filters import FlowFilter, FlowFilterName, FlowRunFilter, FlowRunFilterStartTime

FLOW_NAME = "anpr-bot-folder"


async def main(days: int, dry_run: bool, batch: int = 200) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    flow_filter = FlowFilter(name=FlowFilterName(any_=[FLOW_NAME]))
    run_filter = FlowRunFilter(start_time=FlowRunFilterStartTime(before_=cutoff))
    deleted = 0
    async with get_client() as client:
        while True:
            runs = await client.read_flow_runs(flow_filter=flow_filter, flow_run_filter=run_filter, limit=batch)
            if not runs:
                break
            if dry_run:
                deleted += len(runs)
                if len(runs) < batch:
                    break
                print(f"{deleted}+ runs would be deleted (dry run stops counting here)")
                return
            for run in runs:
                await client.delete_flow_run(run.id)
            deleted += len(runs)
            print(f"deleted {deleted} so far…")
    verb = "would be deleted" if dry_run else "deleted"
    print(f"{deleted} '{FLOW_NAME}' run(s) older than {days} days {verb}.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.days, a.dry_run))
