import argparse

import httpx
from prefect import flow, get_run_logger, task, unmapped

from pipeline.config import load_config


@task(task_run_name="scan via {url}", retries=2, retry_delay_seconds=10)
def scan_with_reader(url: str, timeout: float) -> dict:
    response = httpx.post(f"{url}/scan", timeout=timeout)
    response.raise_for_status()          # an error reply makes the task fail, and Prefect retries it
    result = response.json()
    log = get_run_logger()
    for r in result["results"]:
        log.info(f"[{result['instance']} shard {result['shard']}] {r['source']}: "
                 f"{r['registered']} new, {r['already_known']} known, {r['too_new']} too new, "
                 f"{r['other_shard']} for other copies, {len(r['errors'])} errors, {r['seconds']} s")
        for error in r["errors"][:20]:
            log.warning(f"{r['source']}: {error}")
    return result


@flow(name="scan-sources", log_prints=True)
def scan_sources() -> dict:
    cfg = load_config()["reader"]
    futures = scan_with_reader.map(cfg["instances"], unmapped(cfg["scan_timeout_seconds"]))
    total = {"registered": 0, "images": 0, "errors": 0, "copies_failed": 0}
    for future in futures:
        result = future.result(raise_on_failure=False)
        if isinstance(result, BaseException):      # one copy down shouldn't hide the others' results
            total["copies_failed"] += 1
            continue
        total["registered"] += result["registered"]
        total["images"] += sum(r["images"] for r in result["results"])
        total["errors"] += sum(len(r["errors"]) for r in result["results"])
    print(f"Scan finished: {total['registered']} new folders, {total['images']} images, "
          f"{total['errors']} errors, {total['copies_failed']} reader copies unreachable")
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="keep running and scan on a schedule")
    args = ap.parse_args()
    if args.serve:
        # Imported by module name so Prefect can load it in its own process (see the bot-mode lesson).
        from pipeline.intake_flow import scan_sources as scheduled
        scheduled.serve(name="reader-scan", interval=load_config()["reader"]["scan_every_seconds"])
    else:
        scan_sources()
