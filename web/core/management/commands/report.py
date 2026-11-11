"""
Timing report: how long a folder and a single image take, per bot, with the slowest ones.

    python manage.py report                         # last 24 hours, on screen   (make report)
    python manage.py report --since 7d --bot bot-1  # a period and one bot      (make report SINCE=7d BOT=bot-1)
    python manage.py report --since all --out /tmp/r  # also writes r.html, r_folders.csv, r_images.csv
                                                    #                           (make report-files)

What is measured:
  * folder time  = the bot started the folder -> its results were saved   (WorkItem.claimed_at -> finished_at)
  * queue wait   = the folder was found -> a bot started it               (WorkItem.created_at -> claimed_at)
  * image time   = rate-limit wait + reading the file + OCR call + retries (Image.duration_ms)
                   Images processed before timing was added have no image time.
  * throughput   = folders finished per hour between the first start and the last finish in the period
"""
import csv
import html
import re
from datetime import timedelta
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Avg, Count, Max, Sum
from django.utils import timezone

from core.models import Image, WorkItem


# ---------------------------------------------------------------- helpers
def parse_since(text: str):
    if text == "all":
        return None
    m = re.fullmatch(r"(\d+)([mhd])", text)
    if not m:
        raise CommandError("--since must look like 30m, 24h, 7d or all")
    n, unit = int(m[1]), m[2]
    return timezone.now() - timedelta(**{{"m": "minutes", "h": "hours", "d": "days"}[unit]: n})


def stats(values: list[float]) -> dict:
    """count, avg, min, p50, p95, max of a list of seconds."""
    if not values:
        return {"count": 0}
    v = sorted(values)
    pick = lambda q: v[min(len(v) - 1, round(q * (len(v) - 1)))]
    return {"count": len(v), "avg": sum(v) / len(v), "min": v[0], "p50": pick(0.5), "p95": pick(0.95), "max": v[-1]}


def fmt(seconds) -> str:
    if seconds is None:
        return "-"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 120:
        return f"{seconds:.2f} s"
    if seconds < 7200:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.1f} h"


def busy_seconds(intervals) -> float:
    """Total time covered by at least one (start, end) interval: idle gaps don't count."""
    total, cur_start, cur_end = 0.0, None, None
    for start, end in sorted(intervals):
        if cur_end is None or start > cur_end:
            if cur_end is not None:
                total += (cur_end - cur_start).total_seconds()
            cur_start, cur_end = start, end
        else:
            cur_end = max(cur_end, end)
    if cur_end is not None:
        total += (cur_end - cur_start).total_seconds()
    return total


STAT_COLS = ["count", "avg", "min", "p50", "p95", "max"]


def stat_row(label: str, s: dict) -> list[str]:
    return [label, str(s["count"])] + [fmt(s.get(k)) if s["count"] else "-" for k in STAT_COLS[1:]]


# ---------------------------------------------------------------- the report
class Command(BaseCommand):
    help = "Timing report: time per folder and per image, per bot, slowest items"

    def add_arguments(self, parser):
        parser.add_argument("--since", default="24h", help="period: 30m, 24h, 7d or all (default 24h)")
        parser.add_argument("--bot", help="only this bot")
        parser.add_argument("--top", type=int, default=5, help="how many slowest folders/images to list")
        parser.add_argument("--out", help="also write OUT.html, OUT_folders.csv and OUT_images.csv")

    def handle(self, *args, **o):
        since = parse_since(o["since"])
        period = "all time" if since is None else f"last {o['since']}"
        scope = f"bot {o['bot']}" if o["bot"] else "all bots"

        # Folders finished in the period (done or failed after all attempts)
        items = WorkItem.objects.filter(finished_at__isnull=False, claimed_at__isnull=False)
        if since:
            items = items.filter(finished_at__gte=since)
        if o["bot"]:
            items = items.filter(bot_name=o["bot"])
        items = items.select_related("folder").annotate(n_images=Count("folder__images"),
                                                        avg_ms=Avg("folder__images__duration_ms"),
                                                        max_ms=Max("folder__images__duration_ms"),
                                                        sum_ms=Sum("folder__images__duration_ms"))
        folders = []
        for w in items.iterator(chunk_size=2000):
            folders.append({
                "name": Path(w.path).name, "path": w.path, "bot": w.bot_name or "-", "status": w.status,
                "images": w.n_images, "seconds": (w.finished_at - w.claimed_at).total_seconds(),
                "wait": (w.claimed_at - w.created_at).total_seconds(),
                "avg_image": w.avg_ms / 1000 if w.avg_ms is not None else None,
                "max_image": w.max_ms / 1000 if w.max_ms is not None else None,
                "sum_images": w.sum_ms / 1000 if w.sum_ms is not None else None,
                "plate": w.folder.final_plate if w.folder else "", "attempts": w.attempts,
                "started": timezone.localtime(w.claimed_at), "finished": timezone.localtime(w.finished_at),
            })

        # Images processed in the period
        imgs = Image.objects.filter(folder__workitem__isnull=False)
        if since:
            imgs = imgs.filter(processed_at__gte=since)
        if o["bot"]:
            imgs = imgs.filter(folder__processed_by=o["bot"])
        images = [{"file": f, "folder": Path(p).parent.name, "path": p, "bot": b or "-",
                   "seconds": ms / 1000 if ms is not None else None, "attempts": att or 0,
                   "error": err, "plate": pl}
                  for f, p, b, ms, att, err, pl in imgs.values_list(
                      "file_name", "path", "folder__processed_by", "duration_ms", "api_attempts", "api_error", "plate"
                  ).iterator(chunk_size=5000)]
        timed = [i for i in images if i["seconds"] is not None]

        # Summary numbers
        done = [f for f in folders if f["status"] == WorkItem.Status.DONE]
        failed = [f for f in folders if f["status"] == WorkItem.Status.FAILED]
        span = busy_seconds([(f["started"], f["finished"]) for f in folders])
        per_hour = len(folders) / (span / 3600) if span > 0 else None
        pending = WorkItem.objects.filter(status=WorkItem.Status.PENDING).count()
        in_progress = WorkItem.objects.filter(status=WorkItem.Status.IN_PROGRESS).count()

        summary = [
            ["Folders finished", f"{len(folders)}  ({len(done)} done, {len(failed)} failed)"],
            ["Images sent to OCR", f"{len(images)}  ({len(timed)} with timing)"],
            ["Images with API error", f"{sum(1 for i in images if i['error'])}"],
            ["Images that needed a retry", f"{sum(1 for i in timed if i['attempts'] > 1)}"],
            ["Busy time (at least one folder running)", fmt(span) if folders else "-"],
            ["Throughput while busy", f"{per_hour:,.0f} folders/hour" if per_hour else "-"],
            ["Images per folder (avg)", f"{len(images) / len(folders):.1f}" if folders else "-"],
            ["Queue now", f"{pending} waiting, {in_progress} in progress"],
        ]
        timing = [
            stat_row("Folder (bot start -> saved)", stats([f["seconds"] for f in folders])),
            stat_row("Queue wait (found -> started)", stats([f["wait"] for f in folders])),
            stat_row("One image (read + OCR + retries)", stats([i["seconds"] for i in timed])),
            stat_row("All images of a folder, added up",
                     stats([f["sum_images"] for f in folders if f["sum_images"] is not None])),
        ]
        per_bot = []
        for bot in sorted({f["bot"] for f in folders}):
            fb = [f for f in folders if f["bot"] == bot]
            ib = [i for i in timed if i["bot"] == bot]
            sf, si = stats([f["seconds"] for f in fb]), stats([i["seconds"] for i in ib])
            per_bot.append([bot, str(len(fb)), str(sum(f["images"] for f in fb)),
                            fmt(sf.get("avg")), fmt(sf.get("p95")), fmt(si.get("avg")), fmt(si.get("p95")),
                            str(sum(1 for f in fb if f["status"] == WorkItem.Status.FAILED))])
        top = o["top"]
        slow_folders = [[f["name"], f["bot"], str(f["images"]), fmt(f["seconds"]), fmt(f["max_image"])]
                        for f in sorted(folders, key=lambda f: f["seconds"], reverse=True)[:top]]
        slow_images = [[i["folder"] + "/" + i["file"], i["bot"], fmt(i["seconds"]), str(i["attempts"]),
                        (i["error"][:40] or "ok")]
                       for i in sorted(timed, key=lambda i: i["seconds"], reverse=True)[:top]]

        sections = [
            ("Summary", ["", ""], summary),
            ("Timing", ["", "count", "avg", "min", "median", "p95", "max"], timing),
            ("Per bot", ["bot", "folders", "images", "folder avg", "folder p95", "image avg", "image p95", "failed"],
             per_bot),
            (f"Slowest {top} folders", ["folder", "bot", "images", "time", "slowest image"], slow_folders),
            (f"Slowest {top} images", ["image", "bot", "time", "OCR calls", "result"], slow_images),
        ]
        title = f"ANPR timing report: {period}, {scope}"
        generated = timezone.localtime().strftime("%Y-%m-%d %H:%M")

        self.print_text(title, generated, sections, has_untimed=len(timed) < len(images))
        if o["out"]:
            out = Path(o["out"]).expanduser().resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            self.write_html(out.with_name(out.name + ".html"), title, generated, sections)
            self.write_csv(out.with_name(out.name + "_folders.csv"), folders,
                           ["name", "path", "bot", "status", "images", "seconds", "wait", "avg_image", "max_image", "sum_images",
                            "attempts", "plate", "started", "finished"])
            self.write_csv(out.with_name(out.name + "_images.csv"), images,
                           ["folder", "file", "path", "bot", "seconds", "attempts", "plate", "error"])
            self.stdout.write(f"\nWritten:\n  {out}.html\n  {out}_folders.csv\n  {out}_images.csv")

    # ---------------------------------------------------------------- output
    def print_text(self, title, generated, sections, has_untimed):
        self.stdout.write(f"{title}   (generated {generated})")
        for name, head, rows in sections:
            self.stdout.write(f"\n{name}")
            if not rows:
                self.stdout.write("  (nothing in this period)")
                continue
            table = ([head] if any(head) else []) + rows
            widths = [max(len(r[c]) for r in table) for c in range(len(head))]
            numeric = any(head)                       # key/value tables (Summary) stay left-aligned
            for r in table:
                self.stdout.write("  " + "  ".join(
                    cell.ljust(w) if c == 0 or not numeric else cell.rjust(w)
                    for c, (cell, w) in enumerate(zip(r, widths))).rstrip())
        if has_untimed:
            self.stdout.write("\nNote: images processed before timing was added have no image time; "
                              "they count in totals but not in image timing.")

    def write_csv(self, path: Path, rows: list[dict], cols: list[str]):
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: (round(v, 3) if isinstance(v, float) else v) for k, v in r.items()})

    def write_html(self, path: Path, title, generated, sections):
        e = html.escape
        parts = []
        for name, head, rows in sections:
            parts.append(f"<h2>{e(name)}</h2>")
            if not rows:
                parts.append("<p class=muted>Nothing in this period.</p>")
                continue
            thead = "" if not any(head) else "<tr>" + "".join(f"<th>{e(h)}</th>" for h in head) + "</tr>"
            body = "".join("<tr>" + "".join(f"<td>{e(c)}</td>" for c in r) + "</tr>" for r in rows)
            parts.append(f"<table>{thead}{body}</table>")
        path.write_text(f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>ANPR timing report</title>
<style>
body{{font:15px/1.5 system-ui,sans-serif;color:#1d252c;background:#fff;max-width:960px;margin:32px auto;padding:0 16px}}
h1{{font-size:22px;margin:0}} h2{{font-size:17px;margin:28px 0 8px}} .muted{{color:#5b6670}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}
th,td{{padding:6px 10px;border-bottom:1px solid #e3e7ea;text-align:right;white-space:nowrap}}
th:first-child,td:first-child{{text-align:left;white-space:normal}} th{{color:#5b6670;font-weight:600;font-size:13px}}
</style>
<h1>{e(title)}</h1><p class=muted>Generated {e(generated)}. Folder time = bot start to results saved.
Image time = rate-limit wait + reading the file + OCR call + retries.</p>
{''.join(parts)}
</html>""")
