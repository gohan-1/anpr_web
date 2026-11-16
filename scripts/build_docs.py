"""
Builds docs/ANPR-Guide.pdf from docs/GUIDE.md (+ docs/DATABASE.md as Appendix A) and docs/figures/:  make docs

    docs/GUIDE.md  --(python-markdown)-->  HTML with print styles  --(headless Chrome)-->  PDF

Edit GUIDE.md (and the .svg figures), then run `make docs` again. Needs the `markdown`
package (in requirements.txt) and Google Chrome or Chromium.
"""
import datetime
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent.parent / "docs"
SOURCE, OUT = DOCS / "GUIDE.md", DOCS / "ANPR-Guide.pdf"

CSS = """
@page { size: A4; margin: 16mm 15mm 18mm 15mm; }
body { font: 10.5pt/1.5 "Segoe UI", "Helvetica Neue", Arial, sans-serif; color: #1f2933; }
h1 { display: none; }
h2 { font-size: 19pt; margin: 0 0 10pt; padding-bottom: 5pt; border-bottom: 2px solid #3b6ea5;
     break-before: page; color: #18212a; }
h3 { font-size: 13pt; margin: 16pt 0 6pt; color: #2b3a47; break-after: avoid; }
h4 { font-size: 11.5pt; margin: 14pt 0 5pt; color: #2b3a47; break-after: avoid; }
p, li { orphans: 3; widows: 3; }
a { color: #2c5d91; text-decoration: none; }
code { font: 9pt Consolas, Menlo, monospace; background: #f1f3f5; padding: 0 3px; border-radius: 3px; }
pre { background: #f6f8fa; border: 1px solid #dde3e8; border-radius: 5px; padding: 8pt 10pt;
      font-size: 8.6pt; line-height: 1.4; white-space: pre-wrap; break-inside: avoid; }
pre code { background: none; padding: 0; }
table { border-collapse: collapse; width: 100%; margin: 8pt 0 12pt; font-size: 9pt; break-inside: auto; }
th, td { border: 1px solid #d3d8dc; padding: 4pt 6pt; text-align: left; vertical-align: top; }
th { background: #eef2f5; }
tr { break-inside: avoid; }
img { display: block; width: 100%; margin: 8pt 0 14pt; break-inside: avoid; }
hr { display: none; }
.cover { height: 250mm; display: flex; flex-direction: column; justify-content: center; }
.cover h0 { display: block; font-size: 34pt; font-weight: 700; color: #18212a; }
.cover .sub { font-size: 15pt; color: #3b6ea5; margin-top: 6pt; }
.cover .meta { margin-top: 40pt; color: #5b6670; font-size: 10.5pt; }
.cover .bar { width: 70mm; height: 6px; background: #f2c230; margin: 18pt 0; }
"""


def chrome() -> str:
    for name in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser"):
        if path := shutil.which(name):
            return path
    sys.exit("Google Chrome or Chromium is needed to print the PDF.")


def main() -> None:
    text = SOURCE.read_text()
    # Appendix A: the table reference. Its title becomes a chapter; its own links point inside the PDF.
    db = (DOCS / "DATABASE.md").read_text()
    db = re.sub(r"^(#{2,3}) ", lambda m: "#" + m.group(1) + " ", db, flags=re.M)   # its sections sit inside the appendix
    db = db.replace("# Database reference:", "## Appendix A. Database reference:", 1)
    text += "\n\n" + db.replace("](DATABASE.md)", "](#appendix-a-database-reference-every-table-what-it-stores-and-why-it-matters)")
    text = text.replace("](DATABASE.md)", "](#appendix-a-database-reference-every-table-what-it-stores-and-why-it-matters)")
    body = markdown.markdown(text, extensions=["tables", "fenced_code", "toc", "sane_lists"])
    cover = f"""<section class="cover">
      <h0>ANPR</h0><div class="sub">Developer and Operator Guide</div><div class="bar"></div>
      <div>Architecture · assembly line · bots · Kafka · setups · configuration · tuning · changing the code</div>
      <div class="meta">Built {datetime.date.today():%d %B %Y} from docs/GUIDE.md (make docs)</div></section>"""
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>ANPR Guide</title>
      <base href="{DOCS.as_uri()}/"><style>{CSS}</style></head><body>{cover}{body}</body></html>"""
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp) / "guide.html"
        page.write_text(html)
        subprocess.run([chrome(), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                        "--allow-file-access-from-files", f"--print-to-pdf={OUT}", page.as_uri()],
                       check=True, capture_output=True, timeout=120)
    print(f"Written: {OUT} ({OUT.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
