import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import os  # noqa: E402
os.environ.setdefault("ANPR_USE_PREFECT", "0")   # never send flow runs to a running Prefect server
os.environ.setdefault("ANPR_READER_MODE", "scan")   # whatever envs/.env.local says
