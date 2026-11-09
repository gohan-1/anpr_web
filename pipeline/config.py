"""Loads config/settings.yaml once and exposes it as a dict."""
import os
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("ANPR_CONFIG", ROOT / "config" / "settings.yaml"))


@lru_cache
def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    if os.environ.get("ANPR_API_KEY"):
        cfg["api"]["api_key"] = os.environ["ANPR_API_KEY"]
    return cfg