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
    if os.environ.get("ANPR_USE_PREFECT"):          # "0"/"1": tests switch Prefect off this way
        cfg["pipeline"]["use_prefect"] = os.environ["ANPR_USE_PREFECT"].strip().lower() in ("1", "true", "yes")
    return cfg


def reader_mode() -> str:
    """How reader bots find new folders: "scan" (folders on disk) or "kafka" (the folders topic).
    ANPR_READER_MODE in the env file wins over reader.mode in settings.yaml."""
    return (os.environ.get("ANPR_READER_MODE") or load_config()["reader"].get("mode", "scan")).strip().lower()