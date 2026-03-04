"""Configuration loader.

Reads config.yaml and exposes a typed dataclass tree.
The path to config.yaml can be overridden via the env var DRIFT_CONFIG.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class DbConfig:
    label: str = "default"
    host: str = "localhost"
    port: int = 5432
    dbname: str = "defects"
    user: str = "reader"
    password: str = "reader"
    table: str = "public.defect_results"
    timestamp_column: str = ""


@dataclass
class WatcherConfig:
    poll_interval_sec: int = 600


@dataclass
class StorageConfig:
    dir: str = "storage"


@dataclass
class AppConfig:
    live: bool = True
    databases: list[DbConfig] = field(default_factory=lambda: [DbConfig()])
    watcher: WatcherConfig = field(default_factory=WatcherConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load configuration from a YAML file.

    Resolution order:
      1. Explicit *path* argument.
      2. ``DRIFT_CONFIG`` environment variable.
      3. ``config.yaml`` next to the project root.
    """
    if path is None:
        path = os.environ.get(
            "DRIFT_CONFIG",
            Path(__file__).resolve().parent.parent / "config.yaml",
        )
    path = Path(path)

    if not path.exists():
        return AppConfig()

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # Parse database list
    db_list_raw = raw.get("databases", [])
    if not db_list_raw:
        # Backward compat: single "db:" key
        single = raw.get("db")
        if single:
            # Remap old "name" key to "dbname"
            if "name" in single and "dbname" not in single:
                single["dbname"] = single.pop("name")
            db_list_raw = [single]

    databases = []
    for db_raw in db_list_raw:
        # Remap old "name" key if present
        if "name" in db_raw and "dbname" not in db_raw and "label" not in db_raw:
            db_raw["dbname"] = db_raw.pop("name")
        databases.append(DbConfig(**db_raw))

    if not databases:
        databases = [DbConfig()]

    return AppConfig(
        live=raw.get("live", True),
        databases=databases,
        watcher=WatcherConfig(**raw.get("watcher", {})),
        storage=StorageConfig(**raw.get("storage", {})),
    )
