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
class StorageConfig:
    dir: str = "storage"


@dataclass
class AppConfig:
    databases: list[DbConfig] = field(default_factory=lambda: [DbConfig()])
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

    db_list_raw = raw.get("databases", [])
    databases = []
    for db_raw in db_list_raw:
        databases.append(DbConfig(**db_raw))

    if not databases:
        databases = [DbConfig()]

    return AppConfig(
        databases=databases,
        storage=StorageConfig(**raw.get("storage", {})),
    )
