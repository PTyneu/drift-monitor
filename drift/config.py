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
    host: str = "localhost"
    port: int = 5432
    name: str = "defects"
    user: str = "reader"
    password: str = "reader"
    table: str = "public.defect_results"

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class WatcherConfig:
    poll_interval_sec: int = 600
    coil_id_column: str = "coilid"


@dataclass
class StorageConfig:
    dir: str = "storage"


@dataclass
class AppConfig:
    db: DbConfig = field(default_factory=DbConfig)
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
        path = os.environ.get("DRIFT_CONFIG", Path(__file__).resolve().parent.parent / "config.yaml")
    path = Path(path)

    if not path.exists():
        return AppConfig()

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    return AppConfig(
        db=DbConfig(**raw.get("db", {})),
        watcher=WatcherConfig(**raw.get("watcher", {})),
        storage=StorageConfig(**raw.get("storage", {})),
    )
