"""Parquet-based storage for coil statistics.

Layout (per database)::

    storage/
      <db_label>/
        coil_stats/<coil_id>.parquet
        confidence/<coil_id>.parquet
        class_changes/<coil_id>.parquet
        class_change_top/<coil_id>.parquet
        bbox/<coil_id>.parquet
        spatial/<coil_id>.parquet
        conf_buckets/<coil_id>.parquet
        processed_coils.txt
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


_SUBDIRS = (
    "coil_stats",
    "confidence",
    "class_changes",
    "class_change_top",
    "bbox",
    "spatial",
    "conf_buckets",
)


def _db_dir(base_dir: str | Path, db_label: str) -> Path:
    """Return the storage root for a given database label."""
    return Path(base_dir) / db_label


def _ensure_dirs(base: Path) -> None:
    for sub in _SUBDIRS:
        (base / sub).mkdir(parents=True, exist_ok=True)


def _safe_name(coil_id: str) -> str:
    """Sanitise coil_id for use as a filename."""
    return str(coil_id).replace("/", "_").replace("\\", "_")


def save_coil_stats(base_dir: str | Path, db_label: str, stats: dict) -> None:
    """Persist all computed statistics for a single coil."""
    base = _db_dir(base_dir, db_label)
    _ensure_dirs(base)
    name = _safe_name(stats["coil_id"])

    fetched_at = stats["fetched_at"]

    # 1. Per-class defect counts + high-level summary
    summary = stats["defect_counts"].copy()
    summary["coil_id"] = stats["coil_id"]
    summary["db_label"] = db_label
    summary["fetched_at"] = fetched_at
    summary["total_defects"] = stats["total_defects"]
    summary["changed_count"] = stats["class_change_summary"]["changed_count"]
    summary["changed_pct"] = stats["class_change_summary"]["changed_pct"]
    summary.to_parquet(base / "coil_stats" / f"{name}.parquet", index=False)

    # 2. Confidence describe()
    conf: pd.DataFrame = stats["confidence_stats"]
    if not conf.empty:
        conf = conf.copy()
        conf["coil_id"] = stats["coil_id"]
        conf["fetched_at"] = fetched_at
        conf.to_parquet(base / "confidence" / f"{name}.parquet")

    # 3. Class-change transition matrix
    matrix: pd.DataFrame = stats["class_change_matrix"]
    if not matrix.empty:
        matrix = matrix.copy()
        matrix["fetched_at"] = fetched_at
        matrix.to_parquet(base / "class_changes" / f"{name}.parquet")

    # 4. Top class transitions
    top: pd.DataFrame = stats["class_change_top"]
    if not top.empty:
        top = top.copy()
        top["coil_id"] = stats["coil_id"]
        top["fetched_at"] = fetched_at
        top.to_parquet(base / "class_change_top" / f"{name}.parquet", index=False)

    # 5. Bbox describe()
    bbox: pd.DataFrame = stats["bbox_stats"]
    if not bbox.empty:
        bbox = bbox.copy()
        bbox.columns = ["_".join(str(c) for c in col) for col in bbox.columns]
        bbox["coil_id"] = stats["coil_id"]
        bbox["fetched_at"] = fetched_at
        bbox.to_parquet(base / "bbox" / f"{name}.parquet")

    # 6. Spatial describe()
    sp: pd.DataFrame = stats["spatial_stats"]
    if not sp.empty:
        sp = sp.copy()
        sp.columns = ["_".join(str(c) for c in col) for col in sp.columns]
        sp["coil_id"] = stats["coil_id"]
        sp["fetched_at"] = fetched_at
        sp.to_parquet(base / "spatial" / f"{name}.parquet")

    # 7. Confidence buckets
    cb: pd.DataFrame = stats["confidence_buckets"]
    if not cb.empty:
        cb = cb.copy()
        cb["coil_id"] = stats["coil_id"]
        cb["fetched_at"] = fetched_at
        cb["conf_bucket"] = cb["conf_bucket"].astype(str)
        cb.to_parquet(base / "conf_buckets" / f"{name}.parquet", index=False)

    # 8. Append to processed list
    processed = base / "processed_coils.txt"
    with open(processed, "a", encoding="utf-8") as f:
        f.write(f"{stats['coil_id']}\n")


def load_processed_coils(base_dir: str | Path, db_label: str) -> set[str]:
    """Return the set of coil IDs that have already been processed."""
    path = _db_dir(base_dir, db_label) / "processed_coils.txt"
    if not path.exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def load_last_processed_coil(base_dir: str | Path, db_label: str) -> str | None:
    """Return the most recently processed coil ID (last line of the log)."""
    path = _db_dir(base_dir, db_label) / "processed_coils.txt"
    if not path.exists():
        return None
    last = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                last = stripped
    return last


def load_all_summaries(base_dir: str | Path, db_labels: list[str] | None = None) -> pd.DataFrame:
    """Load and concatenate all coil_stats summaries.

    If *db_labels* is provided, only those databases are included.
    Otherwise all subdirectories with a ``coil_stats/`` folder are scanned.
    """
    base = Path(base_dir)
    if not base.exists():
        return pd.DataFrame()

    if db_labels is None:
        # Auto-discover: any subdir that contains coil_stats/
        db_labels = [
            d.name for d in sorted(base.iterdir())
            if d.is_dir() and (d / "coil_stats").is_dir()
        ]

    frames = []
    for label in db_labels:
        stats_dir = base / label / "coil_stats"
        if not stats_dir.exists():
            continue
        for f in sorted(stats_dir.glob("*.parquet")):
            frames.append(pd.read_parquet(f))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
