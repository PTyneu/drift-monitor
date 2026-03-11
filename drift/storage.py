"""Parquet-based storage for coil statistics.

Layout (per database)::

    storage/
      <db_label>/
        coil_stats/<coil_id>.parquet
        confidence/<coil_id>.parquet
        confidence_raw/<coil_id>.parquet   # raw (defectclass, confidence) per row
        class_changes/<coil_id>.parquet
        class_change_top/<coil_id>.parquet
        bbox/<coil_id>.parquet
        spatial/<coil_id>.parquet
        processed_coils.txt
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .stats import stats_to_frames


_SUBDIRS = (
    "coil_stats",
    "confidence",
    "confidence_raw",
    "class_changes",
    "class_change_top",
    "bbox",
    "spatial",
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

    # Shared frames (summary, confidence, class_change_top)
    frames = stats_to_frames(stats, db_label)

    frames["summary"].to_parquet(base / "coil_stats" / f"{name}.parquet", index=False)

    if not frames["confidence"].empty:
        frames["confidence"].to_parquet(base / "confidence" / f"{name}.parquet")

    if not frames["class_change_top"].empty:
        frames["class_change_top"].to_parquet(base / "class_change_top" / f"{name}.parquet", index=False)

    # Raw confidence values (defectclass + confidence float per row)
    raw_conf: pd.DataFrame = stats.get("confidence_raw", pd.DataFrame())
    if not raw_conf.empty:
        rc = raw_conf[["defectclass", "confidence"]].copy()
        rc["coil_id"] = stats["coil_id"]
        rc["fetched_at"] = fetched_at
        rc.to_parquet(base / "confidence_raw" / f"{name}.parquet", index=False)

    # Class-change transition matrix
    matrix: pd.DataFrame = stats["class_change_matrix"]
    if not matrix.empty:
        matrix = matrix.copy()
        matrix["fetched_at"] = fetched_at
        matrix.to_parquet(base / "class_changes" / f"{name}.parquet")

    # Bbox describe()
    bbox: pd.DataFrame = stats["bbox_stats"]
    if not bbox.empty:
        bbox = bbox.copy()
        bbox.columns = ["_".join(str(c) for c in col) for col in bbox.columns]
        bbox["coil_id"] = stats["coil_id"]
        bbox["fetched_at"] = fetched_at
        bbox.to_parquet(base / "bbox" / f"{name}.parquet")

    # Spatial describe()
    sp: pd.DataFrame = stats["spatial_stats"]
    if not sp.empty:
        sp = sp.copy()
        sp.columns = ["_".join(str(c) for c in col) for col in sp.columns]
        sp["coil_id"] = stats["coil_id"]
        sp["fetched_at"] = fetched_at
        sp.to_parquet(base / "spatial" / f"{name}.parquet")

    # Append to processed list
    processed = base / "processed_coils.txt"
    with open(processed, "a", encoding="utf-8") as f:
        f.write(f"{stats['coil_id']}\n")



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


def _load_subdir(base_dir: str | Path, subdir: str, db_labels: list[str] | None = None,
                  ignore_index: bool = True) -> pd.DataFrame:
    """Generic loader: concatenate all parquets from storage/<label>/<subdir>/."""
    base = Path(base_dir)
    if not base.exists():
        return pd.DataFrame()
    if db_labels is None:
        db_labels = [
            d.name for d in sorted(base.iterdir())
            if d.is_dir() and (d / subdir).is_dir()
        ]
    frames = []
    for label in db_labels:
        sub = base / label / subdir
        if not sub.exists():
            continue
        for f in sorted(sub.glob("*.parquet")):
            df = pd.read_parquet(f)
            df["db_label"] = label
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=ignore_index)


def load_all_confidence_raw(base_dir: str | Path, db_labels: list[str] | None = None) -> pd.DataFrame:
    """Load raw (defectclass, confidence) values for all coils."""
    return _load_subdir(base_dir, "confidence_raw", db_labels)


def load_all_class_change_top(base_dir: str | Path, db_labels: list[str] | None = None) -> pd.DataFrame:
    return _load_subdir(base_dir, "class_change_top", db_labels)


def load_all_confidence(base_dir: str | Path, db_labels: list[str] | None = None) -> pd.DataFrame:
    """Load and concatenate all per-coil confidence describe() parquets."""
    return _load_subdir(base_dir, "confidence", db_labels, ignore_index=False)


def load_all_summaries(base_dir: str | Path, db_labels: list[str] | None = None) -> pd.DataFrame:
    """Load and concatenate all coil_stats summaries."""
    base = Path(base_dir)
    if not base.exists():
        return pd.DataFrame()

    if db_labels is None:
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
