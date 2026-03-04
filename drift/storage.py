"""Parquet-based storage for coil statistics.

Why Parquet?
  - Columnar, compressed — 5–10× smaller than CSV for numeric data.
  - Native pandas read/write — no extra dependencies.
  - Appendable via simple file-per-coil pattern (no server process needed).
  - Easily queryable later via DuckDB / Polars for dashboards.

Layout::

    storage/
      coil_stats/
        <coil_id>.parquet          # per-class summary row per coil
      confidence/
        <coil_id>.parquet          # per-class describe() output
      class_changes/
        <coil_id>.parquet          # transition matrix
      bbox/
        <coil_id>.parquet          # bbox describe() per class
      spatial/
        <coil_id>.parquet          # centre-point describe()
      conf_buckets/
        <coil_id>.parquet          # confidence histogram
      processed_coils.txt          # one coil_id per line
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


def _ensure_dirs(base: Path) -> None:
    for sub in _SUBDIRS:
        (base / sub).mkdir(parents=True, exist_ok=True)


def _safe_name(coil_id: str) -> str:
    """Sanitise coil_id for use as a filename."""
    return str(coil_id).replace("/", "_").replace("\\", "_")


def save_coil_stats(base_dir: str | Path, stats: dict) -> None:
    """Persist all computed statistics for a single coil."""
    base = Path(base_dir)
    _ensure_dirs(base)
    name = _safe_name(stats["coil_id"])

    fetched_at = stats["fetched_at"]

    # 1. Per-class defect counts + high-level summary
    summary = stats["defect_counts"].copy()
    summary["coil_id"] = stats["coil_id"]
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
        # MultiIndex columns → flatten for parquet
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
        # pd.Interval не сериализуется в parquet → строка
        cb["conf_bucket"] = cb["conf_bucket"].astype(str)
        cb.to_parquet(base / "conf_buckets" / f"{name}.parquet", index=False)

    # 8. Append to processed list
    processed = base / "processed_coils.txt"
    with open(processed, "a", encoding="utf-8") as f:
        f.write(f"{stats['coil_id']}\n")


def load_processed_coils(base_dir: str | Path) -> set[str]:
    """Return the set of coil IDs that have already been processed."""
    path = Path(base_dir) / "processed_coils.txt"
    if not path.exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def load_last_processed_coil(base_dir: str | Path) -> str | None:
    """Return the most recently processed coil ID (last line of the log).

    Used as a watermark for ``WHERE coilid > %s`` queries so that we never
    send the full processed set to PostgreSQL.
    """
    path = Path(base_dir) / "processed_coils.txt"
    if not path.exists():
        return None
    last = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                last = stripped
    return last


def load_all_summaries(base_dir: str | Path) -> pd.DataFrame:
    """Load and concatenate all coil_stats summaries into one DataFrame.

    Useful for building time-series / trend charts later.
    """
    base = Path(base_dir) / "coil_stats"
    if not base.exists():
        return pd.DataFrame()
    files = sorted(base.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
