"""Statistical analysis of a single coil's defect data.

Every function accepts a DataFrame returned by ``db.fetch_coil_data`` and
returns a plain dict / DataFrame suitable for serialisation to Parquet.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import numpy as np


# ── helpers ─────────────────────────────────────────────────────

def _bbox_dims(df: pd.DataFrame) -> pd.DataFrame:
    """Add width, height, area columns derived from bounding boxes."""
    out = df.copy()
    out["bbox_width"] = (out["bbox_xbr"] - out["bbox_xtl"]).abs()
    out["bbox_height"] = (out["bbox_ybr"] - out["bbox_ytl"]).abs()
    out["bbox_area"] = out["bbox_width"] * out["bbox_height"]
    out["bbox_aspect_ratio"] = out["bbox_width"] / out["bbox_height"].replace(0, np.nan)
    return out


# ── per-class defect counts ────────────────────────────────────

def defect_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Number of defects per ``defectclass``.

    Returns a DataFrame with columns ``[defectclass, count]``.
    """
    counts = df["defectclass"].value_counts().reset_index()
    counts.columns = ["defectclass", "count"]
    return counts


# ── confidence statistics ──────────────────────────────────────

def confidence_stats(df: pd.DataFrame) -> pd.DataFrame:
    """``describe()`` of confidence grouped by ``defectclass``.

    Returns a DataFrame indexed by defectclass with columns from describe().
    """
    if df.empty:
        return pd.DataFrame()
    return df.groupby("defectclass")["confidence"].describe()


# ── raw vs post-processed class drift ─────────────────────────

def class_change_stats(df: pd.DataFrame) -> dict:
    """Analyse how often ``rawdefectclass`` differs from ``defectclass``.

    Returns a dict with:
      - total_defects: int
      - changed_count: number of rows where raw != final
      - changed_pct: percentage
      - change_matrix: DataFrame (raw → final transition counts)
      - top_changes: most frequent (raw → final) transitions
    """
    changed = df["rawdefectclass"] != df["defectclass"]
    changed_count = int(changed.sum())
    total = len(df)

    # Transition matrix: rows = rawdefectclass, columns = defectclass
    matrix = pd.crosstab(
        df["rawdefectclass"], df["defectclass"],
        margins=True, margins_name="total",
    )

    # Top transitions (only where class actually changed)
    changed_df = df.loc[changed, ["rawdefectclass", "defectclass"]].copy()
    if not changed_df.empty:
        top = (
            changed_df
            .groupby(["rawdefectclass", "defectclass"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
    else:
        top = pd.DataFrame(columns=["rawdefectclass", "defectclass", "count"])

    return {
        "total_defects": total,
        "changed_count": changed_count,
        "changed_pct": round(changed_count / total * 100, 2) if total else 0.0,
        "change_matrix": matrix,
        "top_changes": top,
    }


# ── bounding-box size statistics ───────────────────────────────

def bbox_stats(df: pd.DataFrame) -> pd.DataFrame:
    """``describe()`` of bbox width, height, area, aspect_ratio grouped by class.

    Helps detect if defect sizes are drifting over time.
    """
    ext = _bbox_dims(df)
    cols = ["bbox_width", "bbox_height", "bbox_area", "bbox_aspect_ratio"]
    if ext.empty:
        return pd.DataFrame()
    return ext.groupby("defectclass")[cols].describe()


# ── spatial distribution ───────────────────────────────────────

def spatial_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Centre-point statistics per class (mean / std of bbox centre).

    Useful for detecting if defects cluster in different regions across coils.
    """
    ext = df.copy()
    ext["cx"] = (ext["bbox_xtl"] + ext["bbox_xbr"]) / 2
    ext["cy"] = (ext["bbox_ytl"] + ext["bbox_ybr"]) / 2
    if ext.empty:
        return pd.DataFrame()
    return ext.groupby("defectclass")[["cx", "cy"]].describe()


# ── master function ────────────────────────────────────────────

def compute_coil_stats(coil_id: str, df: pd.DataFrame) -> dict:
    """Run all analyses for one coil and return a dict of DataFrames / dicts.

    This is the single entry point called by the orchestrator.
    """
    cls_change = class_change_stats(df)
    fetched_at = datetime.now(timezone.utc)

    return {
        "coil_id": coil_id,
        "fetched_at": fetched_at,
        "total_defects": len(df),
        "defect_counts": defect_counts(df),
        "confidence_stats": confidence_stats(df),
        "confidence_raw": df[["defectclass", "confidence"]].copy() if not df.empty else pd.DataFrame(),
        "class_change_summary": {
            "changed_count": cls_change["changed_count"],
            "changed_pct": cls_change["changed_pct"],
            "total_defects": cls_change["total_defects"],
        },
        "class_change_matrix": cls_change["change_matrix"],
        "class_change_top": cls_change["top_changes"],
        "bbox_stats": bbox_stats(df),
        "spatial_stats": spatial_stats(df),
    }


def stats_to_frames(stats: dict, db_label: str) -> dict[str, pd.DataFrame]:
    """Convert compute_coil_stats() output into labeled DataFrames.

    Shared by storage (save to parquet) and live (return directly).
    Returns dict with keys: summary, confidence, class_change_top.
    """
    cid = stats["coil_id"]
    ts = stats["fetched_at"]

    summary = stats["defect_counts"].copy()
    summary["coil_id"] = cid
    summary["db_label"] = db_label
    summary["fetched_at"] = ts
    summary["total_defects"] = stats["total_defects"]
    summary["changed_count"] = stats["class_change_summary"]["changed_count"]
    summary["changed_pct"] = stats["class_change_summary"]["changed_pct"]

    conf = stats["confidence_stats"]
    if not conf.empty:
        conf = conf.copy()
        conf["coil_id"] = cid
        conf["fetched_at"] = ts
    else:
        conf = pd.DataFrame()

    top = stats["class_change_top"]
    if not top.empty:
        top = top.copy()
        top["coil_id"] = cid
        top["fetched_at"] = ts
    else:
        top = pd.DataFrame()

    return {
        "summary": summary,
        "confidence": conf,
        "class_change_top": top,
    }
