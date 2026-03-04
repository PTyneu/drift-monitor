"""Period-to-period comparison of coil statistics.

Given two DataFrames of coil summaries (period A and period B), produces:
* **Side-by-side** aggregated tables for visual comparison.
* **Delta table** with absolute and percentage differences per metric.
"""

from __future__ import annotations

import pandas as pd


_AGG_METRICS = {
    "total_defects": "sum",
    "changed_count": "sum",
    "changed_pct": "mean",
    "count": "sum",
}


def _aggregate(summaries: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-class rows into one summary row per defectclass.

    ``total_defects`` and ``changed_count`` are per-coil values (duplicated
    across all defectclass rows of the same coil), so we deduplicate by
    coil_id before summing them.
    """
    if summaries.empty:
        return pd.DataFrame()

    # Per-class metrics: aggregate directly
    per_class_agg = {}
    for col in ("count",):
        if col in summaries.columns:
            per_class_agg[col] = "sum"

    if "defectclass" in summaries.columns:
        result = summaries.groupby("defectclass", as_index=False).agg(per_class_agg) if per_class_agg else pd.DataFrame()

        # Per-coil metrics: deduplicate first, then sum
        if "coil_id" in summaries.columns:
            coil_level = summaries.drop_duplicates(subset=["coil_id"])
            coil_agg = {}
            for col in ("total_defects", "changed_count"):
                if col in coil_level.columns:
                    coil_agg[col] = "sum"
            if "changed_pct" in coil_level.columns:
                coil_agg["changed_pct"] = "mean"
            if coil_agg:
                coil_summary = coil_level.agg(coil_agg).to_frame().T
                coil_summary.columns = list(coil_agg.keys())
                for col_name in coil_summary.columns:
                    result[col_name] = coil_summary[col_name].iloc[0]

        return result

    # No defectclass column — aggregate all rows
    agg = {}
    for col, func in _AGG_METRICS.items():
        if col in summaries.columns:
            agg[col] = func
    out = {}
    for col, func in agg.items():
        out[col] = [getattr(summaries[col], func)()]
    return pd.DataFrame(out)


def compare_periods(
    summaries_a: pd.DataFrame,
    summaries_b: pd.DataFrame,
) -> dict:
    """Compare two periods and return side-by-side + delta tables.

    Returns a dict with keys:
        ``agg_a``   — aggregated DataFrame for period A
        ``agg_b``   — aggregated DataFrame for period B
        ``delta``   — merged DataFrame with _a, _b, diff, diff_pct columns
    """
    agg_a = _aggregate(summaries_a)
    agg_b = _aggregate(summaries_b)

    if agg_a.empty and agg_b.empty:
        return {"agg_a": agg_a, "agg_b": agg_b, "delta": pd.DataFrame()}

    # If one side is empty, still produce a delta with zeroes on the empty side
    if agg_a.empty and not agg_b.empty:
        agg_a = agg_b.copy()
        for col in agg_a.columns:
            if col != "defectclass":
                agg_a[col] = 0
    elif agg_b.empty and not agg_a.empty:
        agg_b = agg_a.copy()
        for col in agg_b.columns:
            if col != "defectclass":
                agg_b[col] = 0

    # Numeric columns to compare
    numeric_cols = [c for c in _AGG_METRICS if c in agg_a.columns or c in agg_b.columns]

    if "defectclass" in agg_a.columns and "defectclass" in agg_b.columns:
        delta = pd.merge(
            agg_a, agg_b,
            on="defectclass", how="outer", suffixes=("_a", "_b"),
        )
    else:
        delta = pd.concat(
            [agg_a.add_suffix("_a"), agg_b.add_suffix("_b")],
            axis=1,
        )

    for col in numeric_cols:
        col_a = f"{col}_a"
        col_b = f"{col}_b"
        if col_a in delta.columns and col_b in delta.columns:
            delta[col_a] = delta[col_a].fillna(0)
            delta[col_b] = delta[col_b].fillna(0)
            delta[f"{col}_diff"] = delta[col_b] - delta[col_a]
            delta[f"{col}_diff_pct"] = (
                (delta[f"{col}_diff"] / delta[col_a].replace(0, float("nan"))) * 100
            ).round(2)

    return {"agg_a": agg_a, "agg_b": agg_b, "delta": delta}
