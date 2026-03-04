"""Integration test — runs the full stats → storage → load pipeline with mock data.

No PostgreSQL required.  Execute:
    python test_pipeline.py
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure the drift package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from drift.stats import compute_coil_stats
from drift.storage import save_coil_stats, load_all_summaries, load_last_processed_coil
from drift.comparison import compare_periods


TEST_STORAGE = Path("test_storage")
DB_LABEL = "test_db"


def _make_coil_df(coil_id: str, n_rows: int = 50, seed: int = 42) -> pd.DataFrame:
    """Generate a realistic DataFrame mimicking fetch_coil_data output."""
    rng = np.random.default_rng(seed)

    classes = ["crack", "scratch", "inclusion", "scale"]
    raw_classes = classes + ["unknown"]

    defectclass = rng.choice(classes, size=n_rows)
    # ~30 % of rows have changed class
    rawdefectclass = defectclass.copy()
    change_mask = rng.random(n_rows) < 0.3
    rawdefectclass[change_mask] = rng.choice(raw_classes, size=change_mask.sum())

    return pd.DataFrame({
        "coilid": coil_id,
        "defectclass": defectclass,
        "rawdefectclass": rawdefectclass,
        "bbox_xtl": rng.uniform(0, 800, n_rows),
        "bbox_ytl": rng.uniform(0, 600, n_rows),
        "bbox_xbr": rng.uniform(50, 850, n_rows),
        "bbox_ybr": rng.uniform(50, 650, n_rows),
        "confidence": rng.uniform(0.1, 1.0, n_rows),
    })


def test_full_pipeline():
    # Clean up previous test run
    if TEST_STORAGE.exists():
        shutil.rmtree(TEST_STORAGE)

    coils = {
        "COIL_001": _make_coil_df("COIL_001", n_rows=60, seed=1),
        "COIL_002": _make_coil_df("COIL_002", n_rows=40, seed=2),
        "COIL_003": _make_coil_df("COIL_003", n_rows=25, seed=3),
    }

    # 1. Compute + save stats for each coil
    for coil_id, df in coils.items():
        stats = compute_coil_stats(coil_id, df)
        save_coil_stats(str(TEST_STORAGE), DB_LABEL, stats)
        print(f"[OK] {coil_id}: {stats['total_defects']} defects, "
              f"{stats['class_change_summary']['changed_count']} changed")

    # 2. Watermark recovery
    last = load_last_processed_coil(str(TEST_STORAGE), DB_LABEL)
    assert last == "COIL_003", f"Expected COIL_003, got {last}"
    print(f"[OK] Watermark: {last}")

    # 3. Load all summaries
    summaries = load_all_summaries(str(TEST_STORAGE), [DB_LABEL])
    assert not summaries.empty, "Summaries should not be empty"
    coil_ids = summaries["coil_id"].unique()
    assert len(coil_ids) == 3, f"Expected 3 coils, got {len(coil_ids)}: {coil_ids}"
    assert "fetched_at" in summaries.columns
    assert "db_label" in summaries.columns
    assert set(summaries["db_label"]) == {DB_LABEL}
    print(f"[OK] Summaries loaded: {len(summaries)} rows")
    print(summaries[["coil_id", "total_defects", "changed_count", "changed_pct", "db_label"]].to_string(index=False))

    # 4. Verify parquet subdirectories exist
    db_dir = TEST_STORAGE / DB_LABEL
    for sub in ("coil_stats", "confidence", "class_changes", "class_change_top", "bbox", "spatial", "conf_buckets"):
        subdir = db_dir / sub
        files = list(subdir.glob("*.parquet"))
        assert len(files) > 0, f"No parquets in {sub}/"
        print(f"[OK] {sub}/: {len(files)} file(s)")
    # 5. Period comparison
    summaries["fetched_at"] = pd.to_datetime(summaries["fetched_at"], utc=True)
    mid = summaries["fetched_at"].median()
    period_a = summaries[summaries["fetched_at"] <= mid]
    period_b = summaries[summaries["fetched_at"] > mid]
    cmp = compare_periods(period_a, period_b)
    assert "agg_a" in cmp and "agg_b" in cmp and "delta" in cmp
    assert not cmp["delta"].empty, "Delta table should not be empty"
    print(f"[OK] Comparison: delta has {len(cmp['delta'])} rows")

    # 6. Cleanup
    shutil.rmtree(TEST_STORAGE)
    print("\n[OK] All tests passed!")


if __name__ == "__main__":
    test_full_pipeline()
