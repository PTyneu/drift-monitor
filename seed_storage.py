"""Seed the storage/ directory with mock coil data for Streamlit demo.

Run once:
    python seed_storage.py
Then:
    streamlit run app.py
"""

from __future__ import annotations

import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from drift.stats import compute_coil_stats
from drift.storage import save_coil_stats


STORAGE_DIR = "storage"
DB_LABEL = "demo"


def _make_coil_df(coil_id: str, n_rows: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    classes = ["crack", "scratch", "inclusion", "scale"]
    raw_classes = classes + ["unknown"]

    defectclass = rng.choice(classes, size=n_rows)
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


def main():
    # Clean previous demo data
    demo_dir = Path(STORAGE_DIR) / DB_LABEL
    if demo_dir.exists():
        shutil.rmtree(demo_dir)

    now = datetime.now(timezone.utc)

    # Coils with different timestamps for comparison demo:
    #   "old" batch  — 2 days ago
    #   "recent" batch — 1 hour ago
    coils = [
        # (coil_id, n_rows, seed, fetched_at)
        ("COIL_001", 80,  10, now - timedelta(days=2, hours=3)),
        ("COIL_002", 55,  20, now - timedelta(days=2, hours=1)),
        ("COIL_003", 30,  30, now - timedelta(days=1, hours=12)),
        ("COIL_004", 100, 40, now - timedelta(hours=1)),
        ("COIL_005", 45,  50, now - timedelta(minutes=30)),
        ("COIL_006", 70,  60, now - timedelta(minutes=15)),
    ]

    for coil_id, n_rows, seed, fetched_at in coils:
        df = _make_coil_df(coil_id, n_rows, seed)
        stats = compute_coil_stats(coil_id, df)
        # Override fetched_at to simulate different time points
        stats["fetched_at"] = fetched_at
        save_coil_stats(STORAGE_DIR, DB_LABEL, stats)
        age = now - fetched_at
        hours_ago = age.total_seconds() / 3600
        print(f"  {coil_id}: {n_rows} defects, fetched {hours_ago:.1f}h ago")

    print(f"\nDone. Storage at: {Path(STORAGE_DIR).resolve()}")


if __name__ == "__main__":
    main()
