"""Generate synthetic data for compare_app.py demo.

Creates:
  - storage/test/  — parquet stats for 6 coils (test environment)
  - storage/prod/  — parquet stats for the same 6 coils (prod environment,
                     slightly different distributions to make comparison meaningful)
  - training_data.csv — simulated training CSV with instance_label + bbox columns
                        (no confidence, no rawdefectclass)

Run once:
    python seed_compare.py
Then:
    streamlit run compare_app.py
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
CLASSES = ["crack", "scratch", "inclusion", "scale"]
RAW_CLASSES = CLASSES + ["unknown"]

COIL_SPECS = [
    # (coil_id, n_rows, seed_offset, hours_ago)
    ("COIL_001", 80,  10, 50),
    ("COIL_002", 55,  20, 48),
    ("COIL_003", 30,  30, 36),
    ("COIL_004", 100, 40, 2),
    ("COIL_005", 45,  50, 1),
    ("COIL_006", 70,  60, 0.25),
]


def _make_coil_df(coil_id: str, n_rows: int, seed: int,
                  class_weights: list[float] | None = None,
                  change_rate: float = 0.3,
                  conf_low: float = 0.1, conf_high: float = 1.0) -> pd.DataFrame:
    """Generate a synthetic coil DataFrame.

    Parameters allow tweaking distributions per environment so test vs prod
    data look realistically different.
    """
    rng = np.random.default_rng(seed)

    if class_weights is None:
        class_weights = [0.25, 0.25, 0.25, 0.25]
    probs = np.array(class_weights) / np.sum(class_weights)

    defectclass = rng.choice(CLASSES, size=n_rows, p=probs)
    rawdefectclass = defectclass.copy()
    mask = rng.random(n_rows) < change_rate
    rawdefectclass[mask] = rng.choice(RAW_CLASSES, size=mask.sum())

    return pd.DataFrame({
        "coilid": coil_id,
        "defectclass": defectclass,
        "rawdefectclass": rawdefectclass,
        "bbox_xtl": rng.uniform(0, 800, n_rows),
        "bbox_ytl": rng.uniform(0, 600, n_rows),
        "bbox_xbr": rng.uniform(50, 850, n_rows),
        "bbox_ybr": rng.uniform(50, 650, n_rows),
        "confidence": rng.uniform(conf_low, conf_high, n_rows),
    })


def _seed_db(label: str, seed_base: int,
             class_weights: list[float], change_rate: float,
             conf_low: float, conf_high: float):
    """Generate and save parquet data for one DB label."""
    db_dir = Path(STORAGE_DIR) / label
    if db_dir.exists():
        shutil.rmtree(db_dir)

    now = datetime.now(timezone.utc)

    for coil_id, n_rows, seed_offset, hours_ago in COIL_SPECS:
        seed = seed_base + seed_offset
        df = _make_coil_df(coil_id, n_rows, seed,
                           class_weights=class_weights,
                           change_rate=change_rate,
                           conf_low=conf_low, conf_high=conf_high)
        stats = compute_coil_stats(coil_id, df)
        stats["fetched_at"] = now - timedelta(hours=hours_ago)
        save_coil_stats(STORAGE_DIR, label, stats)
        print(f"  [{label}] {coil_id}: {n_rows} defects")


def _make_training_csv(path: str, n_rows: int = 500, seed: int = 999):
    """Generate a training CSV with instance_label + bbox (no confidence)."""
    rng = np.random.default_rng(seed)
    # training data has a slightly different class distribution
    weights = [0.35, 0.30, 0.20, 0.15]
    probs = np.array(weights) / np.sum(weights)

    df = pd.DataFrame({
        "instance_label": rng.choice(CLASSES, size=n_rows, p=probs),
        "bbox_xtl": rng.uniform(0, 800, n_rows).round(1),
        "bbox_ytl": rng.uniform(0, 600, n_rows).round(1),
        "bbox_xbr": rng.uniform(50, 850, n_rows).round(1),
        "bbox_ybr": rng.uniform(50, 650, n_rows).round(1),
    })
    df.to_csv(path, index=False)
    print(f"  CSV: {n_rows} rows -> {path}")


def main():
    print("Seeding test DB data...")
    _seed_db(
        label="test", seed_base=100,
        class_weights=[0.30, 0.25, 0.25, 0.20],  # more cracks
        change_rate=0.25,
        conf_low=0.2, conf_high=1.0,
    )

    print("Seeding prod DB data...")
    _seed_db(
        label="prod", seed_base=200,
        class_weights=[0.20, 0.30, 0.30, 0.20],  # more scratches/inclusions
        change_rate=0.35,
        conf_low=0.1, conf_high=0.95,
    )

    print("Generating training CSV...")
    _make_training_csv("training_data.csv")

    print(f"\nDone. Storage: {Path(STORAGE_DIR).resolve()}")
    print("Run:  streamlit run compare_app.py")


if __name__ == "__main__":
    main()
