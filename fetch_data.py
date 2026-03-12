"""Fetch real data from configured PostgreSQL databases and save to storage.

Reads config.yaml, connects to each database (test/prod), fetches coil data
for a given date range, computes statistics, and persists them as parquets.

Usage:
    python fetch_data.py                          # last 7 days, all DBs
    python fetch_data.py --days 30                # last 30 days
    python fetch_data.py --label prod             # only prod DB
    python fetch_data.py --clear                  # wipe storage/ first
    python fetch_data.py --config other.yaml      # custom config path
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from drift.config import load_config
from drift.db import fetch_coils_in_range, fetch_coil_data, open_connection
from drift.stats import compute_coil_stats
from drift.storage import save_coil_stats, load_last_processed_coil


def fetch_db(cfg_db, storage_dir: str, date_from: date, date_to: date) -> None:
    label = cfg_db.label
    print(f"\n[{label}] Connecting to {cfg_db.host}:{cfg_db.port}/{cfg_db.dbname} ...")

    with open_connection(cfg_db) as conn:
        coils = fetch_coils_in_range(cfg_db, date_from, date_to, conn=conn)
        print(f"[{label}] Found {len(coils)} coils in range {date_from} .. {date_to}")

        for i, coil_id in enumerate(coils, 1):
            df = fetch_coil_data(cfg_db, coil_id, conn=conn)
            if df.empty:
                print(f"  [{i}/{len(coils)}] {coil_id}: no rows, skipping")
                continue
            stats = compute_coil_stats(coil_id, df)
            save_coil_stats(storage_dir, label, stats)
            print(f"  [{i}/{len(coils)}] {coil_id}: {len(df)} defects saved")

    print(f"[{label}] Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch data from DBs into storage/")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--days", type=int, default=7, help="Fetch last N days (default 7)")
    parser.add_argument("--label", help="Process only this DB label (e.g. 'prod')")
    parser.add_argument("--clear", action="store_true", help="Remove existing storage/ before fetching")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if not cfg.databases:
        print("No databases configured in", args.config)
        sys.exit(1)

    storage_dir = cfg.storage.dir
    if args.clear:
        p = Path(storage_dir)
        if p.exists():
            shutil.rmtree(p)
            print(f"Cleared {p.resolve()}")

    date_to = date.today()
    date_from = date_to - timedelta(days=args.days)

    for db_cfg in cfg.databases:
        if args.label and db_cfg.label != args.label:
            continue
        try:
            fetch_db(db_cfg, storage_dir, date_from, date_to)
        except Exception as e:
            print(f"[{db_cfg.label}] ERROR: {e}")


if __name__ == "__main__":
    main()
