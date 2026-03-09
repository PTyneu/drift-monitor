"""Compute drift statistics directly from the database (no parquet intermediate).

Fetches coils in a date range, runs compute_coil_stats on each, and returns
the same DataFrame structures that the storage loaders produce — so the drift
tab can consume them interchangeably.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd

from .config import AppConfig
from .db import open_connection, fetch_coils_in_range, fetch_coil_data
from .stats import compute_coil_stats, stats_to_frames

log = logging.getLogger(__name__)


def compute_drift_from_db(
    cfg: AppConfig,
    date_from: date | datetime | None = None,
    date_to: date | datetime | None = None,
    db_labels: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Query DB directly and return DataFrames matching storage loader format.

    Returns dict with keys: summaries, confidence, conf_buckets, class_change_top.
    Uses a single connection per database for efficiency.
    """
    targets = cfg.databases
    if db_labels:
        label_set = set(db_labels)
        targets = [d for d in targets if d.label in label_set]

    summaries_frames = []
    conf_frames = []
    bucket_frames = []
    change_frames = []

    for db_cfg in targets:
        label = db_cfg.label
        try:
            with open_connection(db_cfg) as conn:
                coil_ids = fetch_coils_in_range(db_cfg, date_from, date_to, conn=conn)

                if not coil_ids:
                    log.info("[%s] No coils in range %s – %s", label, date_from, date_to)
                    continue

                log.info("[%s] Processing %d coils from DB...", label, len(coil_ids))
                for coil_id in coil_ids:
                    try:
                        df = fetch_coil_data(db_cfg, coil_id, conn=conn)
                    except Exception:
                        log.exception("[%s] Failed to fetch coil %s", label, coil_id)
                        continue
                    if df.empty:
                        continue

                    stats = compute_coil_stats(coil_id, df)
                    frames = stats_to_frames(stats, label)

                    summaries_frames.append(frames["summary"])
                    if not frames["confidence"].empty:
                        conf_frames.append(frames["confidence"])
                    if not frames["conf_buckets"].empty:
                        bucket_frames.append(frames["conf_buckets"])
                    if not frames["class_change_top"].empty:
                        change_frames.append(frames["class_change_top"])

        except Exception:
            log.exception("[%s] Failed to connect to DB", label)
            continue

    return {
        "summaries": pd.concat(summaries_frames, ignore_index=True) if summaries_frames else pd.DataFrame(),
        "confidence": pd.concat(conf_frames, ignore_index=False) if conf_frames else pd.DataFrame(),
        "conf_buckets": pd.concat(bucket_frames, ignore_index=True) if bucket_frames else pd.DataFrame(),
        "class_change_top": pd.concat(change_frames, ignore_index=True) if change_frames else pd.DataFrame(),
    }
