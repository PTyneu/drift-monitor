"""Coil watcher — detects new coils and triggers statistics computation.

Supports two modes controlled by ``AppConfig.live``:

* **Live** — polls all configured databases on a timer.  Keeps an in-memory
  watermark per DB so the SQL query is always ``WHERE coilid > :wm``.
* **Manual** — no background thread.  Call ``run_manual()`` with a date range
  to fetch and process coils on demand.
"""

from __future__ import annotations

import logging
import threading
from datetime import date

from .config import AppConfig, DbConfig
from .db import fetch_new_coils, fetch_coils_in_range, fetch_coil_data
from .stats import compute_coil_stats
from .storage import save_coil_stats, load_last_processed_coil

log = logging.getLogger(__name__)

_process_lock = threading.Lock()


class CoilWatcher:
    """Watches for new coils across one or more databases."""

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self._stop_event = threading.Event()
        # Per-DB watermarks: {db_label: last_coil_id}
        self._watermarks: dict[str, str | None] = {}
        self._watermarks_loaded: set[str] = set()

    # ── Live mode ───────────────────────────────────────────────

    def run_once(self) -> dict[str, list[str]]:
        """Poll all databases for new coils.  Returns {db_label: [new coil IDs]}."""
        result: dict[str, list[str]] = {}
        with _process_lock:
            for db_cfg in self.cfg.databases:
                new = self._poll_db(db_cfg)
                if new:
                    result[db_cfg.label] = new
        return result

    def start(self) -> threading.Thread:
        """Start the live polling loop in a daemon thread."""
        t = threading.Thread(target=self._loop, daemon=True, name="coil-watcher")
        t.start()
        labels = [d.label for d in self.cfg.databases]
        log.info(
            "Watcher started: databases=%s, poll every %d s",
            labels, self.cfg.watcher.poll_interval_sec,
        )
        return t

    def stop(self) -> None:
        self._stop_event.set()

    # ── Manual mode ─────────────────────────────────────────────

    def run_manual(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        db_labels: list[str] | None = None,
    ) -> dict[str, list[str]]:
        """Fetch and process coils within a date range (manual mode).

        Args:
            date_from: start date (inclusive).  None → today - 7 days.
            date_to:   end date (inclusive).  None → today.
            db_labels: which databases to query.  None → all.

        Returns:
            {db_label: [processed coil IDs]}
        """
        targets = self.cfg.databases
        if db_labels:
            label_set = set(db_labels)
            targets = [d for d in targets if d.label in label_set]

        result: dict[str, list[str]] = {}
        with _process_lock:
            for db_cfg in targets:
                processed = self._query_range(db_cfg, date_from, date_to)
                if processed:
                    result[db_cfg.label] = processed
        return result

    # ── internals ───────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(timeout=self.cfg.watcher.poll_interval_sec)

    def _poll_db(self, db_cfg: DbConfig) -> list[str]:
        """Live-mode poll for a single database."""
        label = db_cfg.label

        # Lazy watermark init
        if label not in self._watermarks_loaded:
            self._watermarks[label] = load_last_processed_coil(
                self.cfg.storage.dir, label,
            )
            self._watermarks_loaded.add(label)
            log.info("[%s] Watermark initialised: %s", label, self._watermarks[label])

        try:
            new_coils = fetch_new_coils(db_cfg, after=self._watermarks[label])
        except Exception:
            log.exception("[%s] Failed to fetch new coils", label)
            return []

        if not new_coils:
            log.debug("[%s] No new coils since %s", label, self._watermarks[label])
            return []

        log.info("[%s] New coils: %s", label, new_coils)
        for coil_id in new_coils:
            self._process_coil(db_cfg, coil_id)
        return new_coils

    def _query_range(
        self, db_cfg: DbConfig, date_from: date | None, date_to: date | None,
    ) -> list[str]:
        """Manual-mode query for a single database."""
        label = db_cfg.label
        try:
            coils = fetch_coils_in_range(db_cfg, date_from, date_to)
        except Exception:
            log.exception("[%s] Failed to fetch coils in range", label)
            return []

        if not coils:
            log.info("[%s] No coils found in range %s – %s", label, date_from, date_to)
            return []

        log.info("[%s] Found %d coils in range, processing…", label, len(coils))
        for coil_id in coils:
            self._process_coil(db_cfg, coil_id)
        return coils

    def _process_coil(self, db_cfg: DbConfig, coil_id: str) -> None:
        label = db_cfg.label
        log.info("[%s] Processing coil %s…", label, coil_id)
        try:
            df = fetch_coil_data(db_cfg, coil_id)
        except Exception:
            log.exception("[%s] Failed to fetch data for coil %s", label, coil_id)
            return

        if df.empty:
            log.warning("[%s] Coil %s has no rows, skipping", label, coil_id)
            return

        stats = compute_coil_stats(coil_id, df)
        save_coil_stats(self.cfg.storage.dir, label, stats)
        self._watermarks[label] = coil_id
        log.info("[%s] Coil %s done (%d defects)", label, coil_id, stats["total_defects"])
