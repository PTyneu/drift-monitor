"""Coil watcher — detects new coils and triggers statistics computation.

Polls the DB every ``poll_interval_sec`` seconds.  Keeps the last-seen coil ID
in memory as a watermark so the SQL query is always a cheap
``WHERE coilid > :watermark``.  On first launch the watermark is restored from
``processed_coils.txt`` (survives restarts).
"""

from __future__ import annotations

import logging
import threading

from .config import AppConfig
from .db import fetch_new_coils, fetch_coil_data
from .stats import compute_coil_stats
from .storage import save_coil_stats, load_last_processed_coil

log = logging.getLogger(__name__)

# Global lock prevents concurrent processing of the same coil when the
# background watcher and the manual "Run check now" button overlap.
_process_lock = threading.Lock()


class CoilWatcher:
    """Continuously watches for new coils and computes drift statistics."""

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self._stop_event = threading.Event()
        # In-memory watermark: last processed coil ID.
        # Initialised lazily on first run_once() from processed_coils.txt,
        # then updated in memory — no file I/O on every polling cycle.
        self._watermark: str | None = None
        self._watermark_loaded = False

    # ── public API ──────────────────────────────────────────────

    def run_once(self) -> list[str]:
        """Check for unprocessed coils and compute stats.  Return new coil IDs."""
        with _process_lock:
            # Lazy init: read watermark from disk only on the very first call.
            if not self._watermark_loaded:
                self._watermark = load_last_processed_coil(self.cfg.storage.dir)
                self._watermark_loaded = True
                log.info("Watermark initialised: %s", self._watermark)

            try:
                new_coils = fetch_new_coils(self.cfg.db, after=self._watermark)
            except Exception:
                log.exception("Failed to fetch coil list from DB")
                return []

            if not new_coils:
                log.debug("No new coils since %s", self._watermark)
                return []

            log.info("New coils detected: %s", new_coils)
            for coil_id in new_coils:
                self._process_coil(coil_id)
            return new_coils

    def start(self) -> threading.Thread:
        """Start the watcher loop in a daemon thread."""
        t = threading.Thread(target=self._loop, daemon=True, name="coil-watcher")
        t.start()
        log.info(
            "Watcher started (poll every %d s)", self.cfg.watcher.poll_interval_sec,
        )
        return t

    def stop(self) -> None:
        self._stop_event.set()

    # ── internals ───────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(timeout=self.cfg.watcher.poll_interval_sec)

    def _process_coil(self, coil_id: str) -> None:
        log.info("Processing coil %s …", coil_id)
        try:
            df = fetch_coil_data(self.cfg.db, coil_id)
        except Exception:
            log.exception("Failed to fetch data for coil %s", coil_id)
            return

        if df.empty:
            log.warning("Coil %s has no defect rows, skipping", coil_id)
            return

        stats = compute_coil_stats(coil_id, df)
        save_coil_stats(self.cfg.storage.dir, stats)
        # Advance the in-memory watermark — next cycle will only ask for
        # coils newer than this one.
        self._watermark = coil_id
        log.info("Coil %s done (%d defects)", coil_id, stats["total_defects"])
