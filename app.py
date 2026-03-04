"""Streamlit entry point for Drift Monitor.

Run:
    streamlit run app.py
"""

import pandas as pd
import streamlit as st

from drift.config import load_config
from drift.watcher import CoilWatcher
from drift.storage import load_processed_coils, load_all_summaries

# ── config ──────────────────────────────────────────────────────
# Config path can be set via sidebar or DRIFT_CONFIG env var.

st.set_page_config(page_title="Drift Monitor", layout="wide")
st.title("Drift Monitor")

with st.sidebar:
    st.header("Configuration")
    config_path = st.text_input("Path to config.yaml", value="config.yaml")
    cfg = load_config(config_path)

    st.markdown("---")
    st.markdown(f"**DB table:** `{cfg.db.table}`")
    st.markdown(f"**Poll interval:** {cfg.watcher.poll_interval_sec} s")
    st.markdown(f"**Storage dir:** `{cfg.storage.dir}`")

# ── watcher control ─────────────────────────────────────────────

_WATCHER_KEY = "drift_watcher_running"

if _WATCHER_KEY not in st.session_state:
    st.session_state[_WATCHER_KEY] = False
    st.session_state["_watcher_instance"] = None


col1, col2 = st.columns(2)

with col1:
    if st.button("Run check now"):
        watcher = CoilWatcher(cfg)
        new = watcher.run_once()
        if new:
            st.success(f"Processed coils: {', '.join(str(c) for c in new)}")
        else:
            st.info("No new coils detected")

with col2:
    if not st.session_state[_WATCHER_KEY]:
        if st.button("Start background watcher"):
            watcher = CoilWatcher(cfg)
            watcher.start()
            st.session_state[_WATCHER_KEY] = True
            st.session_state["_watcher_instance"] = watcher
            st.success("Background watcher started")
    else:
        if st.button("Stop background watcher"):
            inst = st.session_state.get("_watcher_instance")
            if inst:
                inst.stop()
            st.session_state[_WATCHER_KEY] = False
            st.info("Watcher stopped")

# ── overview ────────────────────────────────────────────────────

st.markdown("---")
st.subheader("Processed coils")

processed = load_processed_coils(cfg.storage.dir)
st.write(f"**Total processed:** {len(processed)}")

summaries = load_all_summaries(cfg.storage.dir)
if not summaries.empty:
    # ── date range filter ───────────────────────────────────────
    if "fetched_at" in summaries.columns:
        summaries["fetched_at"] = pd.to_datetime(summaries["fetched_at"], utc=True)
        min_date = summaries["fetched_at"].min().date()
        max_date = summaries["fetched_at"].max().date()
        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        # st.date_input returns a single date or incomplete tuple while user
        # is still picking the second date — only filter when both are set.
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            date_from, date_to = date_range
            mask = (
                (summaries["fetched_at"].dt.date >= date_from)
                & (summaries["fetched_at"].dt.date <= date_to)
            )
            summaries = summaries.loc[mask]

    st.dataframe(summaries, use_container_width=True)
else:
    st.info("No statistics computed yet. Click 'Run check now' or start the watcher.")
