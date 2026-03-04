"""Streamlit entry point for Drift Monitor.

Run:
    streamlit run app.py
"""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from drift.config import load_config
from drift.watcher import CoilWatcher
from drift.storage import load_all_summaries

# ── config ──────────────────────────────────────────────────────

st.set_page_config(page_title="Drift Monitor", layout="wide")
st.title("Drift Monitor")

with st.sidebar:
    st.header("Configuration")
    config_path = st.text_input("Path to config.yaml", value="config.yaml")
    cfg = load_config(config_path)

    st.markdown("---")
    mode_label = "Live (auto)" if cfg.live else "Manual (on demand)"
    st.markdown(f"**Mode:** `{mode_label}`")
    st.markdown(f"**Databases:** {', '.join(d.label for d in cfg.databases)}")
    if cfg.live:
        st.markdown(f"**Poll interval:** {cfg.watcher.poll_interval_sec} s")
    st.markdown(f"**Storage dir:** `{cfg.storage.dir}`")

# ── watcher instance (shared via session_state) ────────────────

if "watcher" not in st.session_state:
    st.session_state["watcher"] = CoilWatcher(cfg)
    st.session_state["watcher_running"] = False

watcher: CoilWatcher = st.session_state["watcher"]

# ── mode-specific controls ─────────────────────────────────────

st.markdown("---")

if cfg.live:
    # ── LIVE MODE ───────────────────────────────────────────────
    st.subheader("Live mode")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Run check now"):
            result = watcher.run_once()
            if result:
                for db_label, coils in result.items():
                    st.success(f"**{db_label}**: {', '.join(str(c) for c in coils)}")
            else:
                st.info("No new coils detected")

    with col2:
        if not st.session_state["watcher_running"]:
            if st.button("Start background watcher"):
                watcher.start()
                st.session_state["watcher_running"] = True
                st.success("Background watcher started")
        else:
            if st.button("Stop background watcher"):
                watcher.stop()
                st.session_state["watcher_running"] = False
                # Create fresh watcher for next start
                st.session_state["watcher"] = CoilWatcher(cfg)
                st.info("Watcher stopped")

else:
    # ── MANUAL MODE ─────────────────────────────────────────────
    st.subheader("Manual mode")

    # Date range
    default_from = date.today() - timedelta(days=7)
    default_to = date.today()

    col_from, col_to = st.columns(2)
    with col_from:
        d_from = st.date_input("From", value=default_from)
    with col_to:
        d_to = st.date_input("To", value=default_to)

    # DB selector
    all_labels = [d.label for d in cfg.databases]
    if len(all_labels) > 1:
        selected_dbs = st.multiselect(
            "Databases to query",
            options=all_labels,
            default=all_labels,
        )
    else:
        selected_dbs = all_labels

    # Run button
    if st.button("Run query", type="primary"):
        with st.spinner("Querying databases…"):
            result = watcher.run_manual(
                date_from=d_from,
                date_to=d_to,
                db_labels=selected_dbs,
            )
        if result:
            for db_label, coils in result.items():
                st.success(f"**{db_label}**: processed {len(coils)} coils")
        else:
            st.info("No coils found in the selected range")

# ── results ─────────────────────────────────────────────────────

st.markdown("---")
st.subheader("Results")

db_labels = [d.label for d in cfg.databases]
summaries = load_all_summaries(cfg.storage.dir, db_labels)

if not summaries.empty:
    # Date range filter on stored results
    if "fetched_at" in summaries.columns:
        summaries["fetched_at"] = pd.to_datetime(summaries["fetched_at"], utc=True)
        min_date = summaries["fetched_at"].min().date()
        max_date = summaries["fetched_at"].max().date()
        date_range = st.date_input(
            "Filter results by date",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="results_date_filter",
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            f_from, f_to = date_range
            mask = (
                (summaries["fetched_at"].dt.date >= f_from)
                & (summaries["fetched_at"].dt.date <= f_to)
            )
            summaries = summaries.loc[mask]

    # DB filter (if multiple)
    if "db_label" in summaries.columns and summaries["db_label"].nunique() > 1:
        selected = st.multiselect(
            "Filter by database",
            options=sorted(summaries["db_label"].unique()),
            default=sorted(summaries["db_label"].unique()),
            key="results_db_filter",
        )
        summaries = summaries[summaries["db_label"].isin(selected)]

    st.dataframe(summaries, use_container_width=True)
else:
    action = "start the watcher" if cfg.live else "click 'Run query'"
    st.info(f"No statistics computed yet. {action.capitalize()} to begin.")
