"""Standalone launcher for the comparison page.

Run:
    streamlit run compare_app.py
"""

import streamlit as st

from drift.config import load_config
from drift.compare_page import render_compare_tab

st.set_page_config(page_title="Drift Compare", layout="wide")
st.title("Сравнение моделей")

cfg = load_config(st.sidebar.text_input("path_to_config.yaml", value="config.yaml"))
render_compare_tab(cfg)
