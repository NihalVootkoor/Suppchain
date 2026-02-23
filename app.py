"""Streamlit entrypoint for Auto Supply Chain Risk Monitor."""

from __future__ import annotations

import streamlit as st

from src.config import get_config

st.set_page_config(
    page_title="Auto Supply Chain Risk Monitor",
    layout="wide",
)

config = get_config()
db_label = "Supabase" if config.db_url else "Local (SQLite)"
st.info(f"**Database:** {db_label}")

st.title("Automotive Supply Chain Risk Monitor")
st.markdown(
    "Use the navigation sidebar to explore Command Center, Risk Radar, "
    "Trends, and Mitigation Explorer."
)
