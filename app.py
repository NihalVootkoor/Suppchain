"""Streamlit entrypoint for Auto Supply Chain Risk Monitor."""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Auto Supply Chain Risk Monitor",
    layout="wide",
)

try:
    from src.ui_utils import inject_full_width_css
    inject_full_width_css()
except Exception:
    pass

try:
    from src.config import get_config
    from src.ui_utils import render_groq_status

    config = get_config()
    db_label = "Supabase" if config.db_url else "Local (SQLite)"
    st.info(f"**Database:** {db_label}")
    render_groq_status()
except Exception as e:
    st.warning(f"Config/status: {e}")

st.title("Automotive Supply Chain Risk Monitor")
st.markdown(
    "Use the navigation sidebar to explore Command Center, Risk Radar, "
    "Trends, and Mitigation Explorer."
)
