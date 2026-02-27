"""Streamlit entrypoint for Auto Supply Chain Risk Monitor."""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Command Center",
    layout="wide",
)

try:
    from src.ui_utils import inject_full_width_css
    inject_full_width_css()
except Exception:
    pass

from src.command_center import render_command_center

# Single nav: Command Center + other pages (no duplicate "app" list)
nav = st.navigation([
    st.Page(render_command_center, title="Command Center", default=True),
    st.Page("pages/2_Risk_Radar.py", title="Risk Radar"),
    st.Page("pages/3_Trends.py", title="Trends"),
    st.Page("pages/4_Mitigation_Explorer.py", title="Mitigation Explorer"),
])
nav.run()
