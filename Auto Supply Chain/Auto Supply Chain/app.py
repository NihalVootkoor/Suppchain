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

nav = st.navigation([
    st.Page(render_command_center, title="Command Center", default=True),
    st.Page("pages/4_Mitigation_Explorer.py", title="All Events"),
])
nav.run()
