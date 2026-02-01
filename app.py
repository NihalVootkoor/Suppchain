"""Streamlit entrypoint for Auto Supply Chain Risk Monitor."""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Auto Supply Chain Risk Monitor",
    layout="wide",
)

st.title("Automotive Supply Chain Risk Monitor")
st.markdown(
    "Use the navigation sidebar to explore Command Center, Risk Radar, "
    "Trends, and Mitigation Explorer."
)
