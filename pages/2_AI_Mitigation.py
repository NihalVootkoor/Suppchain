"""AI-Powered Mitigation — Top 3 high-risk events with Groq mitigation guidance."""

from __future__ import annotations

import streamlit as st

from src.command_center import _render_top_event_card
from src.config import get_config
from src.ui_utils import load_events, render_sidebar, render_debug_panel


def render_ai_mitigation() -> None:
    config = get_config()
    st.title("AI-Powered Mitigation")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return

    top_events = sorted(
        filtered,
        key=lambda item: (
            float(item["risk_score_0to100"]),
            float(item["exposure_usd_est"]),
            str(item["published_at"]),
        ),
        reverse=True,
    )[:3]

    st.markdown(
        "<p style='font-size:1.3rem;font-weight:600;margin-bottom:0;'>Top 3 High-Risk Events</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Ranked by risk score and estimated exposure. "
        "Click any article title to read the source. "
        "Mitigation guidance is AI-powered via Groq LLM when configured."
    )
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    for rank, event in enumerate(top_events, 1):
        _render_top_event_card(rank, event, config)


render_ai_mitigation()
