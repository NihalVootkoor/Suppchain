"""Command Center view logic (shared by main app and sidebar page)."""

from __future__ import annotations

import streamlit as st

from src.aggregation import compute_kpis
from src.config import get_config
from src.ui_utils import (
    load_events,
    render_debug_panel,
    render_events_table,
    render_groq_status,
    render_sidebar,
)


def render_command_center() -> None:
    """Render the Command Center page (KPIs, top events, table)."""
    config = get_config()
    st.info(f"**Database:** {'Supabase' if config.db_url else 'Local (SQLite)'}")
    render_groq_status()
    st.title("Command Center")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return
    kpis = compute_kpis(filtered)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Active Risk Events", kpis.total_events)
    col2.metric("High/Critical Events", kpis.high_critical_events)
    col3.metric("Avg Severity Today", kpis.avg_severity_today)
    col4, col5, col6 = st.columns(3)
    col4.metric("Delta vs Yesterday Avg Severity", kpis.delta_vs_yesterday)
    col5.metric("Avg Estimated Delay (days)", kpis.avg_delay_days)
    col6.metric("Total $ Exposure at Risk (Estimated)", kpis.total_exposure_usd)
    top_events = sorted(
        filtered,
        key=lambda item: (
            float(item["risk_score_0to100"]),
            float(item["exposure_usd_est"]),
            str(item["published_at"]),
        ),
        reverse=True,
    )[:3]
    st.subheader("Top 3 Current High-Risk Events")
    st.caption(
        "Ranked by risk score, estimated exposure, and recency. "
        "Mitigation is personalized when Groq LLM is configured."
    )
    for event in top_events:
        st.markdown(f"### [{event['title']}]({event['article_url']})")
        st.write(event["event_summary"])
        st.write(f"**Why this is a risk:** {event['reason_flagged']}")
        blurb = event.get("dashboard_blurb")
        if blurb:
            st.write(f"**Risks identified:** {blurb}")
        actions = event.get("mitigation_actions") or []
        if actions:
            st.write("**Mitigation actions:**")
            for action in actions:
                st.write(f"- {action}")
        if event.get("mitigation_description"):
            st.caption(event["mitigation_description"])
    st.subheader("All Events")
    st.caption(
        "Sort, filter, and paginate. AG Grid when available; "
        "otherwise Streamlit native table."
    )
    render_events_table(filtered, use_aggrid=True, height=420, selection_mode="single")
