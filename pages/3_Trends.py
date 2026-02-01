"""Trends page."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.config import get_config
from src.ui_utils import load_events, render_debug_panel, render_sidebar


def main() -> None:
    """Render the Trends page."""

    st.title("Trends")
    config = get_config()
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return
    df = pd.DataFrame(filtered)
    df["published_date"] = pd.to_datetime(df["published_at"]).dt.date
    severity = df.groupby("published_date")["risk_score_0to100"].mean().reset_index()
    volume = df.groupby("published_date")["event_id"].count().reset_index()
    st.subheader("Risk Severity Over Time")
    st.line_chart(severity, x="published_date", y="risk_score_0to100")
    st.subheader("Event Volume Over Time")
    st.line_chart(volume, x="published_date", y="event_id")


if __name__ == "__main__":
    main()
