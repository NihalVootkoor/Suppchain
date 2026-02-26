"""Mitigation Explorer page."""
from __future__ import annotations

import streamlit as st

from src.config import get_config
from src.ui_utils import (
    load_events,
    render_debug_panel,
    render_events_table,
    render_groq_status,
    render_sidebar,
)


def main() -> None:
    """Render the Mitigation Explorer page."""
    config = get_config()
    render_groq_status()
    st.title("Mitigation Explorer")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return
    st.caption("Professional event table: sort, filter, paginate, row selection. AG Grid when available.")
    render_events_table(filtered, use_aggrid=True, height=500, selection_mode="multiple")


if __name__ == "__main__":
    main()
