"""Mitigation Explorer page."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.config import get_config
from src.ui_utils import load_events, render_debug_panel, render_sidebar


def main() -> None:
    """Render the Mitigation Explorer page."""

    st.title("Mitigation Explorer")
    config = get_config()
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return
    df = pd.DataFrame(filtered)
    st.dataframe(df, width="stretch")


if __name__ == "__main__":
    main()
