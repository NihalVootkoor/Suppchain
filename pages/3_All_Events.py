"""All Events page — filterable table with quick-filters and CSV export."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from src.config import get_config
from src.date_utils import parse_datetime
from src.ui_utils import (
    load_events,
    render_debug_panel,
    render_events_table,
    render_sidebar,
)


def main() -> None:
    """Render the All Events page."""
    config = get_config()
    st.title("All Events")
    st.markdown("**Event table:** Sort, filter, and paginate. Click a title to open the article.")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return

    # ── Quick-filter chips ────────────────────────────────────────────────────
    col_new, col_high, col_export, _ = st.columns([1, 1, 1, 4])
    with col_new:
        new_this_week = st.checkbox("New This Week", value=False)
    with col_high:
        high_only = st.checkbox("High Severity", value=False)

    # Apply quick filters
    display_events = filtered
    if high_only:
        display_events = [e for e in display_events if e.get("severity_band") == "High"]
    if new_this_week:
        week_ago = date.today() - timedelta(days=7)
        display_events = [
            e for e in display_events
            if parse_datetime(str(e.get("published_at", ""))).date() >= week_ago
        ]

    # ── CSV export ────────────────────────────────────────────────────────────
    if display_events:
        export_rows = []
        for e in display_events:
            export_rows.append({
                "title": e.get("title", ""),
                "risk_category": e.get("risk_category", ""),
                "disruption_type": e.get("disruption_type", ""),
                "geo_region": e.get("geo_region", ""),
                "geo_country": e.get("geo_country", ""),
                "risk_score": round(float(e.get("risk_score_0to100") or 0), 1),
                "severity_band": e.get("severity_band", ""),
                "exposure_usd": round(float(e.get("exposure_usd_est") or 0), 0),
                "delay_days": int(e.get("estimated_delay_days") or 0),
                "published_at": str(e.get("published_at", "")),
                "article_url": e.get("article_url", ""),
            })
        csv_bytes = pd.DataFrame(export_rows).to_csv(index=False).encode("utf-8")
        with col_export:
            st.download_button(
                label="Export CSV",
                data=csv_bytes,
                file_name=f"supply_chain_events_{date.today()}.csv",
                mime="text/csv",
            )

    render_events_table(display_events)


if __name__ == "__main__":
    main()
