"""Risk Radar page."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.aggregation import category_breakdown
from src.config import get_config
from src.ui_utils import load_events, render_debug_panel, render_sidebar


def region_metrics(events: list[dict[str, object]], metric: str) -> list[dict[str, object]]:
    """Aggregate region metrics."""

    totals: dict[str, list[float]] = {}
    for event in events:
        region = str(event["geo_region"])
        totals.setdefault(region, []).append(float(event["risk_score_0to100"]))
    if metric == "count":
        return [{"region": region, "value": len(values)} for region, values in totals.items()]
    if metric == "avg_severity":
        return [
            {"region": region, "value": round(sum(values) / len(values), 2)}
            for region, values in totals.items()
        ]
    exposure: dict[str, float] = {}
    for event in events:
        region = str(event["geo_region"])
        exposure[region] = exposure.get(region, 0.0) + float(event["exposure_usd_est"])
    return [{"region": region, "value": round(value, 2)} for region, value in exposure.items()]


def main() -> None:
    """Render the Risk Radar page."""

    st.title("Risk Radar")
    config = get_config()
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return
    metric = st.selectbox("Heat map metric", ["count", "avg_severity", "exposure_usd"])
    region_data = region_metrics(filtered, metric)
    st.subheader("Regional Summary")
    if region_data:
        df = pd.DataFrame(region_data).set_index("region")
        st.bar_chart(df)
    else:
        st.info("No regional data available.")
    st.subheader("PESTEL Category Breakdown")
    st.dataframe(category_breakdown(filtered), use_container_width=True)


if __name__ == "__main__":
    main()
