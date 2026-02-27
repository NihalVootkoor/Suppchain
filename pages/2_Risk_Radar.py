"""Risk Radar page."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from src.aggregation import category_breakdown
from src.config import get_config
from src.geo_utils import get_event_coordinates
from src.ui_utils import inject_full_width_css, load_events, render_debug_panel, render_groq_status, render_sidebar


def _render_pestel_bar_chart(events: list[dict]) -> None:
    """PESTEL category breakdown as an interactive Plotly bar chart."""
    counts = category_breakdown(events)
    if not counts:
        st.info("No PESTEL category data available.")
        return
    df = pd.DataFrame([{"category": k, "count": v} for k, v in counts.items()])
    df = df.sort_values("count", ascending=True)
    fig = go.Figure(
        data=[
            go.Bar(
                x=df["count"],
                y=df["category"],
                orientation="h",
                marker=dict(
                    color=df["count"],
                    colorscale="Reds",
                    showscale=True,
                    colorbar=dict(title="Count"),
                ),
                text=df["count"],
                textposition="outside",
                texttemplate="%{text}",
                hovertemplate="%{y}<br>Count: %{x}<extra></extra>",
            )
        ],
        layout=go.Layout(
            title=None,
            xaxis=dict(title="Number of events", gridcolor="rgba(128,128,128,0.2)"),
            yaxis=dict(
                title="",
                automargin=True,
                tickfont=dict(size=16),
            ),
            margin=dict(l=20, r=80),
            height=460,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        ),
    )
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        hovermode="y unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})


def _render_world_risk_map(events: list[dict], layer_type: str) -> None:
    """Interactive world risk map with Pydeck (Scatterplot, Heatmap, or Hexagon)."""
    if not events:
        return
    # Build list of plain dicts with native Python types (Pydeck JSON serialization)
    import random
    map_data = []
    for e in events:
        lat, lon = get_event_coordinates(e)
        jitter = 0.3
        lat = float(lat + random.uniform(-jitter, jitter))
        lon = float(lon + random.uniform(-jitter, jitter))
        score = float(e.get("risk_score_0to100") or 0)
        exposure = float(e.get("exposure_usd_est") or 0)
        r_val = min(255, int(score * 2.55))
        g_val = max(0, 255 - r_val)
        map_data.append({
            "lat": lat,
            "lon": lon,
            "risk_score": score,
            "exposure_usd": round(exposure, 0),
            "radius": float(80000 + score * 3000),
            "title": str(e.get("title") or "")[:60],
            "region": str(e.get("geo_region") or ""),
            "color": [r_val, g_val, 0],
        })

    if layer_type == "scatter":
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_data,
            get_position=["lon", "lat"],
            get_radius="radius",
            get_fill_color="color",
            get_line_color=[0, 0, 0],
            line_width_min_pixels=1,
            pickable=True,
            opacity=0.7,
        )
    else:
        layer = pdk.Layer(
            "HeatmapLayer",
            data=map_data,
            get_position=["lon", "lat"],
            get_weight="risk_score",
            radius_pixels=40,
            intensity=1,
            threshold=0.05,
            pickable=True,
        )

    view_state = pdk.ViewState(
        latitude=25.0,
        longitude=20.0,
        zoom=1.5,
        pitch=25.0,
        bearing=0,
    )
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>{title}</b><br/>Region: {region}<br/>Risk: {risk_score}<br/>Exposure: {exposure_usd}",
            "style": {"backgroundColor": "steelblue", "color": "white", "padding": "6px"},
        },
        map_style="dark",
        map_provider="carto",
    )
    try:
        st.pydeck_chart(r, width="stretch")
    except TypeError:
        st.pydeck_chart(r, use_container_width=True)


def main() -> None:
    """Render the Risk Radar page."""
    inject_full_width_css()
    config = get_config()
    render_groq_status()
    st.title("Risk Radar")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return

    st.subheader("PESTEL Category Breakdown")
    _render_pestel_bar_chart(filtered)

    st.subheader("World Risk Map")
    _render_world_risk_map(filtered, "scatter")


if __name__ == "__main__":
    main()
