"""Risk Radar page."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from src.aggregation import category_breakdown
from src.config import get_config
from src.geo_utils import get_event_coordinates
from src.ui_utils import load_events, render_debug_panel, render_groq_status, render_sidebar


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
            )
        ],
        layout=go.Layout(
            title=dict(text="PESTEL Category Breakdown", font=dict(size=18)),
            xaxis=dict(title="Number of events", gridcolor="rgba(128,128,128,0.2)"),
            yaxis=dict(title="", automargin=True),
            margin=dict(l=20, r=80),
            height=320,
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
    # Add lat/lon for each event (with small jitter to avoid exact overlap)
    import random
    map_data = []
    for e in events:
        lat, lon = get_event_coordinates(e)
        jitter = 0.3
        lat += random.uniform(-jitter, jitter)
        lon += random.uniform(-jitter, jitter)
        score = float(e.get("risk_score_0to100") or 0)
        exposure = float(e.get("exposure_usd_est") or 0)
        # Color: red scale by risk (low=green, high=red)
        r_val = min(255, int(score * 2.55))
        g_val = max(0, 255 - r_val)
        map_data.append({
            "lat": lat,
            "lon": lon,
            "risk_score": score,
            "exposure_usd": exposure,
            "title": str(e.get("title") or "")[:60],
            "region": str(e.get("geo_region") or ""),
            "color": [r_val, g_val, 0],
        })
    df = pd.DataFrame(map_data)

    if layer_type == "scatter":
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["lon", "lat"],
            get_radius=80000 + (df["risk_score"] * 3000),
            get_fill_color="color",
            get_line_color=[0, 0, 0],
            line_width_min_pixels=1,
            pickable=True,
            opacity=0.7,
        )
    elif layer_type == "heatmap":
        layer = pdk.Layer(
            "HeatmapLayer",
            data=df,
            get_position=["lon", "lat"],
            get_weight="risk_score",
            radius_pixels=40,
            intensity=1,
            threshold=0.05,
            pickable=True,
        )
    else:
        # hexagon
        layer = pdk.Layer(
            "HexagonLayer",
            data=df,
            get_position=["lon", "lat"],
            get_elevation="risk_score",
            elevation_scale=50,
            radius=200000,
            extruded=True,
            pickable=True,
            elevation_range=[0, 100],
            coverage=0.9,
        )

    view_state = pdk.ViewState(
        latitude=25,
        longitude=20,
        zoom=1.5,
        pitch=40 if layer_type == "hexagon" else 25,
        bearing=0,
    )
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>{title}</b><br/>Region: {region}<br/>Risk: {risk_score}<br/>Exposure: ${exposure_usd:,.0f}",
            "style": {"backgroundColor": "steelblue", "color": "white", "padding": "6px"},
        },
        map_style="mapbox://styles/mapbox/light-v11",
    )
    st.pydeck_chart(r, use_container_width=True)


def main() -> None:
    """Render the Risk Radar page."""
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

    metric = st.selectbox("Regional summary metric", ["count", "avg_severity", "exposure_usd"])
    region_data = region_metrics(filtered, metric)
    st.subheader("Regional Summary")
    if region_data:
        df = pd.DataFrame(region_data).set_index("region")
        st.bar_chart(df)
    else:
        st.info("No regional data available.")

    st.subheader("PESTEL Category Breakdown")
    _render_pestel_bar_chart(filtered)

    st.subheader("World Risk Map")
    map_layer = st.radio(
        "Map layer",
        ["ScatterplotLayer (dots by risk)", "HeatmapLayer (density)", "HexagonLayer (aggregation)"],
        horizontal=True,
    )
    layer_key = "scatter" if "Scatterplot" in map_layer else ("heatmap" if "Heatmap" in map_layer else "hexagon")
    _render_world_risk_map(filtered, layer_key)


if __name__ == "__main__":
    main()
