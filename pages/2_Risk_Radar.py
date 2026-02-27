"""Risk Radar page."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
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
                    colorscale=[[0, "rgb(220,245,220)"], [0.5, "rgb(100,200,120)"], [1, "rgb(20,120,60)"]],
                    showscale=True,
                    colorbar=dict(
                        title=dict(text="Count", font=dict(color="#FAFAFA")),
                        tickfont=dict(color="#FAFAFA"),
                    ),
                ),
                text=df["count"],
                textposition="outside",
                texttemplate="%{text}",
                textfont=dict(color="#FAFAFA"),
                hovertemplate="%{y}<br>Count: %{x}<extra></extra>",
            )
        ],
        layout=go.Layout(
            title=None,
            xaxis=dict(title="Number of events", gridcolor="rgba(250,250,250,0.15)", tickfont=dict(color="#FAFAFA")),
            yaxis=dict(
                title="",
                automargin=True,
                tickfont=dict(size=16, color="#FAFAFA"),
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
    # Use config only for Plotly options to avoid "keyword arguments deprecated" warning
    st.plotly_chart(fig, config={"displayModeBar": True})


def _render_world_risk_map(events: list[dict]) -> None:
    """Interactive world risk map (Plotly scatter_geo). No Pydeck to avoid 'undefined' label in UI."""
    if not events:
        return
    lats, lons, scores, titles, regions, exposures = [], [], [], [], [], []
    for e in events:
        lat, lon = get_event_coordinates(e)
        lats.append(float(lat))
        lons.append(float(lon))
        score = float(e.get("risk_score_0to100") or 0)
        scores.append(score)
        titles.append((e.get("title") or "").strip() or "—")
        regions.append((e.get("geo_region") or "").strip() or "—")
        exposures.append(round(float(e.get("exposure_usd_est") or 0), 0))

    fig = go.Figure(
        go.Scattergeo(
            lat=lats,
            lon=lons,
            text=[f"{t}<br>Region: {r}<br>Risk: {s:.1f}<br>Exposure: ${e:,.0f}" for t, r, s, e in zip(titles, regions, scores, exposures)],
            mode="markers",
            marker=dict(
                size=[12 + s / 3.5 for s in scores],
                color=scores,
                colorscale="Reds",
                showscale=True,
                colorbar=dict(
                    title=dict(text="Risk score", font=dict(color="#FAFAFA")),
                    tickfont=dict(color="#FAFAFA"),
                ),
                line=dict(width=0),
                opacity=0.92,
                sizemode="diameter",
            ),
            hoverinfo="text",
            hoverlabel=dict(bgcolor="#262730", font=dict(color="#FAFAFA", size=12)),
            name="",
        )
    )
    # Match Streamlit dark theme: background #0E1117, secondary #262730, text #FAFAFA
    theme_bg = "#0E1117"
    theme_secondary = "#262730"
    theme_text = "#FAFAFA"
    fig.update_geos(
        showland=True,
        showcountries=True,
        showlakes=True,
        showocean=True,
        landcolor="rgb(45, 48, 58)",
        oceancolor=theme_bg,
        countrycolor="rgb(60, 63, 75)",
        coastlinecolor="rgb(55, 58, 70)",
        lakecolor=theme_bg,
        projection_type="natural earth",
        bgcolor=theme_bg,
        lataxis=dict(gridcolor="rgba(250,250,250,0.12)"),
        lonaxis=dict(gridcolor="rgba(250,250,250,0.12)"),
    )
    fig.update_layout(
        title=None,
        height=620,
        margin=dict(l=0, r=0, t=24, b=0),
        geo=dict(scope="world"),
        paper_bgcolor=theme_bg,
        plot_bgcolor=theme_bg,
        font=dict(color=theme_text, size=11),
    )
    # Use config only for Plotly options to avoid "keyword arguments deprecated" warning
    st.plotly_chart(fig, config={"displayModeBar": True})


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
    _render_world_risk_map(filtered)


if __name__ == "__main__":
    main()
