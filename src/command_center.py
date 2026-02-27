"""Command Center — single-page dashboard with KPIs, charts, and risk event mitigation."""

from __future__ import annotations

import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.aggregation import category_breakdown, compute_kpis
from src.config import get_config
from src.geo_utils import get_event_coordinates
from src.ui_utils import (
    load_events,
    render_debug_panel,
    render_sidebar,
)

# ── Severity styling ──────────────────────────────────────────────────────────
_SEVERITY_COLORS = {
    "Critical": "#c0392b",
    "High": "#e67e22",
    "Medium": "#c9a800",
    "Low": "#27ae60",
}
_SEVERITY_BG = {
    "Critical": "#fdf0ef",
    "High": "#fef9ef",
    "Medium": "#fefdef",
    "Low": "#f0fef4",
}

# ── Mitigation action display ─────────────────────────────────────────────────
_ACTION_LABELS = ["Immediate Action", "Near-Term Action", "Strategic Action"]
_ACTION_COLORS = ["#e74c3c", "#f39c12", "#27ae60"]

_FALLBACK_ACTIONS: dict[str, list[str]] = {
    "Labor Strike": [
        "Validate union negotiations and identify all affected supplier sites.",
        "Activate alternate component sourcing outside the affected region.",
        "Notify OEM procurement and production scheduling teams immediately.",
    ],
    "Plant Shutdown": [
        "Confirm shutdown scope and estimated restart timeline with the supplier.",
        "Activate emergency alternate sourcing protocols for critical parts.",
        "Adjust production schedules and alert downstream OEM partners.",
    ],
    "Port Congestion": [
        "Re-route shipments through alternate ports or intermodal channels.",
        "Accelerate customs clearance for all critical in-transit inventory.",
        "Increase safety stock levels at key regional distribution centers.",
    ],
    "Export Restriction": [
        "Engage trade counsel to assess full sanction or tariff exposure.",
        "Identify alternate country-of-origin sourcing for restricted materials.",
        "Initiate compliance review of all affected SKUs and HS codes.",
    ],
    "Cyberattack": [
        "Conduct immediate cybersecurity status check with all affected suppliers.",
        "Isolate affected supply nodes and activate business continuity protocols.",
        "Assess data integrity and switch to manual backup processes where needed.",
    ],
    "Natural Disaster": [
        "Assess supplier facility damage and estimated recovery timeline.",
        "Activate emergency inventory buffers and spot-market sourcing.",
        "Monitor government disaster response for infrastructure recovery ETA.",
    ],
    "Supplier Insolvency": [
        "Confirm financial status and insolvency proceedings with legal counsel.",
        "Initiate urgent dual-sourcing or spot procurement for critical parts.",
        "Review contractual protections and IP/tooling escrow arrangements.",
    ],
    "Regulatory Change": [
        "Engage compliance and legal teams to assess applicability and timeline.",
        "Audit affected product lines and certification requirements.",
        "Plan phased supply chain transition to minimize operational disruption.",
    ],
}
_DEFAULT_FALLBACK = [
    "Confirm affected suppliers and assess inventory buffer levels.",
    "Review alternate routing and logistics contingency options.",
    "Notify procurement and production planning teams immediately.",
]

# ── Groq mitigation (live, cached per event for 1 hour) ──────────────────────
@st.cache_data(ttl=3600)
def _fetch_groq_mitigation(
    event_id: str,
    event_title: str,
    event_summary: str,
    reason_flagged: str,
    disruption_type: str,
    geo_country: str,
    component_entities_json: str,
    groq_api_key: str,
    groq_model: str,
) -> dict | None:
    from src.groq_client import generate_mitigation_text
    return generate_mitigation_text(
        event_title=event_title,
        event_summary=event_summary,
        reason_flagged=reason_flagged,
        disruption_type=disruption_type,
        geo_country=geo_country,
        component_entities=json.loads(component_entities_json),
        api_key=groq_api_key,
        model=groq_model,
    )


def _get_mitigation(event: dict, config) -> tuple[str, list[str], bool]:
    """Return (description, actions[1-3], used_groq).
    Priority: live Groq → stored DB → deterministic playbook.
    """
    if config.groq_api_key:
        try:
            result = _fetch_groq_mitigation(
                event_id=str(event.get("event_id") or event.get("article_url") or ""),
                event_title=str(event.get("title") or ""),
                event_summary=str(event.get("event_summary") or ""),
                reason_flagged=str(event.get("reason_flagged") or ""),
                disruption_type=str(event.get("disruption_type") or "Other"),
                geo_country=str(event.get("geo_country") or "Unknown"),
                component_entities_json=json.dumps(event.get("component_entities") or []),
                groq_api_key=config.groq_api_key,
                groq_model=config.groq_model,
            )
        except Exception:
            result = None
        if result:
            desc = result.get("mitigation_description") or ""
            actions = result.get("mitigation_actions") or []
            if isinstance(actions, list) and len(actions) >= 1:
                return desc, [str(a) for a in actions[:3]], True

    stored = event.get("mitigation_actions") or []
    stored_desc = event.get("mitigation_description") or ""
    if stored:
        return stored_desc, [str(a) for a in stored[:3]], False

    dtype = str(event.get("disruption_type") or "Other")
    fallback = _FALLBACK_ACTIONS.get(dtype, _DEFAULT_FALLBACK)
    return "Groq LLM not configured — deterministic playbook applied.", fallback, False


# ── KPI Cards — uniform single color, no emojis ───────────────────────────────
_KPI_CARD_BG = "#1e3050"
_KPI_ACCENT = "#2563eb"
_KPI_VALUE_COLOR = "#ffffff"
_KPI_LABEL_COLOR = "#94a3b8"


def _kpi_card_html(label: str, value: str) -> str:
    return (
        f'<div style="background:{_KPI_CARD_BG};border-radius:10px;'
        f'padding:20px 22px 18px;border-top:3px solid {_KPI_ACCENT};'
        f'margin-bottom:8px;">'
        f'<div style="font-size:0.64rem;color:{_KPI_LABEL_COLOR};text-transform:uppercase;'
        f'letter-spacing:0.1em;font-weight:600;margin-bottom:10px;">{label}</div>'
        f'<div style="font-size:2rem;font-weight:700;color:{_KPI_VALUE_COLOR};'
        f'line-height:1.1;">{value}</div>'
        f"</div>"
    )


def _render_kpi_cards(kpis) -> None:
    """Six uniform KPI cards in two rows of three."""
    delta_up = kpis.delta_vs_yesterday > 0
    delta_arrow = "▲" if delta_up else "▼"
    delta_val = f"{delta_arrow} {abs(kpis.delta_vs_yesterday):.2f}"

    cards = [
        ("Active Risk Events", str(kpis.total_events)),
        ("High / Critical Events", str(kpis.high_critical_events)),
        ("Avg Severity Score", f"{kpis.avg_severity_today:.1f}"),
        # "Severity Change (24h)" — avg risk score today minus yesterday's average.
        # Positive (▲) means risk severity increased; negative (▼) means it improved.
        ("Severity Change (24h)", delta_val),
        ("Avg Estimated Delay", f"{kpis.avg_delay_days:.1f} days"),
        ("Total Estimated Exposure", f"${kpis.total_exposure_usd:,.0f}"),
    ]

    col1, col2, col3 = st.columns(3)
    for col, (label, value) in zip([col1, col2, col3], cards[:3]):
        col.markdown(_kpi_card_html(label, value), unsafe_allow_html=True)

    st.markdown("<div style='height:2px'></div>", unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)
    for col, (label, value) in zip([col4, col5, col6], cards[3:]):
        col.markdown(_kpi_card_html(label, value), unsafe_allow_html=True)


# ── Charts ────────────────────────────────────────────────────────────────────
_CHART_BG = "rgba(0,0,0,0)"
_GRID_COLOR = "rgba(255,255,255,0.07)"
_TICK_COLOR = "#9ca3af"


def _render_severity_trend(events: list[dict]) -> None:
    """Risk Severity Over Time — Plotly area line chart."""
    df = pd.DataFrame(events)
    if df.empty:
        st.info("No data available.")
        return
    df["published_date"] = pd.to_datetime(df["published_at"]).dt.date
    severity = (
        df.groupby("published_date")["risk_score_0to100"]
        .mean()
        .reset_index()
        .sort_values("published_date")
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=severity["published_date"],
            y=severity["risk_score_0to100"],
            mode="lines+markers",
            line=dict(color=_KPI_ACCENT, width=2),
            marker=dict(size=4, color=_KPI_ACCENT),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.10)",
            hovertemplate="<b>%{x}</b><br>Avg Severity: %{y:.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        height=310,
        margin=dict(l=0, r=0, t=8, b=0),
        xaxis=dict(
            title="",
            gridcolor=_GRID_COLOR,
            tickfont=dict(color=_TICK_COLOR, size=10),
            showgrid=True,
        ),
        yaxis=dict(
            title=dict(text="Avg Risk Score (0–100)", font=dict(color=_TICK_COLOR, size=10)),
            gridcolor=_GRID_COLOR,
            tickfont=dict(color=_TICK_COLOR, size=10),
            range=[0, 100],
        ),
        plot_bgcolor=_CHART_BG,
        paper_bgcolor=_CHART_BG,
        font=dict(color=_TICK_COLOR),
        showlegend=False,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_pestel_chart(events: list[dict]) -> None:
    """PESTEL category breakdown — horizontal Plotly bar chart."""
    counts = category_breakdown(events)
    if not counts:
        st.info("No PESTEL data available.")
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
                    colorscale=[
                        [0, "rgb(37,99,235,0.3)"],
                        [0.5, "rgb(37,99,235,0.65)"],
                        [1, "rgb(37,99,235,1)"],
                    ],
                    showscale=False,
                ),
                text=df["count"],
                textposition="outside",
                texttemplate="%{text}",
                textfont=dict(color=_TICK_COLOR, size=11),
                hovertemplate="%{y}<br>Count: %{x}<extra></extra>",
            )
        ],
        layout=go.Layout(
            height=310,
            margin=dict(l=10, r=40, t=8, b=0),
            xaxis=dict(
                title="Events",
                gridcolor=_GRID_COLOR,
                tickfont=dict(color=_TICK_COLOR, size=10),
            ),
            yaxis=dict(
                title="",
                automargin=True,
                tickfont=dict(color=_TICK_COLOR, size=12),
            ),
            plot_bgcolor=_CHART_BG,
            paper_bgcolor=_CHART_BG,
            showlegend=False,
        ),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_world_risk_map(events: list[dict]) -> None:
    """Interactive world risk map — Plotly Scattergeo."""
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
            text=[
                f"{t}<br>Region: {r}<br>Risk: {s:.1f}<br>Exposure: ${e:,.0f}"
                for t, r, s, e in zip(titles, regions, scores, exposures)
            ],
            mode="markers",
            marker=dict(
                size=[12 + s / 3.5 for s in scores],
                color=scores,
                colorscale="Reds",
                showscale=True,
                colorbar=dict(
                    title=dict(text="Risk Score", font=dict(color="#FAFAFA")),
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
    theme_bg = "#0E1117"
    fig.update_geos(
        showland=True,
        showcountries=True,
        showlakes=True,
        showocean=True,
        landcolor="rgb(45,48,58)",
        oceancolor=theme_bg,
        countrycolor="rgb(60,63,75)",
        coastlinecolor="rgb(55,58,70)",
        lakecolor=theme_bg,
        projection_type="natural earth",
        bgcolor=theme_bg,
        lataxis=dict(gridcolor="rgba(250,250,250,0.10)"),
        lonaxis=dict(gridcolor="rgba(250,250,250,0.10)"),
    )
    fig.update_layout(
        title=None,
        height=520,
        margin=dict(l=0, r=0, t=8, b=0),
        geo=dict(scope="world"),
        paper_bgcolor=theme_bg,
        plot_bgcolor=theme_bg,
        font=dict(color="#FAFAFA", size=11),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})


# ── Top risk event cards with mitigation ─────────────────────────────────────
def _render_top_event_card(rank: int, event: dict, config) -> None:
    severity_band = str(event.get("severity_band") or "Medium")
    sev_color = _SEVERITY_COLORS.get(severity_band, "#888")
    sev_bg = _SEVERITY_BG.get(severity_band, "#fafafa")
    score = float(event.get("risk_score_0to100") or 0)
    title = str(event.get("title") or "Untitled Event")
    url = str(event.get("article_url") or "#")
    summary = str(event.get("event_summary") or "")
    reason = str(event.get("reason_flagged") or "")
    disruption = str(event.get("disruption_type") or "")
    country = str(event.get("geo_country") or "")
    region = str(event.get("geo_region") or "")
    blurb = str(event.get("dashboard_blurb") or "")

    reason_html = (
        f"<div style='font-size:0.81rem;color:#777;font-style:italic;margin-top:8px;'>"
        f"Flagged: {reason}</div>"
        if reason else ""
    )
    blurb_html = (
        f"<div style='font-size:0.81rem;color:#555;margin-top:6px;'>"
        f"<b>Risks identified:</b> {blurb}</div>"
        if blurb else ""
    )
    location_line = " · ".join(filter(None, [country, region, disruption]))

    st.markdown(
        f"""<div style="border:1.5px solid {sev_color};border-radius:12px;
  padding:22px 26px 16px;background:{sev_bg};
  box-shadow:0 3px 12px rgba(0,0,0,0.07);margin-bottom:6px;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;
    gap:14px;margin-bottom:10px;">
    <div style="font-size:0.98rem;font-weight:700;color:#1a1a2e;line-height:1.45;flex:1;">
      <span style="background:{_KPI_CARD_BG};color:#94a3b8;font-size:0.68rem;font-weight:700;
        padding:2px 9px;border-radius:20px;margin-right:8px;vertical-align:middle;">#{rank}</span>
      <a href="{url}" target="_blank" style="color:#1e3a6e;text-decoration:none;
        font-weight:700;">{title}</a>
    </div>
    <div style="display:flex;gap:7px;align-items:center;flex-shrink:0;">
      <span style="background:{sev_color};color:#fff;font-size:0.65rem;font-weight:800;
        padding:3px 10px;border-radius:20px;letter-spacing:0.05em;">{severity_band.upper()}</span>
      <span style="background:{_KPI_CARD_BG};color:#f0c040;font-size:0.68rem;font-weight:800;
        padding:3px 10px;border-radius:20px;">RISK {score:.0f} / 100</span>
    </div>
  </div>
  <div style="font-size:0.78rem;color:#888;margin-bottom:10px;">{location_line}</div>
  <div style="font-size:0.9rem;color:#333;line-height:1.65;">{summary}</div>
  {reason_html}{blurb_html}
</div>""",
        unsafe_allow_html=True,
    )

    # Mitigation panel
    mit_desc, mit_actions, used_groq = _get_mitigation(event, config)
    source_label = "AI-Powered" if used_groq else "Playbook"

    action_html = ""
    for i, action in enumerate(mit_actions[:3]):
        label = _ACTION_LABELS[i] if i < len(_ACTION_LABELS) else f"Action {i + 1}"
        color = _ACTION_COLORS[i] if i < len(_ACTION_COLORS) else "#888"
        action_html += (
            f"<div style='border-left:3px solid {color};background:rgba(255,255,255,0.06);"
            f"border-radius:5px;padding:10px 14px;margin-bottom:8px;color:#e2e8f0;"
            f"font-size:0.86rem;line-height:1.6;'>"
            f"<span style='font-weight:700;color:{color};'>{label}:&nbsp;</span>"
            f"{action}</div>"
        )

    desc_html = (
        f"<div style='color:#94a3b8;font-size:0.81rem;margin-bottom:13px;"
        f"font-style:italic;'>{mit_desc}</div>"
        if mit_desc else ""
    )

    st.markdown(
        f"""<div style="background:{_KPI_CARD_BG};border-top:3px solid {_KPI_ACCENT};
  border-radius:10px;padding:18px 22px 14px;margin-bottom:28px;">
  <div style="font-size:0.68rem;color:{_KPI_LABEL_COLOR};text-transform:uppercase;
    letter-spacing:0.1em;font-weight:700;margin-bottom:10px;">{source_label} Mitigation</div>
  {desc_html}{action_html}
</div>""",
        unsafe_allow_html=True,
    )


# ── Main entry point ──────────────────────────────────────────────────────────
def render_command_center() -> None:
    """Render the Command Center — tabbed: Overview | Top Risk Events."""
    config = get_config()
    st.title("Command Center")
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return

    kpis = compute_kpis(filtered)

    tab_overview, tab_events = st.tabs(["Overview", "Top Risk Events"])

    # ── Tab 1: Overview ───────────────────────────────────────────────────────
    with tab_overview:
        _render_kpi_cards(kpis)

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # Side-by-side: Severity Trend | PESTEL Breakdown
        col_l, col_r = st.columns([3, 2])
        with col_l:
            st.markdown("**Risk Severity Over Time**")
            st.caption("Average daily risk score across all active events.")
            _render_severity_trend(filtered)
        with col_r:
            st.markdown("**PESTEL Category Breakdown**")
            st.caption("Event count by PESTEL risk category.")
            _render_pestel_chart(filtered)

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        st.markdown("**World Risk Map**")
        st.caption("Geographic distribution of active risk events. Bubble size and color indicate risk score.")
        _render_world_risk_map(filtered)

    # ── Tab 2: Top Risk Events & Mitigation ───────────────────────────────────
    with tab_events:
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
            "Mitigation is AI-powered via Groq LLM when configured, "
            "otherwise a deterministic playbook is applied."
        )

        for rank, event in enumerate(top_events, 1):
            _render_top_event_card(rank, event, config)
