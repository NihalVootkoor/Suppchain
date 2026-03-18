"""Command Center — single-page dashboard with KPIs, charts, and risk event mitigation."""

from __future__ import annotations

import html as _html
import json
import logging

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

_logger = logging.getLogger(__name__)

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
        except Exception as _exc:
            _logger.warning("Groq mitigation failed for event %s: %s", event.get("event_id"), _exc)
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


def _kpi_card_html(label: str, value: str, change_html: str = "") -> str:
    change_div = (
        f'<div style="font-size:1rem;font-weight:700;white-space:nowrap;">{change_html}</div>'
        if change_html else ""
    )
    return (
        f'<div style="background:{_KPI_CARD_BG};border-radius:10px;'
        f'padding:20px 22px 18px;border-top:3px solid {_KPI_ACCENT};'
        f'margin-bottom:8px;">'
        f'<div style="font-size:0.64rem;color:{_KPI_LABEL_COLOR};text-transform:uppercase;'
        f'letter-spacing:0.1em;font-weight:600;margin-bottom:10px;">{label}</div>'
        f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
        f'<div style="font-size:2rem;font-weight:700;color:{_KPI_VALUE_COLOR};'
        f'line-height:1.1;">{value}</div>'
        f'{change_div}'
        f'</div>'
        f"</div>"
    )


def _render_kpi_cards(kpis) -> None:
    """Six uniform KPI cards in two rows of three."""
    # Week-over-week event delta — red if more events (worse), green if fewer (better)
    week_delta = kpis.events_this_week - kpis.events_last_week
    week_arrow = "▲" if week_delta >= 0 else "▼"
    week_color = "#ef4444" if week_delta >= 0 else "#22c55e"
    week_change_html = (
        f'<span style="color:{week_color};">'
        f'{week_arrow} {abs(week_delta)} vs last week'
        f'</span>'
    )

    # 7-day severity delta — red if severity rose (worse), green if fell (better)
    sev_delta = kpis.delta_vs_yesterday
    sev_arrow = "▲" if sev_delta > 0 else "▼"
    sev_color = "#ef4444" if sev_delta > 0 else ("#22c55e" if sev_delta < 0 else "#94a3b8")
    sev_change_html = (
        f'<span style="color:{sev_color};">'
        f'{sev_arrow} {abs(sev_delta):.1f} vs last week'
        f'</span>'
    )

    col1, col2, col3 = st.columns(3)
    col1.markdown(
        _kpi_card_html("Active Risk Events", str(kpis.total_events), change_html=week_change_html),
        unsafe_allow_html=True,
    )
    col2.markdown(_kpi_card_html("High / Critical Events", str(kpis.high_critical_events)), unsafe_allow_html=True)
    col3.markdown(
        _kpi_card_html("Avg Severity Score (7d)", f"{kpis.avg_severity_today:.1f}", change_html=sev_change_html),
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:2px'></div>", unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)
    col4.markdown(
        _kpi_card_html("Highest Risk Region", kpis.highest_risk_region or "—"),
        unsafe_allow_html=True,
    )
    col5.markdown(_kpi_card_html("Avg Estimated Delay", f"{kpis.avg_delay_days:.1f} days"), unsafe_allow_html=True)
    col6.markdown(
        _kpi_card_html("Total Estimated Exposure", f"${kpis.total_exposure_usd:,.0f}"),
        unsafe_allow_html=True,
    )


# ── Charts ────────────────────────────────────────────────────────────────────
_CHART_BG = "rgba(0,0,0,0)"
_GRID_COLOR = "rgba(255,255,255,0.07)"
_TICK_COLOR = "#9ca3af"


def _render_severity_trend(events: list[dict]) -> None:
    """Risk Severity Over Time — Plotly area line chart with threshold lines and 7d rolling avg."""
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
    # 7-day rolling average (over data points, min 1 period)
    severity["rolling_7d"] = severity["risk_score_0to100"].rolling(7, min_periods=1).mean()

    fig = go.Figure()
    # Area: daily avg
    fig.add_trace(
        go.Scatter(
            x=severity["published_date"],
            y=severity["risk_score_0to100"],
            mode="lines+markers",
            name="Daily Avg",
            line=dict(color=_KPI_ACCENT, width=2),
            marker=dict(size=4, color=_KPI_ACCENT),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.10)",
            hovertemplate="<b>%{x}</b><br>Daily Avg: %{y:.1f}<extra></extra>",
        )
    )
    # 7-day rolling average line
    fig.add_trace(
        go.Scatter(
            x=severity["published_date"],
            y=severity["rolling_7d"],
            mode="lines",
            name="7d Avg",
            line=dict(color="rgba(255,255,255,0.55)", width=1.5, dash="dot"),
            hovertemplate="<b>%{x}</b><br>7d Avg: %{y:.1f}<extra></extra>",
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
        showlegend=True,
        legend=dict(
            orientation="h",
            x=0,
            y=1.08,
            font=dict(color=_TICK_COLOR, size=10),
            bgcolor="rgba(0,0,0,0)",
        ),
        hovermode="x unified",
    )
    # High threshold line (70)
    fig.add_hline(
        y=70,
        line_dash="dash",
        line_color="#e67e22",
        line_width=1,
        annotation_text="High (70)",
        annotation_position="right",
        annotation_font=dict(color="#e67e22", size=9),
    )
    # Critical threshold line (85)
    fig.add_hline(
        y=85,
        line_dash="dash",
        line_color="#c0392b",
        line_width=1,
        annotation_text="Critical (85)",
        annotation_position="right",
        annotation_font=dict(color="#c0392b", size=9),
    )
    # Legend-only dummy traces for threshold lines
    fig.add_trace(
        go.Scatter(
            x=[None], y=[None],
            mode="lines",
            name="High Threshold (70)",
            line=dict(color="#e67e22", width=1, dash="dash"),
            showlegend=True,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[None], y=[None],
            mode="lines",
            name="Critical Threshold (85)",
            line=dict(color="#c0392b", width=1, dash="dash"),
            showlegend=True,
        )
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
        if lat == 0.0 and lon == 0.0:
            continue
        lats.append(float(lat))
        lons.append(float(lon))
        try:
            score = float(e.get("risk_score_0to100") or 0)
        except (ValueError, TypeError):
            _logger.warning("Invalid risk_score_0to100 for event %s", e.get("event_id"))
            score = 0.0
        scores.append(score)
        titles.append((e.get("title") or "").strip() or "—")
        regions.append((e.get("geo_region") or "").strip() or "—")
        try:
            exposures.append(round(float(e.get("exposure_usd_est") or 0), 0))
        except (ValueError, TypeError):
            exposures.append(0.0)

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
                cmin=0,
                cmax=100,
                showscale=True,
                colorbar=dict(
                    title=dict(text="Risk Score", font=dict(color="#FAFAFA")),
                    tickfont=dict(color="#FAFAFA"),
                    x=1.01,
                    xanchor="left",
                    y=0.5,
                    yanchor="middle",
                    len=0.6,
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
        title="",
        height=520,
        margin=dict(l=0, r=70, t=8, b=0),
        geo=dict(scope="world", lonaxis=dict(range=[-180, 180]), lataxis=dict(range=[-90, 90])),
        paper_bgcolor=theme_bg,
        plot_bgcolor=theme_bg,
        font=dict(color="#FAFAFA", size=11),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})


# ── Top risk event cards with mitigation ─────────────────────────────────────
def _render_top_event_card(rank: int, event: dict, config) -> None:
    """Render a unified card: event metadata + brief summary + AI mitigation."""
    # ── Data extraction ────────────────────────────────────────────────────────
    severity_band = str(event.get("severity_band") or "Medium")
    sev_color = _SEVERITY_COLORS.get(severity_band, "#888")
    sev_bg = _SEVERITY_BG.get(severity_band, "#f8fafc")
    try:
        score = float(event.get("risk_score_0to100") or 0)
    except (ValueError, TypeError):
        _logger.warning("Invalid risk_score_0to100 for event %s", event.get("event_id"))
        score = 0.0
    title = _html.escape(str(event.get("title") or "Untitled Event"))
    url = str(event.get("article_url") or "#")
    disruption = str(event.get("disruption_type") or "Unknown Type")
    country = str(event.get("geo_country") or "")
    region = str(event.get("geo_region") or "")
    delay_days = int(event.get("estimated_delay_days") or 0)
    published_at = str(event.get("published_at") or "")
    ingested_at = str(event.get("ingested_at") or "")

    # Date
    try:
        date_str = pd.to_datetime(published_at).strftime("%b %d, %Y")
    except Exception:
        date_str = published_at[:10] if published_at else "—"

    # NEW badge: event ingested within the last 48 hours
    is_new = False
    if ingested_at:
        try:
            from datetime import datetime, timedelta, timezone
            ingested_dt = pd.to_datetime(ingested_at, utc=True)
            cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
            is_new = ingested_dt.to_pydatetime() >= cutoff
        except Exception:
            pass

    # Location — drop "Unknown" tokens
    loc_parts = [p for p in [country, region] if p and p.lower() not in ("unknown", "")]
    location_str = ", ".join(loc_parts) if loc_parts else "Location TBD"

    # Duration label
    duration_str = f"~{delay_days} days est. delay" if delay_days else "Unknown duration"

    # ── Compute mitigation first (before building HTML) ───────────────────────
    mit_desc, mit_actions, used_groq = _get_mitigation(event, config)
    source_label = "AI-Powered Mitigation" if used_groq else "Playbook Mitigation"

    # ── Mitigation actions HTML ────────────────────────────────────────────────
    action_html = ""
    for i, action in enumerate(mit_actions[:3]):
        label = _ACTION_LABELS[i] if i < len(_ACTION_LABELS) else f"Action {i + 1}"
        color = _ACTION_COLORS[i] if i < len(_ACTION_COLORS) else "#888"
        action_html += (
            f"<div style='border-left:3px solid {color};"
            f"background:rgba(255,255,255,0.04);border-radius:0 6px 6px 0;"
            f"padding:10px 14px;margin-bottom:8px;'>"
            f"<div style='font-size:0.68rem;font-weight:800;color:{color};"
            f"text-transform:uppercase;letter-spacing:0.06em;margin-bottom:3px;'>{label}</div>"
            f"<div style='font-size:0.85rem;color:#e2e8f0;line-height:1.6;'>{_html.escape(str(action))}</div>"
            f"</div>"
        )

    desc_html = (
        f"<div style='color:#ffffff;font-size:0.85rem;font-weight:700;margin-bottom:12px;"
        f"line-height:1.6;'>{_html.escape(str(mit_desc))}</div>"
        if mit_desc else ""
    )

    # Escape remaining chip strings
    _loc  = _html.escape(location_str)
    _disr = _html.escape(disruption)
    _dur  = _html.escape(duration_str)
    _date = _html.escape(date_str)
    _sev  = _html.escape(severity_band.upper())

    # ── Single unified card HTML ───────────────────────────────────────────────
    # Use st.html() to render raw HTML directly — avoids Streamlit's markdown
    # pre-processor treating deeply-indented lines as code blocks.
    st.html(
        f'<div style="border:1.5px solid #3f4450;border-radius:14px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.18);margin-bottom:26px;">'
        f'<div style="background:#0E1117;padding:22px 26px 20px;">'
        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:14px;">'
        f'<div style="flex:1;min-width:0;">'
        f'<span style="background:rgba(255,255,255,0.08);color:#94a3b8;font-size:0.62rem;font-weight:700;padding:2px 8px;border-radius:4px;margin-right:9px;vertical-align:middle;">#{rank}</span>'
        + (f'<span style="background:#16a34a;color:#fff;font-size:0.58rem;font-weight:800;padding:2px 7px;border-radius:4px;margin-right:9px;vertical-align:middle;letter-spacing:0.06em;">NEW</span>' if is_new else "")
        + f'<a href="{url}" target="_blank" style="color:#3b82f6;text-decoration:underline;text-underline-offset:3px;text-decoration-color:#3b82f6;font-weight:700;font-size:0.97rem;line-height:1.45;word-break:break-word;">{title} ↗</a>'
        f'</div>'
        f'<div style="display:flex;gap:6px;align-items:center;flex-shrink:0;">'
        f'<span style="background:{sev_color};color:#fff;font-size:0.63rem;font-weight:800;padding:4px 11px;border-radius:4px;letter-spacing:0.07em;white-space:nowrap;">{_sev}</span>'
        f'<span style="background:rgba(239,68,68,0.18);color:#f87171;font-size:0.65rem;font-weight:800;padding:4px 11px;border-radius:4px;white-space:nowrap;">RISK {score:.0f}/100</span>'
        f'</div>'
        f'</div>'
        f'<div style="display:flex;flex-wrap:wrap;align-items:center;gap:0;font-size:0.72rem;font-weight:500;color:#ffffff;">'
        f'<span>{_date}</span>'
        f'<span style="margin:0 8px;color:rgba(255,255,255,0.3);">&middot;</span>'
        f'<span>{_loc}</span>'
        f'<span style="margin:0 8px;color:rgba(255,255,255,0.3);">&middot;</span>'
        f'<span>{_disr}</span>'
        f'<span style="margin:0 8px;color:rgba(255,255,255,0.3);">&middot;</span>'
        f'<span>{_dur}</span>'
        f'</div>'
        f'</div>'
        f'<div style="background:#0E1117;padding:18px 24px 16px;border-top:1.5px solid #3f4450;">'
        f'<div style="font-size:0.64rem;color:#ffffff;text-transform:uppercase;letter-spacing:0.11em;font-weight:700;margin-bottom:11px;">{source_label}</div>'
        f'{desc_html}{action_html}'
        f'</div>'
        f'</div>'
    )


# ── Main entry point ──────────────────────────────────────────────────────────
def render_command_center() -> None:
    """Render the Command Center — KPIs, charts, and world map."""
    config = get_config()
    st.title("Command Center")
    st.markdown(
        "This dashboard gives you a real-time view of risks affecting the global automotive supply chain. "
        "It pulls in live data from global news feeds, scores each event by severity, and surfaces the issues "
        "most likely to impact your suppliers.\n\n"
        "**How to use it:** Use the sidebar to view \"All Events\" and \"AI-Powered Mitigation\" for high risk events. "
        "Command Center provides an at a glance view with KPI cards at the top. "
        "The charts below break down risk trends over time and by category. "
        "Scroll down to the world map to see where risks are concentrated geographically."
    )
    events = load_events(config.db_path)
    filtered, show_debug = render_sidebar(events)
    if show_debug:
        render_debug_panel(config.db_path)
    if not filtered:
        st.info("No events available. Use Refresh data to ingest RSS feeds.")
        return

    kpis = compute_kpis(filtered)
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
