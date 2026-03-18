"""Shared Streamlit UI helpers."""

from __future__ import annotations

__all__ = [
    "inject_full_width_css",
    "load_events",
    "filter_events",
    "render_sidebar",
    "render_debug_panel",
    "render_events_table",
]

from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
from typing import Iterable  # noqa: UP006 — used in public __all__ signatures

import pandas as pd
import streamlit as st

from src.config import get_config
from src.date_utils import parse_datetime
from src.debug import get_debug_data
from src.rss_ingest import run_pipeline
from src.storage import DbPaths, fetch_enriched_events, get_meta_value, init_db, set_meta_value
from src.storage_utils import row_to_dict

_logger = logging.getLogger(__name__)


@st.cache_data(ttl=300)
def load_events(db_path: Path) -> list[dict[str, object]]:
    """Load enriched events for display."""

    config = get_config()
    paths = DbPaths(db_path, config.db_url)
    init_db(paths)
    rows = fetch_enriched_events(paths)
    return [row_to_dict(dict(row)) for row in rows]


def auto_refresh_if_due(config: object) -> bool:
    """Run pipeline if the last refresh is older than config.refresh_interval_hours (default 24h)."""

    interval_hours = getattr(config, "refresh_interval_hours", 24)
    paths = DbPaths(config.db_path, config.db_url)
    init_db(paths)
    last_refresh = get_meta_value(paths, "last_refresh_at")
    now = datetime.now(timezone.utc)
    if last_refresh:
        last_dt = parse_datetime(last_refresh)
        if (now - last_dt).total_seconds() < interval_hours * 3600:
            return False
    run_pipeline(config)
    set_meta_value(paths, "last_refresh_at", now.isoformat())
    st.cache_data.clear()
    return True


@st.cache_data
def filter_events(
    events: list[dict[str, object]],
    start: date,
    end: date,
    categories: tuple[str, ...],
    regions: tuple[str, ...],
    severity_range: tuple[float, float],
) -> list[dict[str, object]]:
    """Filter events using sidebar filters."""

    filtered: list[dict[str, object]] = []
    for event in events:
        published_at = parse_datetime(str(event["published_at"]))
        if not (start <= published_at.date() <= end):
            continue
        if categories and str(event["risk_category"]) not in categories:
            continue
        if regions and str(event["geo_region"]) not in regions:
            continue
        score = float(event["risk_score_0to100"])
        if not (severity_range[0] <= score <= severity_range[1]):
            continue
        filtered.append(event)
    return filtered


def _default_date_range(dates: Iterable[date]) -> tuple[date, date]:
    """Return default date range (last 365 days) within data bounds."""

    dates_list = list(dates)
    today = date.today()
    if not dates_list:
        return today - timedelta(days=365), today
    min_date = min(dates_list)
    max_date = max(dates_list)
    start = max(max_date - timedelta(days=365), min_date)
    end = max(max_date, start)
    return start, end


def inject_full_width_css() -> None:
    """Inject all global CSS — call once at app startup before any page renders."""
    st.markdown(
        """
        <style>
        /* ── Full-width layout ── */
        section.main .block-container,
        .main .block-container,
        div[data-testid="stAppViewContainer"] main .block-container,
        div[data-testid="stAppViewContainer"] section.main div,
        section[data-testid="stSidebar"] ~ div .block-container,
        .block-container {
            max-width: 100% !important;
            width: 100% !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
        }
        section.main > div {
            max-width: 100% !important;
            width: 100% !important;
        }
        /* ── Reduce top gap ── */
        .block-container,
        div[data-testid="stMainBlockContainer"] {
            padding-top: 2rem !important;
        }
        /* ── Page titles ── */
        h1 { font-size: 1.6rem !important; font-weight: 700 !important; }
        /* ── Sidebar nav links ── */
        [data-testid="stSidebarNavLink"] p,
        [data-testid="stSidebarNavLink"] span {
            font-size: 1.25rem !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebarNavLink"] {
            padding-top: 0.6rem !important;
            padding-bottom: 0.6rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )



def render_sidebar(events: list[dict[str, object]]) -> tuple[list[dict[str, object]], bool]:
    """Render global sidebar controls."""
    config = get_config()

    # ── Filters ───────────────────────────────────────────────────────────────
    dates = [parse_datetime(str(item["published_at"])).date() for item in events]
    start_default, end_default = _default_date_range(dates)

    with st.sidebar.expander("Filters", expanded=False):
        date_value = st.date_input("Date range", (start_default, end_default))
        if isinstance(date_value, tuple) and len(date_value) == 2:
            start, end = date_value
        else:
            st.warning("Select a start and end date to apply the range.")
            start, end = start_default, end_default
        if start > end:
            st.warning("Start date must be before end date.")
            start, end = start_default, end_default
        categories = sorted({str(item["risk_category"]) for item in events})
        selected_categories = st.multiselect(
            "Categories", categories, default=categories
        )
        regions = sorted({str(item["geo_region"]) for item in events})
        selected_regions = st.multiselect("Regions", regions, default=regions)
        severity = st.slider("Severity", 0.0, 100.0, (0.0, 100.0))

    filtered = filter_events(
        events,
        start=start,
        end=end,
        categories=tuple(selected_categories),
        regions=tuple(selected_regions),
        severity_range=severity,
    )

    # ── Controls (bottom) ─────────────────────────────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.header("Controls")
    # Skip auto-refresh when using Supabase (Cloud) to avoid DB statement timeouts
    if not config.db_url and auto_refresh_if_due(config):
        st.sidebar.success("Auto refresh complete.")
    refresh_clicked = st.sidebar.button("Refresh data")
    last_refresh = get_meta_value(DbPaths(config.db_path, config.db_url), "last_refresh_at")
    if last_refresh:
        try:
            last_dt = parse_datetime(last_refresh)
            st.sidebar.markdown(
                f"<span style='color: #27ae60; font-size: 0.8rem;'>Last refresh: {last_dt.strftime('%b %d, %Y %H:%M UTC')}</span>",
                unsafe_allow_html=True,
            )
        except Exception as _exc:
            _logger.warning("Failed to parse last_refresh_at %r: %s", last_refresh, _exc)
            st.sidebar.markdown(
                f"<span style='color: #27ae60; font-size: 0.8rem;'>Last refresh: {last_refresh[:16] if last_refresh else '—'}</span>",
                unsafe_allow_html=True,
            )
    else:
        st.sidebar.markdown(
            "<span style='color: #27ae60; font-size: 0.8rem;'>Last refresh: never</span>",
            unsafe_allow_html=True,
        )
    if refresh_clicked:
        try:
            run_pipeline(config)
            set_meta_value(
                DbPaths(config.db_path, config.db_url),
                "last_refresh_at",
                datetime.now(timezone.utc).isoformat(),
            )
            st.cache_data.clear()
            st.sidebar.success("Refresh complete.")
        except Exception as exc:
            st.sidebar.error(f"Refresh failed: {exc}")

    status_line = (
        f"Currently displaying {len(filtered)} events across "
        f"{len(selected_categories)} categories in {len(selected_regions)} regions"
    )
    st.sidebar.markdown(
        f"<span style='color: #2980b9; font-size: 0.8rem;'>{status_line}</span>",
        unsafe_allow_html=True,
    )
    show_debug = st.sidebar.checkbox("Show debug panel", value=False)
    st.sidebar.markdown(
        "<div style='margin-top: 12px;'>Built by Nihal Vootkoor &nbsp;·&nbsp; <strong>DEMO</strong></div>",
        unsafe_allow_html=True,
    )
    return filtered, show_debug


def render_debug_panel(db_path: Path) -> None:
    """Render debug data in the sidebar."""

    config = get_config()
    seeds_path = config.project_root / "data" / "seeds.csv"
    st.sidebar.subheader("Admin actions")
    if st.sidebar.button("Import Seeds (Backfill)"):
        try:
            from src.backfill import run_seed_backfill
            stats = run_seed_backfill(str(seeds_path))
            st.cache_data.clear()
            st.sidebar.success("Seed backfill complete.")
            st.session_state["seed_backfill_stats"] = stats
        except Exception as exc:
            st.sidebar.error(f"Seed backfill failed: {exc}")
    stats = st.session_state.get("seed_backfill_stats")
    if stats:
        st.sidebar.subheader("Seed backfill stats")
        st.sidebar.json(
            {
                "raw_upserts": stats.get("raw_upserts", 0),
                "candidates": stats.get("candidates", 0),
                "enriched_written": stats.get("enriched_written", 0),
                "rejected_after_validation": stats.get("rejected_after_validation", 0),
                "oldest_event_date": stats.get("oldest_event_date", ""),
            }
        )

    db_label = "Supabase" if config.db_url else "Local (SQLite)"
    st.sidebar.info(f"**Database:** {db_label}")
    if config.groq_api_key:
        st.sidebar.success("**Groq LLM:** configured")
    else:
        st.sidebar.warning("**Groq LLM:** not configured")

    debug = get_debug_data(DbPaths(db_path, config.db_url))
    st.sidebar.subheader("Pipeline counts")
    st.sidebar.json(debug.counts)


def _safe_url(raw: object) -> str:
    """Return url only if it starts with http/https, else empty string."""
    s = str(raw or "").strip()
    return s if s.startswith(("http://", "https://")) else ""


def _events_to_display_df(events: list[dict]) -> pd.DataFrame:
    """Build a display DataFrame from event dicts (selected columns for table)."""
    rows: list[dict] = []
    for e in events:
        try:
            risk_score = round(float(e.get("risk_score_0to100") or 0), 1)
        except (ValueError, TypeError):
            risk_score = 0.0
        try:
            exposure_usd = round(float(e.get("exposure_usd_est") or 0), 0)
        except (ValueError, TypeError):
            exposure_usd = 0.0
        try:
            delay_days = int(e.get("estimated_delay_days") or 0)
        except (ValueError, TypeError):
            delay_days = 0
        rows.append({
            "title": str(e.get("title") or ""),
            "risk_category": str(e.get("risk_category") or ""),
            "disruption_type": str(e.get("disruption_type") or ""),
            "geo_region": str(e.get("geo_region") or ""),
            "geo_country": str(e.get("geo_country") or ""),
            "risk_score": risk_score,
            "severity_band": str(e.get("severity_band") or ""),
            "exposure_usd": exposure_usd,
            "delay_days": delay_days,
            "published_at": str(e.get("published_at") or ""),
            "article_url": _safe_url(e.get("article_url")),
        })
    return pd.DataFrame(rows)


def render_events_table(
    events: list[dict],
    use_aggrid: bool = True,
    height: int = 400,
    selection_mode: str = "single",
) -> None:
    """Render a professional event table: AG Grid if available, else st.dataframe with column_config."""
    if not events:
        st.info("No events to display.")
        return
    df = _events_to_display_df(events)
    if use_aggrid:
        try:
            from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

            title_link_renderer = JsCode("""
class TitleLinkRenderer {
    init(params) {
        this.eGui = document.createElement('a');
        const url = params.data.article_url || '#';
        this.eGui.setAttribute('href', url);
        this.eGui.setAttribute('target', '_blank');
        this.eGui.setAttribute('rel', 'noopener noreferrer');
        this.eGui.style.color = '#4da6ff';
        this.eGui.style.textDecoration = 'none';
        this.eGui.style.cursor = 'pointer';
        this.eGui.innerText = params.value || '';
    }
    getGui() { return this.eGui; }
}
""")

            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=25)
            gb.configure_side_bar()
            gb.configure_default_column(sortable=True, filterable=True)
            gb.configure_column("article_url", hide=True)
            gb.configure_column("title", flex=2, cellRenderer=title_link_renderer)
            gb.configure_column("risk_score", width=95, sort="desc", sortIndex=0)
            gb.configure_column("exposure_usd", width=110)
            gb.configure_column("delay_days", width=95)
            if selection_mode == "single":
                gb.configure_selection(selection_mode="single", use_checkbox=True)
            elif selection_mode == "multiple":
                gb.configure_selection(selection_mode="multiple", use_checkbox=True)
            grid_options = gb.build()
            st.markdown("**Event table** — sort, filter, and paginate. Click a title to open the article.")
            grid_response = AgGrid(
                df,
                gridOptions=grid_options,
                height=height,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                theme="streamlit",
                allow_unsafe_jscode=True,
            )
            selected = grid_response.get("selected_rows")
            if selected is not None:
                has_selection = (
                    not selected.empty
                    if hasattr(selected, "empty")
                    else bool(selected)
                )
                if has_selection:
                    st.subheader("Selected row(s)")
                    records = (
                        selected.to_dict(orient="records")
                        if hasattr(selected, "to_dict")
                        else selected
                    )
                    st.json(records)
        except ImportError:
            _render_events_dataframe_fallback(df)
    else:
        _render_events_dataframe_fallback(df)


def _render_events_dataframe_fallback(df: pd.DataFrame) -> None:
    """Streamlit-native table with column_config (links, number format)."""
    st.dataframe(
        df,
        column_config={
            "title": st.column_config.TextColumn("Title", width="large"),
            "risk_category": st.column_config.TextColumn("Category"),
            "disruption_type": st.column_config.TextColumn("Disruption"),
            "geo_region": st.column_config.TextColumn("Region"),
            "geo_country": st.column_config.TextColumn("Country"),
            "risk_score": st.column_config.NumberColumn("Risk", format="%.1f"),
            "severity_band": st.column_config.TextColumn("Severity"),
            "exposure_usd": st.column_config.NumberColumn("Exposure (USD)", format="$%d"),
            "delay_days": st.column_config.NumberColumn("Delay (days)"),
            "published_at": st.column_config.TextColumn("Published"),
            "article_url": st.column_config.LinkColumn("Link", display_text="Open"),
        },
        hide_index=True,
        width="stretch",
    )
