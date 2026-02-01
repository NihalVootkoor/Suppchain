"""Shared Streamlit UI helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Tuple

import streamlit as st

from src.aggregation import compute_kpis
from src.backfill import run_seed_backfill
from src.config import get_config
from src.date_utils import parse_datetime
from src.debug import get_debug_data
from src.pdf_report import generate_pdf
from src.rss_ingest import run_pipeline
from src.storage import DbPaths, fetch_enriched_events, get_meta_value, init_db, set_meta_value
from src.storage_utils import row_to_dict


@st.cache_data
def load_events(db_path: Path) -> list[dict[str, object]]:
    """Load enriched events for display."""

    config = get_config()
    paths = DbPaths(db_path, config.db_url)
    init_db(paths)
    rows = fetch_enriched_events(paths)
    return [row_to_dict(dict(row)) for row in rows]


def auto_refresh_if_due(config: object, interval_hours: int = 3) -> bool:
    """Run pipeline if the last refresh is older than the interval."""

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
    categories: Tuple[str, ...],
    regions: Tuple[str, ...],
    severity_range: Tuple[float, float],
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


def render_sidebar(events: list[dict[str, object]]) -> tuple[list[dict[str, object]], bool]:
    """Render global sidebar controls."""

    config = get_config()
    st.sidebar.header("Controls")
    if auto_refresh_if_due(config):
        st.sidebar.success("Auto refresh complete.")
    refresh_clicked = st.sidebar.button("Refresh data")
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
    dates = [parse_datetime(str(item["published_at"])).date() for item in events]
    start_default, end_default = _default_date_range(dates)
    date_value = st.sidebar.date_input("Date range", (start_default, end_default))
    if isinstance(date_value, tuple) and len(date_value) == 2:
        start, end = date_value
    else:
        st.sidebar.warning("Select a start and end date to apply the range.")
        start, end = start_default, end_default
    if start > end:
        st.sidebar.warning("Start date must be before end date.")
        start, end = start_default, end_default
    categories = sorted({str(item["risk_category"]) for item in events})
    selected_categories = st.sidebar.multiselect(
        "Categories", categories, default=categories
    )
    regions = sorted({str(item["geo_region"]) for item in events})
    selected_regions = st.sidebar.multiselect("Regions", regions, default=regions)
    severity = st.sidebar.slider("Severity", 0.0, 100.0, (0.0, 100.0))
    filtered = filter_events(
        events,
        start=start,
        end=end,
        categories=tuple(selected_categories),
        regions=tuple(selected_regions),
        severity_range=severity,
    )
    status_line = (
        f"Currently displaying {len(filtered)} events across "
        f"{len(selected_categories)} categories in {len(selected_regions)} regions"
    )
    st.sidebar.caption(status_line)
    if filtered:
        kpis = compute_kpis(filtered)
        top_events = sorted(
            filtered,
            key=lambda item: (
                float(item["risk_score_0to100"]),
                float(item["exposure_usd_est"]),
                str(item["published_at"]),
            ),
            reverse=True,
        )[:3]
        pdf_bytes = generate_pdf(
            top_events,
            kpis={
                "Total Active Risk Events": kpis.total_events,
                "High/Critical Events": kpis.high_critical_events,
                "Avg Severity Today": kpis.avg_severity_today,
                "Delta vs Yesterday Avg Severity": kpis.delta_vs_yesterday,
                "Avg Estimated Delay (days)": kpis.avg_delay_days,
                "Total $ Exposure at Risk (Estimated)": kpis.total_exposure_usd,
            },
        )
        if pdf_bytes:
            st.sidebar.download_button(
                "Download PDF report",
                data=pdf_bytes,
                file_name="risk_report.pdf",
                mime="application/pdf",
            )
        else:
            st.sidebar.info("PDF export requires the fpdf package.")
    show_debug = st.sidebar.checkbox("Show debug panel", value=False)
    st.sidebar.caption("Built by Nihal Vootkoor")
    return filtered, show_debug


def render_debug_panel(db_path: Path) -> None:
    """Render debug data in the sidebar."""

    config = get_config()
    seeds_path = config.project_root / "data" / "seeds.csv"
    st.sidebar.subheader("Admin actions")
    if st.sidebar.button("Import Seeds (Backfill)"):
        try:
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

    debug = get_debug_data(DbPaths(db_path, config.db_url))
    st.sidebar.subheader("Pipeline counts")
    st.sidebar.json(debug.counts)
    st.sidebar.subheader("Rejected sample")
    st.sidebar.table(debug.rejections)
