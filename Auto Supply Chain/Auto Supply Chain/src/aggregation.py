"""Aggregation helpers for dashboard views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from src.date_utils import parse_datetime


@dataclass(frozen=True)
class KpiSummary:
    """KPIs for the Command Center."""

    total_events: int
    high_critical_events: int
    avg_severity_today: float
    delta_vs_yesterday: float
    avg_delay_days: float
    total_exposure_usd: float


def _as_float(values: Iterable[float]) -> float:
    """Compute average or return 0."""

    values_list = list(values)
    if not values_list:
        return 0.0
    return sum(values_list) / len(values_list)


def compute_kpis(rows: list[dict[str, object]]) -> KpiSummary:
    """Compute KPI summary from enriched events rows."""

    today = date.today()
    yesterday = today - timedelta(days=1)
    today_scores = []
    yesterday_scores = []
    for row in rows:
        published_at = parse_datetime(str(row["published_at"])).date()
        score = float(row["risk_score_0to100"])
        if published_at == today:
            today_scores.append(score)
        if published_at == yesterday:
            yesterday_scores.append(score)
    avg_today = _as_float(today_scores)
    avg_yesterday = _as_float(yesterday_scores)
    delta = avg_today - avg_yesterday
    high_critical = sum(1 for row in rows if row["severity_band"] in {"High", "Critical"})
    avg_delay = _as_float([float(row["estimated_delay_days"]) for row in rows])
    exposure = sum(float(row["exposure_usd_est"]) for row in rows)
    return KpiSummary(
        total_events=len(rows),
        high_critical_events=high_critical,
        avg_severity_today=round(avg_today, 2),
        delta_vs_yesterday=round(delta, 2),
        avg_delay_days=round(avg_delay, 2),
        total_exposure_usd=round(exposure, 2),
    )


def category_breakdown(rows: list[dict[str, object]]) -> dict[str, int]:
    """Count events by risk category."""

    counts: dict[str, int] = {}
    for row in rows:
        category = str(row["risk_category"])
        counts[category] = counts.get(category, 0) + 1
    return counts


def region_breakdown(rows: list[dict[str, object]]) -> dict[str, int]:
    """Count events by geo region."""

    counts: dict[str, int] = {}
    for row in rows:
        region = str(row["geo_region"])
        counts[region] = counts.get(region, 0) + 1
    return counts
