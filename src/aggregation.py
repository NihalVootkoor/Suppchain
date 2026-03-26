"""Aggregation helpers for dashboard views."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from src.date_utils import parse_datetime


@dataclass(frozen=True)
class KpiSummary:
    """KPIs for the Command Center."""

    total_events: int
    high_critical_events: int
    avg_severity_today: float   # 7-day rolling average of risk scores
    delta_vs_yesterday: float   # delta between current 7d avg and prior 7d avg
    avg_delay_days: float
    total_exposure_usd: float
    events_this_week: int       # events published in the last 7 days
    events_last_week: int       # events published in the 7 days before that
    highest_risk_region: str    # region with highest avg risk score (excluding Unknown)
    highest_risk_component: str  # component entity with highest avg risk score


def _as_float(values: Iterable[float]) -> float:
    """Compute average or return 0."""

    values_list = list(values)
    if not values_list:
        return 0.0
    return sum(values_list) / len(values_list)


def compute_kpis(rows: list[dict[str, object]]) -> KpiSummary:
    """Compute KPI summary from enriched events rows.

    Severity metrics use 7-day rolling windows to smooth daily noise:
    - avg_severity_today: mean risk score over the current 7-day window
    - delta_vs_yesterday: difference between current 7d avg and prior 7d avg
    """

    today = date.today()
    # Current window: last 7 days (today inclusive)
    week_start = today - timedelta(days=6)
    # Prior window: 7 days before that
    prior_end = today - timedelta(days=7)
    prior_start = today - timedelta(days=13)

    current_7d_scores: list[float] = []
    prior_7d_scores: list[float] = []
    events_this_week = 0
    events_last_week = 0

    for row in rows:
        published_at = parse_datetime(str(row["published_at"])).date()
        score = float(row["risk_score_0to100"])
        if week_start <= published_at <= today:
            current_7d_scores.append(score)
            events_this_week += 1
        if prior_start <= published_at <= prior_end:
            prior_7d_scores.append(score)
            events_last_week += 1

    avg_current_7d = _as_float(current_7d_scores)
    avg_prior_7d = _as_float(prior_7d_scores)
    delta = avg_current_7d - avg_prior_7d

    high_critical = sum(1 for row in rows if row["severity_band"] in {"High", "Critical"})
    avg_delay = _as_float([float(row["estimated_delay_days"]) for row in rows])
    exposure = sum(float(row["exposure_usd_est"]) for row in rows)

    # Highest risk region by average score, excluding Unknown/empty.
    # Require at least 3 events to avoid a single outlier dominating.
    region_scores: dict[str, list[float]] = {}
    for row in rows:
        region = str(row.get("geo_region") or "").strip()
        if region and region.lower() not in ("unknown", "n/a", ""):
            region_scores.setdefault(region, []).append(float(row["risk_score_0to100"]))
    qualified = {r: scores for r, scores in region_scores.items() if len(scores) >= 3}
    candidates = qualified if qualified else region_scores
    highest_risk_region = (
        max(candidates, key=lambda r: sum(candidates[r]) / len(candidates[r]))
        if candidates else ""
    )

    # Highest risk component: weighted avg score per component, factoring in
    # component_criticality (high=1.3×, medium=1.0×, low=0.8×) so a critical
    # component with a slightly lower score still outranks a low-criticality one.
    # No minimum-appearance threshold — component coverage is sparse by nature.
    _CRIT_WEIGHT = {"high": 1.3, "medium": 1.0, "low": 0.8}
    component_scores: dict[str, list[float]] = {}
    for row in rows:
        raw = row.get("component_entities") or "[]"
        try:
            components: list[str] = json.loads(raw) if isinstance(raw, str) else list(raw)
        except (json.JSONDecodeError, TypeError):
            components = []
        score = float(row["risk_score_0to100"])
        crit = str(row.get("component_criticality") or "low").lower()
        weight = _CRIT_WEIGHT.get(crit, 1.0)
        for comp in components:
            comp = comp.strip()
            if comp:
                component_scores.setdefault(comp, []).append(score * weight)
    highest_risk_component = (
        max(component_scores, key=lambda c: sum(component_scores[c]) / len(component_scores[c]))
        if component_scores else ""
    )

    return KpiSummary(
        total_events=len(rows),
        high_critical_events=high_critical,
        avg_severity_today=round(avg_current_7d, 2),
        delta_vs_yesterday=round(delta, 2),
        avg_delay_days=round(avg_delay, 2),
        total_exposure_usd=round(exposure, 2),
        events_this_week=events_this_week,
        events_last_week=events_last_week,
        highest_risk_region=highest_risk_region,
        highest_risk_component=highest_risk_component,
    )


def category_breakdown(rows: list[dict[str, object]]) -> dict[str, int]:
    """Count events by risk category."""

    counts: dict[str, int] = {}
    for row in rows:
        category = str(row["risk_category"])
        counts[category] = counts.get(category, 0) + 1
    return counts


