"""Deterministic scoring and enrichment."""

from __future__ import annotations

from datetime import datetime, timezone

from src.llm_extract import build_event_id
from src.models import EnrichedEvent, LLMExtraction, RawArticle


def compute_risk_score(extraction: LLMExtraction) -> float:
    """Compute a 0-100 risk score from severity signals.

    Weights: impact 40%, probability 30%, time_sensitivity 15%, exposure 15%.
    Each factor is normalized to its max so the sum is always in [0, 100].
    """
    impact_score   = (extraction.impact_1to5 / 5) * 40
    prob_score     = (extraction.probability_1to5 / 5) * 30
    time_score     = (extraction.time_sensitivity_1to3 / 3) * 15
    exposure_score = (extraction.exposure_proxy_1to5 / 5) * 15
    score = impact_score + prob_score + time_score + exposure_score
    return round(max(0.0, min(100.0, score)), 2)


def severity_band(score: float) -> str:
    """Map a score to a severity band."""

    if score >= 85:
        return "Critical"
    if score >= 70:
        return "High"
    if score >= 45:
        return "Medium"
    return "Low"


def estimate_exposure_usd(extraction: LLMExtraction) -> float:
    """Estimate exposure using a delay-buffer based model."""

    daily_exposure = 1_000_000 * (extraction.exposure_proxy_1to5 / 5)
    buffer_days = max(1, 7 - (extraction.time_sensitivity_1to3 * 2))
    effective_delay = max(1, extraction.estimated_delay_days - buffer_days)
    return round(daily_exposure * effective_delay, 2)


def build_enriched_event(article: RawArticle, extraction: LLMExtraction) -> EnrichedEvent:
    """Combine raw article + extraction into an enriched event."""

    risk_score = compute_risk_score(extraction)
    exposure_est = estimate_exposure_usd(extraction)
    now = datetime.now(timezone.utc)
    return EnrichedEvent(
        event_id=build_event_id(article.article_url, article.published_at),
        article_url=article.article_url,
        source_name=article.source_name,
        source_weight=article.source_weight,
        published_at=article.published_at,
        ingested_at=article.ingested_at,
        title=article.title,
        event_summary=extraction.event_summary,
        dashboard_blurb=extraction.risks_identified if getattr(extraction, "risks_identified", None) else None,
        reason_flagged=extraction.reason_flagged,
        oem_entities=extraction.oem_entities,
        supplier_entities=extraction.supplier_entities,
        component_entities=extraction.component_entities,
        component_criticality=extraction.component_criticality,
        risk_category=extraction.risk_category,
        disruption_type=extraction.disruption_type,
        geo_country=extraction.geo_country,
        geo_region=extraction.geo_region,
        geo_confidence=extraction.geo_confidence,
        impact_1to5=extraction.impact_1to5,
        probability_1to5=extraction.probability_1to5,
        time_sensitivity_1to3=extraction.time_sensitivity_1to3,
        exposure_proxy_1to5=extraction.exposure_proxy_1to5,
        severity_confidence=extraction.severity_confidence,
        risk_score_0to100=risk_score,
        severity_band=severity_band(risk_score),
        estimated_delay_days=extraction.estimated_delay_days,
        delay_confidence=extraction.delay_confidence,
        delay_rationale=extraction.delay_rationale,
        exposure_usd_est=exposure_est,
        exposure_confidence=extraction.severity_confidence,
        exposure_assumptions="Based on proxy model.",
        mitigation_description=None,
        mitigation_actions=None,
        mitigation_generated_at=None,
        llm_validation_passed=extraction.llm_validation_passed,
        rejected_reason=extraction.rejected_reason,
        created_at=now,
    )
