"""Serialization helpers for database rows."""

from __future__ import annotations

import json

from src.models import EnrichedEvent, RawArticle


def raw_to_row(article: RawArticle) -> dict[str, object]:
    """Serialize RawArticle to a DB row."""

    return {
        "article_id": article.article_id,
        "article_url": article.article_url,
        "source_name": article.source_name,
        "source_weight": article.source_weight,
        "published_at": article.published_at.isoformat(),
        "ingested_at": article.ingested_at.isoformat(),
        "title": article.title,
        "summary": article.summary,
        "content": article.content,
    }


def event_to_row(event: EnrichedEvent) -> dict[str, object]:
    """Serialize EnrichedEvent to a DB row."""

    return {
        "event_id": event.event_id,
        "article_url": event.article_url,
        "source_name": event.source_name,
        "source_weight": event.source_weight,
        "published_at": event.published_at.isoformat(),
        "ingested_at": event.ingested_at.isoformat(),
        "title": event.title,
        "event_summary": event.event_summary,
        "dashboard_blurb": event.dashboard_blurb,
        "reason_flagged": event.reason_flagged,
        "oem_entities": json.dumps(event.oem_entities, ensure_ascii=True),
        "supplier_entities": json.dumps(event.supplier_entities, ensure_ascii=True),
        "component_entities": json.dumps(event.component_entities, ensure_ascii=True),
        "component_criticality": event.component_criticality,
        "risk_category": event.risk_category,
        "disruption_type": event.disruption_type,
        "geo_country": event.geo_country,
        "geo_region": event.geo_region,
        "geo_confidence": event.geo_confidence,
        "impact_1to5": event.impact_1to5,
        "probability_1to5": event.probability_1to5,
        "time_sensitivity_1to3": event.time_sensitivity_1to3,
        "exposure_proxy_1to5": event.exposure_proxy_1to5,
        "severity_confidence": event.severity_confidence,
        "risk_score_0to100": event.risk_score_0to100,
        "severity_band": event.severity_band,
        "estimated_delay_days": event.estimated_delay_days,
        "delay_confidence": event.delay_confidence,
        "delay_rationale": event.delay_rationale,
        "exposure_usd_est": event.exposure_usd_est,
        "exposure_confidence": event.exposure_confidence,
        "exposure_assumptions": event.exposure_assumptions,
        "mitigation_description": event.mitigation_description,
        "mitigation_actions": json.dumps(event.mitigation_actions, ensure_ascii=True) if event.mitigation_actions is not None else None,
        "mitigation_generated_at": event.mitigation_generated_at.isoformat()
        if event.mitigation_generated_at
        else None,
        "llm_validation_passed": int(event.llm_validation_passed),
        "rejected_reason": event.rejected_reason,
        "created_at": event.created_at.isoformat(),
    }
