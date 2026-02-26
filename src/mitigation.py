"""Mitigation generation for top events."""

from __future__ import annotations

from datetime import datetime, timezone

from src.config import get_config
from src.groq_client import generate_mitigation_text
from src.models import EnrichedEvent


def _base_actions(event: EnrichedEvent) -> list[str]:
    """Generate deterministic action list."""

    actions = [
        "Confirm affected suppliers and inventory buffers.",
        "Review alternate routing and logistics options.",
        "Notify procurement and production planning teams.",
    ]
    if event.disruption_type in {"Labor Strike", "Plant Shutdown"}:
        actions.append("Validate contractual clauses and union communications.")
    if event.disruption_type in {"Port Congestion", "Export Restriction"}:
        actions.append("Assess port diversification and customs lead times.")
    if event.disruption_type == "Cyberattack":
        actions.append("Coordinate supplier cybersecurity status checks.")
    return actions


def generate_mitigation(event: EnrichedEvent) -> EnrichedEvent:
    """Attach mitigation guidance to the event. Uses Groq for personalized mitigation when API key is set (e.g. for Top 3); otherwise deterministic playbook."""

    config = get_config()
    if config.groq_api_key:
        result = generate_mitigation_text(
            event_title=event.title,
            event_summary=event.event_summary,
            reason_flagged=event.reason_flagged,
            disruption_type=event.disruption_type,
            geo_country=event.geo_country,
            component_entities=event.component_entities or [],
            api_key=config.groq_api_key,
            model=config.groq_model,
        )
        if result:
            event.mitigation_description = result.get("mitigation_description") or "Prioritize supply continuity and monitor impact signals."
            event.mitigation_actions = result.get("mitigation_actions") or _base_actions(event)
            event.mitigation_generated_at = datetime.now(timezone.utc)
            return event
    actions = _base_actions(event)
    event.mitigation_description = (
        "Prioritize supply continuity actions and monitor impact signals."
    )
    event.mitigation_actions = actions
    event.mitigation_generated_at = datetime.now(timezone.utc)
    return event
