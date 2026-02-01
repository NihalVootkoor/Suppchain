"""Mitigation generation for top events."""

from __future__ import annotations

from datetime import datetime, timezone

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
    """Attach mitigation guidance to the event."""

    actions = _base_actions(event)
    event.mitigation_description = (
        "Prioritize supply continuity actions and monitor impact signals."
    )
    event.mitigation_actions = actions
    event.mitigation_generated_at = datetime.now(timezone.utc)
    return event
