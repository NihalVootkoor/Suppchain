"""Mitigation generation for top events."""

from __future__ import annotations

from datetime import datetime, timezone

from src.config import get_config
from src.groq_client import generate_mitigation_text
from src.models import EnrichedEvent

_FALLBACK_DESCRIPTIONS: dict[str, str] = {
    "Labor Strike": "Monitor union negotiations closely and assess second-source capacity for all affected components.",
    "Plant Shutdown": "Confirm shutdown scope and restart timeline with the supplier; activate emergency alternate sourcing immediately.",
    "Logistics Disruption": "Re-route critical shipments and accelerate safety stock replenishment at key distribution centers.",
    "Trade Restriction": "Engage trade counsel to quantify tariff or sanction exposure and identify compliant alternate sourcing.",
    "Capacity Constraint": "Confirm output reduction scope with the supplier and activate alternate sourcing for the highest-criticality parts.",
    "Cyberattack": "Isolate affected supply nodes, validate data integrity, and activate business continuity protocols before resuming procurement.",
    "Natural Disaster": "Assess supplier facility damage and estimated recovery timeline; activate emergency inventory buffers immediately.",
    "Supplier Insolvency": "Confirm insolvency status with legal counsel and initiate urgent dual-sourcing for all critical parts.",
    "Regulatory Change": "Engage compliance and legal teams to assess applicability, then plan a phased supply chain transition.",
}
_DEFAULT_FALLBACK_DESC = "Prioritize supply continuity actions and monitor all impact signals closely."


def _base_actions(event: EnrichedEvent) -> list[str]:
    """Generate deterministic action list aligned to Immediate / Near-Term / Strategic tiers."""
    dtype = event.disruption_type

    _PLAYBOOK: dict[str, list[str]] = {
        "Labor Strike": [
            "Validate union negotiations and identify all affected supplier sites immediately.",
            "Activate alternate component sourcing outside the affected region.",
            "Review and strengthen multi-source contracts to reduce single-supplier dependency.",
            "Open direct communication with union representatives to monitor resolution timeline.",
        ],
        "Plant Shutdown": [
            "Confirm shutdown scope and estimated restart timeline with the supplier.",
            "Activate emergency alternate sourcing protocols for all critical parts.",
            "Adjust production schedules and alert downstream OEM partners.",
            "Engage union contacts and local management to assess restart conditions.",
        ],
        "Logistics Disruption": [
            "Re-route shipments through alternate ports or intermodal channels now.",
            "Accelerate customs clearance for all critical in-transit inventory.",
            "Increase safety stock levels at key regional distribution centers.",
            "Qualify alternate freight forwarders and regional sourcing hubs to reduce choke-point dependency.",
        ],
        "Trade Restriction": [
            "Engage trade counsel to assess full sanction or tariff exposure.",
            "Identify alternate country-of-origin sourcing for restricted materials.",
            "Initiate compliance review of all affected SKUs and HS codes.",
            "Model tariff impact on landed cost and update supplier contracts accordingly.",
        ],
        "Capacity Constraint": [
            "Confirm output reduction scope and estimated recovery timeline with supplier.",
            "Activate alternate sourcing and increase safety stock for affected components.",
            "Adjust production schedules and notify downstream OEM planning teams.",
        ],
        "Cyberattack": [
            "Conduct immediate cybersecurity status check with all affected suppliers.",
            "Isolate affected supply nodes and activate business continuity protocols.",
            "Assess data integrity and switch to manual backup processes where needed.",
            "Mandate supplier cybersecurity attestation and verify EDI/API channel integrity before resuming orders.",
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
    return _PLAYBOOK.get(dtype, [
        "Confirm affected suppliers and assess inventory buffer levels.",
        "Review alternate routing and logistics contingency options.",
        "Notify procurement and production planning teams immediately.",
    ])


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
            risk_score=float(event.risk_score_0to100 or 0),
            exposure_usd_est=float(event.exposure_usd_est or 0),
            estimated_delay_days=int(event.estimated_delay_days or 0),
            severity_band=str(event.severity_band or "Medium"),
        )
        if result:
            event.mitigation_description = result.get("mitigation_description") or _FALLBACK_DESCRIPTIONS.get(event.disruption_type, _DEFAULT_FALLBACK_DESC)
            event.mitigation_actions = result.get("mitigation_actions") or _base_actions(event)
            event.mitigation_generated_at = datetime.now(timezone.utc)
            return event
    event.mitigation_description = _FALLBACK_DESCRIPTIONS.get(event.disruption_type, _DEFAULT_FALLBACK_DESC)
    event.mitigation_actions = _base_actions(event)
    event.mitigation_generated_at = datetime.now(timezone.utc)
    return event
