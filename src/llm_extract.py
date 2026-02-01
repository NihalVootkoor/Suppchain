"""Deterministic extraction for RSS-only pipeline."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.config import (
    AUTO_TERMS,
    COUNTRY_MAP,
    DISRUPTION_TRIGGERS,
    DISRUPTION_TYPES,
    GEO_REGIONS,
    NEGATIVE_KEYWORDS,
    OEMS,
    RISK_CATEGORIES,
    TIER1S,
)
from src.models import LLMExtraction, RawArticle
from src.url_utils import hash_id


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def _find_entities(text: str, vocab: List[str], limit: int = 12) -> List[str]:
    t = _norm(text)
    hits: List[str] = []
    for token in vocab:
        value = token.lower().strip()
        if not value:
            continue
        if len(value) <= 3:
            if re.search(rf"\b{re.escape(value)}\b", t):
                hits.append(token)
        else:
            if value in t:
                hits.append(token)
    seen = set()
    out: List[str] = []
    for hit in hits:
        key = hit.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(hit)
        if len(out) >= limit:
            break
    return out


def _extract_geo(text: str) -> Tuple[str, str, str]:
    t = _norm(text)
    matches: List[Tuple[str, str]] = []
    for key, (country, region) in COUNTRY_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", t):
            matches.append((country, region))

    if not matches:
        return ("Unknown", "Unknown", "Low")

    country, region = matches[0]
    geo_conf = "Medium"
    for trig in DISRUPTION_TRIGGERS[:40]:
        if not trig:
            continue
        if re.search(rf"{re.escape(trig)}.{0,40}\b{re.escape(country.lower())}\b", t):
            geo_conf = "High"
            break
        if re.search(rf"\b{re.escape(country.lower())}\b.{0,40}{re.escape(trig)}", t):
            geo_conf = "High"
            break

    return (country, region if region in GEO_REGIONS else "Unknown", geo_conf)


def _classify_disruption_type(text: str) -> str:
    t = _norm(text)
    rules = [
        ("Cyberattack", ["ransomware", "cyberattack", "hack", "outage"]),
        ("Labor Strike", ["strike", "walkout", "labor dispute", "union"]),
        ("Port Congestion", ["port congestion", "congestion", "container backlog", "intermodal backlog"]),
        ("Export Restriction", ["export ban", "export restriction", "sanctions", "tariff"]),
        ("Natural Disaster", ["earthquake", "flood", "wildfire", "hurricane", "typhoon", "storm"]),
        ("Supplier Insolvency", ["bankruptcy", "insolvency", "creditor", "restructuring"]),
        ("Plant Shutdown", ["shutdown", "shut down", "halted production", "halt production", "stoppage"]),
        ("Regulatory Change", ["regulation", "regulatory", "compliance", "rule change", "law"]),
    ]
    for label, kws in rules:
        if any(kw in t for kw in kws):
            return label
    return "Other"


def _classify_pestel(disruption_type: str) -> str:
    mapping = {
        "Labor Strike": "Social",
        "Cyberattack": "Technological",
        "Natural Disaster": "Environmental",
        "Export Restriction": "Political",
        "Regulatory Change": "Legal",
        "Supplier Insolvency": "Economic",
        "Port Congestion": "Operational",
        "Plant Shutdown": "Operational",
        "Other": "Operational",
    }
    return mapping.get(disruption_type, "Operational")


def _severity_signals(text: str, disruption_type: str) -> Dict[str, Any]:
    t = _norm(text)
    impact = 3
    if any(k in t for k in ["force majeure", "halted production", "production halted", "plant shutdown", "shutdown"]):
        impact = 5
    elif any(k in t for k in ["output cut", "capacity reduced", "major delays", "stoppage"]):
        impact = 4
    elif any(k in t for k in ["delay", "disruption", "shortage", "congestion"]):
        impact = 3
    else:
        impact = 2

    probability = 3
    if any(k in t for k in ["confirmed", "announced", "began", "started", "is underway", "has been"]):
        probability = 4
    if any(k in t for k in ["effective immediately", "in effect", "has taken effect"]):
        probability = 5
    if any(k in t for k in ["could", "may", "might", "reportedly", "rumor"]):
        probability = max(2, probability - 1)

    time_sens = 2
    if any(k in t for k in ["immediately", "now", "today", "this week", "halted", "shutdown", "outage"]):
        time_sens = 3
    if disruption_type in ["Export Restriction", "Regulatory Change"]:
        time_sens = 1
    if disruption_type in [
        "Cyberattack",
        "Labor Strike",
        "Port Congestion",
        "Plant Shutdown",
        "Natural Disaster",
    ]:
        time_sens = 3

    exposure_proxy = 3
    if any(k in t for k in ["single-source", "sole supplier", "only supplier", "exclusive"]):
        exposure_proxy = 5
    elif any(k in t for k in ["key supplier", "critical supplier", "major supplier", "primary supplier"]):
        exposure_proxy = 4
    elif any(k in t for k in ["supplier", "suppliers", "parts maker"]):
        exposure_proxy = 3
    else:
        exposure_proxy = 2

    return {
        "impact_1to5": int(impact),
        "probability_1to5": int(probability),
        "time_sensitivity_1to3": int(time_sens),
        "exposure_proxy_1to5": int(exposure_proxy),
    }


def _estimate_delay_days(text: str, disruption_type: str) -> Tuple[int, str, str]:
    t = _norm(text)
    match = re.search(r"\b(\d{1,3})\s*(day|days|week|weeks|month|months)\b", t)
    if match:
        n = int(match.group(1))
        unit = match.group(2)
        days = n
        if "week" in unit:
            days = n * 7
        elif "month" in unit:
            days = n * 30
        return (days, "High", f"Extracted explicit duration: {match.group(0)}")

    defaults = {
        "Port Congestion": 14,
        "Labor Strike": 10,
        "Cyberattack": 7,
        "Natural Disaster": 21,
        "Plant Shutdown": 14,
        "Supplier Insolvency": 60,
        "Export Restriction": 90,
        "Regulatory Change": 120,
        "Other": 14,
    }
    return (
        int(defaults.get(disruption_type, 14)),
        "Low",
        f"Default delay for disruption_type={disruption_type}",
    )


def _component_criticality(components: List[str]) -> str:
    joined = " ".join([c.lower() for c in components])
    if any(k in joined for k in ["semiconductor", "chip", "ecu", "battery", "lithium", "cathode", "anode"]):
        return "high"
    if any(k in joined for k in ["wiring harness", "steel", "aluminum", "motor"]):
        return "medium"
    return "low"


def _should_reject_as_not_event(text: str) -> Optional[str]:
    t = _norm(text)
    if any(k in t for k in NEGATIVE_KEYWORDS):
        return "Consumer/review content"
    if not any(trig in t for trig in DISRUPTION_TRIGGERS):
        return "No disruption trigger found"
    return None


def _make_summary(
    disruption_type: str,
    geo_country: str,
    oems: List[str],
    suppliers: List[str],
    components: List[str],
    delay_days: int,
) -> str:
    parts: List[str] = []
    if disruption_type != "Other":
        parts.append(disruption_type)
    if geo_country and geo_country != "Unknown":
        parts.append(f"in {geo_country}")
    if oems:
        parts.append(f"affecting OEMs like {', '.join(oems[:2])}")
    if suppliers and not oems:
        parts.append(f"involving suppliers like {', '.join(suppliers[:2])}")
    if components:
        parts.append(f"with potential impact to {components[0]}")
    summary = " ".join(parts).strip()
    if not summary:
        summary = "Potential supply chain disruption identified from curated RSS."
    return f"{summary}. Estimated disruption duration ~{delay_days} days (est.)."


def extract_structured_event(article: RawArticle) -> LLMExtraction:
    text = f"{article.title} {article.summary} {article.content}"
    reject_reason = _should_reject_as_not_event(text)
    if reject_reason:
        payload = {
            "event_summary": article.summary or article.title or "Rejected event",
            "reason_flagged": reject_reason,
            "geo_country": "Unknown",
            "geo_region": "Unknown",
            "geo_confidence": "Low",
            "risk_category": "Operational",
            "disruption_type": "Other",
            "impact_1to5": 1,
            "probability_1to5": 1,
            "time_sensitivity_1to3": 1,
            "exposure_proxy_1to5": 1,
            "severity_confidence": "Low",
            "estimated_delay_days": 0,
            "delay_confidence": "Low",
            "delay_rationale": reject_reason,
        "oem_entities": [],
        "supplier_entities": [],
        "component_entities": [],
        "component_criticality": "low",
            "llm_validation_passed": False,
            "rejected_reason": reject_reason,
        }
        return LLMExtraction(**payload)

    disruption_type = _classify_disruption_type(text)
    if disruption_type not in DISRUPTION_TYPES:
        disruption_type = "Other"
    risk_category = _classify_pestel(disruption_type)
    if risk_category not in RISK_CATEGORIES:
        risk_category = "Operational"

    geo_country, geo_region, geo_conf = _extract_geo(text)
    oems = _find_entities(text, OEMS)
    suppliers = _find_entities(text, TIER1S)
    components = _find_entities(text, AUTO_TERMS)
    component_criticality = _component_criticality(components)
    severity = _severity_signals(text, disruption_type)
    delay_days, delay_conf, delay_rat = _estimate_delay_days(text, disruption_type)

    evidence_points = 0
    evidence_points += 1 if disruption_type != "Other" else 0
    evidence_points += 1 if len(oems) + len(suppliers) > 0 else 0
    evidence_points += 1 if geo_country != "Unknown" else 0
    evidence_points += 1 if any(trig in _norm(text) for trig in DISRUPTION_TRIGGERS) else 0
    if evidence_points >= 4:
        severity_conf = "High"
    elif evidence_points <= 2:
        severity_conf = "Low"
    else:
        severity_conf = "Medium"

    reason_flagged = f"Matched disruption={disruption_type}; category={risk_category}; geo={geo_country} ({geo_conf})"
    event_summary = _make_summary(
        disruption_type,
        geo_country,
        oems,
        suppliers,
        components,
        delay_days,
    )

    payload = {
        "llm_validation_passed": True,
        "rejected_reason": None,
        "event_summary": event_summary,
        "reason_flagged": reason_flagged,
        "risk_category": risk_category,
        "disruption_type": disruption_type,
        "geo_country": geo_country,
        "geo_region": geo_region,
        "geo_confidence": geo_conf,
        "oem_entities": oems,
        "supplier_entities": suppliers,
        "component_entities": components[:8],
        "component_criticality": component_criticality,
        **severity,
        "severity_confidence": severity_conf,
        "estimated_delay_days": int(delay_days),
        "delay_confidence": delay_conf,
        "delay_rationale": delay_rat,
    }
    return LLMExtraction(**payload)


def extract_with_llm(article: RawArticle) -> LLMExtraction:
    """Return schema-validated extraction for an article."""
    return extract_structured_event(article)


def build_event_id(article_url: str, published_at: datetime) -> str:
    """Build a stable event id from URL and date."""
    return f"{hash_id(article_url)}-{published_at:%Y%m%d}"
