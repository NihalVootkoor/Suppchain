"""Deterministic extraction for RSS-only pipeline."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

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
    get_config,
)
from src.groq_client import classify_disruption_and_risks
from src.models import LLMExtraction, RawArticle
from src.url_utils import hash_id


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


# Canonical aliases Groq sometimes returns instead of the full country name.
_GEO_COUNTRY_ALIASES: dict[str, str] = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "uk": "United Kingdom",
    "eu": "European Union",
    "uae": "United Arab Emirates",
    "south korea": "South Korea",
    "north korea": "North Korea",
    "czech republic": "Czech Republic",
}

# Derive region from normalized country name (built lazily from COUNTRY_MAP).
def _country_to_region(country: str) -> str | None:
    """Return a GEO_REGIONS value for a known country, or None if not mappable."""
    lookup: dict[str, str] = {}
    for _, (_country, _region) in COUNTRY_MAP.items():
        lookup.setdefault(_country.lower(), _region)
    return lookup.get(country.lower())


def _normalize_groq_geo(
    groq_country: str | None,
    groq_region: str | None,
) -> tuple[str, str]:
    """Normalize Groq's geo outputs to canonical country + region strings."""
    country = str(groq_country or "").strip()
    region = str(groq_region or "").strip()

    # Resolve aliases
    alias = _GEO_COUNTRY_ALIASES.get(country.lower())
    if alias:
        country = alias

    # Reject sentinel strings
    if country.lower() in ("null", "none", "unknown", "n/a", ""):
        country = "Unknown"

    # If region is missing/invalid but country is known, derive from COUNTRY_MAP
    if (not region or region.lower() in ("null", "none", "unknown", "n/a", "")) and country != "Unknown":
        derived = _country_to_region(country)
        region = derived if derived else "Unknown"

    if region not in GEO_REGIONS:
        region = "Unknown"

    return country, region


def _find_entities(text: str, vocab: list[str], limit: int = 12) -> list[str]:
    t = _norm(text)
    hits: list[str] = []
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
    out: list[str] = []
    for hit in hits:
        key = hit.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(hit)
        if len(out) >= limit:
            break
    return out


def _extract_geo(text: str) -> tuple[str, str, str]:
    t = _norm(text)
    # Count occurrences of each country key to pick the most-mentioned one,
    # avoiding the first-match-wins problem on multi-country articles.
    country_counts: dict[tuple[str, str], int] = {}
    for key, (country, region) in COUNTRY_MAP.items():
        count = len(re.findall(rf"\b{re.escape(key)}\b", t))
        if count > 0:
            pair = (country, region)
            country_counts[pair] = country_counts.get(pair, 0) + count

    if not country_counts:
        return ("Unknown", "Unknown", "Low")

    country, region = max(country_counts, key=lambda k: country_counts[k])
    geo_conf = "Medium"
    for trig in DISRUPTION_TRIGGERS:
        if not trig:
            continue
        if re.search(rf"{re.escape(trig)}.{{0,40}}\b{re.escape(country.lower())}\b", t):
            geo_conf = "High"
            break
        if re.search(rf"\b{re.escape(country.lower())}\b.{{0,40}}{re.escape(trig)}", t):
            geo_conf = "High"
            break

    return (country, region if region in GEO_REGIONS else "Unknown", geo_conf)


def _classify_disruption_type(text: str) -> str:
    """Keyword-based fallback classifier. Used when no Groq API key is set."""
    t = _norm(text)
    # Most specific / least ambiguous types first to avoid false-positive matches.
    rules = [
        ("Cyberattack", ["ransomware", "cyberattack", "cyber attack", "data breach", "it outage", "hack"]),
        ("Natural Disaster", ["earthquake", "flood", "wildfire", "hurricane", "typhoon", "storm", "tornado", "tsunami", "drought"]),
        ("Labor Strike", ["strike", "walkout", "labor dispute", "industrial action", "worker stoppage"]),
        # Trade/tariffs — broader name reflects reality (tariffs ≠ pure export ban)
        ("Trade Restriction", ["export ban", "export restriction", "import ban", "sanctions", "tariff", "trade war", "import duty", "trade restriction"]),
        # Insolvency — tightened: "restructuring" alone is too broad
        ("Supplier Insolvency", ["bankruptcy", "insolvency", "chapter 11", "liquidation", "creditor"]),
        # Regulatory — tightened: bare "regulation"/"law" fire on too many unrelated articles
        ("Regulatory Change", ["new regulation", "regulatory change", "compliance mandate", "rule change", "legislation", "new law", "government mandate"]),
        # Capacity cuts — new type for production/output reductions that aren't full shutdowns
        ("Capacity Constraint", ["production cut", "capacity cut", "output reduction", "capacity reduction", "idle capacity", "production curtail", "volume reduction"]),
        # Plant Shutdown — require plant/factory/facility context to avoid bare "shutdown" misfires
        ("Plant Shutdown", ["plant shutdown", "factory shutdown", "facility shutdown", "plant closure", "factory closure", "halted production", "production halted", "production halt"]),
        # Logistics — broader than port congestion: covers rail, trucking, air freight
        ("Logistics Disruption", ["port congestion", "shipping delay", "container backlog", "intermodal backlog", "freight delay", "rail disruption", "trucking shortage", "port closure", "shipping disruption"]),
    ]
    for label, kws in rules:
        if any(kw in t for kw in kws):
            return label
    return "Other"


def _classify_sc_category(disruption_type: str) -> str:
    """Map disruption_type to a traditional supply chain risk category."""
    mapping = {
        "Labor Strike": "Labor & Social",
        "Plant Shutdown": "Supply Disruption",
        "Logistics Disruption": "Logistics & Transport",
        "Trade Restriction": "Geopolitical & Trade",
        "Cyberattack": "Cyber & Technology",
        "Natural Disaster": "Natural Disaster & Climate",
        "Supplier Insolvency": "Supply Disruption",
        "Regulatory Change": "Regulatory & Compliance",
        "Capacity Constraint": "Supply Disruption",
        "Other": "Supply Disruption",
    }
    return mapping.get(disruption_type, "Supply Disruption")


def _severity_signals(text: str, disruption_type: str) -> dict[str, Any]:
    t = _norm(text)

    # Impact: severity of the production/supply effect
    if any(k in t for k in ["force majeure", "halted production", "production halted", "plant shutdown", "shutdown"]):
        impact = 5
    elif any(k in t for k in ["output cut", "capacity reduced", "major delays", "stoppage"]):
        impact = 4
    elif any(k in t for k in ["delay", "disruption", "shortage", "congestion"]):
        impact = 3
    else:
        impact = 2

    # Probability: confidence the disruption is real and happening.
    # Start at 2 (reported); escalate only with firm confirmation language.
    probability = 2
    if any(k in t for k in ["confirmed", "announced", "began", "started", "is underway", "has been", "officially"]):
        probability = 3
    if any(k in t for k in ["production halted", "plant shutdown", "shutdown", "force majeure", "halted"]):
        probability = 4
    if any(k in t for k in ["effective immediately", "in effect", "has taken effect", "completely halted"]):
        probability = 5
    if any(k in t for k in ["could", "may", "might", "reportedly", "rumor", "possible", "potential"]):
        probability = max(1, probability - 1)

    # Time sensitivity: urgency of the impact window.
    # Use disruption type as a baseline; only escalate to 3 with explicit urgency signals.
    if disruption_type in ["Trade Restriction", "Regulatory Change", "Capacity Constraint"]:
        # Policy changes and gradual capacity reductions are slow-moving by nature
        time_sens = 1
        if any(k in t for k in ["effective immediately", "in effect", "has taken effect"]):
            time_sens = 2
    elif disruption_type == "Supplier Insolvency":
        time_sens = 2
    elif disruption_type in ["Cyberattack", "Natural Disaster", "Labor Strike", "Plant Shutdown", "Logistics Disruption"]:
        # These can be urgent, but only if confirmed as active/immediate
        time_sens = 2
        if any(k in t for k in ["immediately", "now", "today", "this week", "ongoing", "active", "outage", "halted", "in effect"]):
            time_sens = 3
    else:
        # "Other" — conservative default
        time_sens = 1
        if any(k in t for k in ["immediately", "now", "today", "this week", "halted", "outage"]):
            time_sens = 2

    # Exposure: concentration/criticality of supply dependency
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


def _estimate_delay_days(text: str, disruption_type: str) -> tuple[int, str, str]:
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
        "Logistics Disruption": 14,
        "Labor Strike": 10,
        "Cyberattack": 7,
        "Natural Disaster": 21,
        "Plant Shutdown": 14,
        "Supplier Insolvency": 60,
        "Trade Restriction": 90,
        "Regulatory Change": 120,
        "Capacity Constraint": 30,
        "Other": 14,
    }
    return (
        int(defaults.get(disruption_type, 14)),
        "Low",
        f"Default delay for disruption_type={disruption_type}",
    )


def _component_criticality(components: list[str]) -> str:
    joined = " ".join(c.lower() for c in components)
    if any(k in joined for k in ["semiconductor", "chip", "ecu", "battery", "lithium", "cathode", "anode"]):
        return "high"
    if any(k in joined for k in ["wiring harness", "steel", "aluminum", "motor"]):
        return "medium"
    return "low"


def _should_reject_as_not_event(text: str) -> str | None:
    t = _norm(text)
    if any(k in t for k in NEGATIVE_KEYWORDS):
        return "Consumer/review content"
    if not any(trig in t for trig in DISRUPTION_TRIGGERS):
        return "No disruption trigger found"
    return None


def _make_summary(
    disruption_type: str,
    geo_country: str,
    oems: list[str],
    suppliers: list[str],
    components: list[str],
    delay_days: int,
) -> str:
    parts: list[str] = []
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
            "risk_category": "Supply Disruption",
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

    # Keyword classifier provides a fast fallback (used when no API key is set).
    keyword_disruption = _classify_disruption_type(text)
    if keyword_disruption not in DISRUPTION_TYPES:
        keyword_disruption = "Other"
    disruption_type = keyword_disruption

    risks_identified: str | None = None
    groq_geo_country: str | None = None
    groq_geo_region: str | None = None
    groq_severity: dict | None = None

    # Groq is the primary classifier — called for every event when a key is available.
    # Keywords are only the fallback, not the source of truth.
    config = get_config()
    if config.groq_api_key:
        groq_result = classify_disruption_and_risks(
            text, config.groq_api_key, model=config.groq_model
        )
        if groq_result is None:
            import sys
            print("[groq] WARNING: Groq returned None — falling back to keyword extraction", flush=True, file=sys.stderr)
        else:
            # Relevance gate: reject events Groq deems not automotive SC risks.
            if not groq_result.get("is_automotive_sc_risk", True):
                payload = {
                    "event_summary": article.summary or article.title or "Rejected event",
                    "reason_flagged": "Not an automotive supply chain risk",
                    "geo_country": "Unknown",
                    "geo_region": "Unknown",
                    "geo_confidence": "Low",
                    "risk_category": "Supply Disruption",
                    "disruption_type": "Other",
                    "impact_1to5": 1,
                    "probability_1to5": 1,
                    "time_sensitivity_1to3": 1,
                    "exposure_proxy_1to5": 1,
                    "severity_confidence": "Low",
                    "estimated_delay_days": 0,
                    "delay_confidence": "Low",
                    "delay_rationale": "Rejected: not an automotive supply chain risk",
                    "oem_entities": [],
                    "supplier_entities": [],
                    "component_entities": [],
                    "component_criticality": "low",
                    "llm_validation_passed": False,
                    "rejected_reason": "Not an automotive supply chain risk",
                }
                return LLMExtraction(**payload)

            groq_disruption = groq_result.get("disruption_type") or keyword_disruption
            if groq_disruption not in DISRUPTION_TYPES:
                groq_disruption = keyword_disruption
            disruption_type = groq_disruption
            risks_identified = groq_result.get("risks_identified") or None
            groq_geo_country = groq_result.get("geo_country")
            groq_geo_region = groq_result.get("geo_region")

            # Capture Groq severity scores if all four fields were returned.
            _gs = {
                k: groq_result.get(k)
                for k in ("impact_1to5", "probability_1to5", "time_sensitivity_1to3", "exposure_proxy_1to5")
            }
            if all(v is not None for v in _gs.values()):
                groq_severity = _gs

    # Always derive risk_category deterministically from disruption_type for consistency.
    risk_category = _classify_sc_category(disruption_type)
    if risk_category not in RISK_CATEGORIES:
        risk_category = "Supply Disruption"

    # Geo resolution: Groq is primary (richer inference), keywords are fallback.
    kw_country, kw_region, kw_conf = _extract_geo(text)

    # Normalize Groq's geo output (handles aliases like "US", missing regions, sentinels).
    norm_groq_country, norm_groq_region = _normalize_groq_geo(groq_geo_country, groq_geo_region)

    if norm_groq_country != "Unknown":
        geo_country = norm_groq_country
        # Groq confidence is High when it agrees with keyword extraction, Medium otherwise.
        geo_conf = "High" if kw_country != "Unknown" and kw_country.lower() in norm_groq_country.lower() else "Medium"
    else:
        geo_country = kw_country
        geo_conf = kw_conf

    if norm_groq_region != "Unknown":
        geo_region = norm_groq_region
    elif kw_region != "Unknown":
        geo_region = kw_region
    else:
        # Last resort: derive from resolved country
        derived = _country_to_region(geo_country)
        geo_region = derived if derived else "Unknown"
    oems = _find_entities(text, OEMS)
    suppliers = _find_entities(text, TIER1S)
    components = _find_entities(text, AUTO_TERMS)
    component_criticality = _component_criticality(components)
    # Severity: Groq scores are used when available; keyword heuristics are the fallback.
    kw_severity = _severity_signals(text, disruption_type)
    if groq_severity is not None:
        severity = groq_severity
        severity_conf = "High"  # Groq-scored events always get High severity confidence.
    else:
        severity = kw_severity
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
    delay_days, delay_conf, delay_rat = _estimate_delay_days(text, disruption_type)

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
        "risks_identified": risks_identified,
    }
    return LLMExtraction(**payload)


def extract_with_llm(article: RawArticle) -> LLMExtraction:
    """Return schema-validated extraction for an article."""
    return extract_structured_event(article)


def build_event_id(article_url: str, published_at: datetime) -> str:
    """Build a stable event id from URL and date."""
    return f"{hash_id(article_url)}-{published_at:%Y%m%d}"
