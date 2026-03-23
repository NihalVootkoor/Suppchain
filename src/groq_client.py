"""Groq LLM client for event classification and personalized mitigation."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from src.config import DISRUPTION_TYPES, GEO_REGIONS

_DISRUPTION_LIST = ", ".join(DISRUPTION_TYPES)
_GEO_REGION_LIST = ", ".join(r for r in GEO_REGIONS if r != "Unknown")


def _get_client(api_key: Optional[str]) -> Optional[Any]:
    if not api_key:
        return None
    try:
        from groq import Groq
        return Groq(api_key=api_key)
    except Exception:
        return None


def _strip_fences(raw: str) -> str:
    """Strip markdown code fences that LLMs sometimes wrap JSON in."""
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    return re.sub(r"\s*```\s*$", "", raw)


def classify_disruption_and_risks(
    text: str,
    api_key: Optional[str],
    model: str = "llama-3.3-70b-versatile",
) -> Optional[dict[str, Any]]:
    """
    Use Groq to classify disruption type, assess automotive relevance, and extract geo.
    Returns dict with: is_automotive_sc_risk, disruption_type, risks_identified, geo_country, geo_region.
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    prompt = f"""You are an expert automotive supply chain risk analyst. Analyze this news article.

STEP 1 — Relevance: Is this a specific, real disruption to the automotive supply chain?
YES: plant shutdowns, strikes at auto plants/suppliers, tariffs on vehicles/parts, disasters hitting auto manufacturing regions, supplier bankruptcies, port/shipping disruptions affecting auto trade, cyberattacks on OEMs/suppliers, emissions/safety regulation changes.
NO: product launches, acquisitions, industry trend reports, forecasts, opinion pieces, warehouse automation, general trucking news unrelated to autos, awards, fundraising announcements, executive interviews, earnings reports without a stated supply impact, autonomous vehicle demos.

Allowed disruption_type (pick exactly one): {_DISRUPTION_LIST}

Text (title + summary):
---
{text[:4000]}
---

Respond with ONLY a single JSON object, no markdown:
{{
  "is_automotive_sc_risk": <true or false>,
  "disruption_type": "<one of the allowed types>",
  "risks_identified": "<semicolon-separated list of 1-4 specific risks, empty string if not a risk>",
  "geo_country": "<full country name — infer from company names, agencies, or geography; use null ONLY if truly impossible>",
  "geo_region": "<REQUIRED when geo_country is known — one of: {_GEO_REGION_LIST}; use null only when geo_country is also null>",
  "impact_1to5": <integer 1-5 — see scale below>,
  "probability_1to5": <integer 1-5 — see scale below>,
  "time_sensitivity_1to3": <integer 1-3 — see scale below>,
  "exposure_proxy_1to5": <integer 1-5 — see scale below>
}}

SCORING SCALES — be accurate, not conservative. Real confirmed disruptions should score 3-5, not 1-2.

impact_1to5 (severity of supply/production effect):
  1 = brief mention with no quantified impact
  2 = possible minor disruption, speculative or early warning
  3 = confirmed delay, shortage, or partial output reduction (e.g. "production slowed", "shipments delayed weeks")
  4 = major output cut, significant stoppage, price increase >10%, or broad industry impact (e.g. "cut 40,000 units", "25% tariff on all auto parts")
  5 = force majeure, complete production halt, or irreversible supply chain break

probability_1to5 (confidence the disruption is real and occurring):
  1 = rumored, speculative, analyst warning ("could", "may", "might", "feared")
  2 = reported but unconfirmed by primary source
  3 = officially announced or confirmed by company/government ("announced", "confirmed", "signed", "enacted")
  4 = actively happening right now ("halted", "ongoing", "workers are striking", "tariff took effect", "plant is shut")
  5 = fully in effect and irreversible (bankruptcy filed, factory destroyed, law in force with no suspension)
  NOTE: Tariffs signed by executive order and currently in effect = 4-5. Factory confirmed shut = 4. Announced strike starting next week = 3.

time_sensitivity_1to3 (urgency of the impact window):
  1 = long-term policy or gradual trend (months away, regulatory phase-in)
  2 = near-term impact expected within weeks
  3 = immediate — disruption is happening now or within days

exposure_proxy_1to5 (supply concentration / dependency risk):
  1 = no specific suppliers or components mentioned
  2 = general industry or sector mentioned
  3 = specific OEM(s) or supplier(s) named
  4 = named as key/critical/major supplier or sole-source
  5 = exclusive or single-source supplier explicitly stated

Classification rules for disruption_type (only when is_automotive_sc_risk=true):
- Labor Strike: workers walk out, union dispute, industrial action at auto plant or supplier
- Plant Shutdown: factory/facility closure, fire, or production halt at automotive site
- Logistics Disruption: port congestion, shipping delays, container backlog, Strait of Hormuz/Red Sea/Suez Canal disruption, freight rerouting due to conflict, rail or trucking disruption affecting auto shipments
- Trade Restriction: tariffs on vehicles or auto parts, sanctions, export/import bans, trade war measures affecting auto industry
- Cyberattack: ransomware, hacking, IT outage at automotive company or supplier
- Natural Disaster: earthquake, flood, wildfire, hurricane affecting auto manufacturing region
- Supplier Insolvency: bankruptcy, chapter 11, liquidation of auto parts supplier or OEM
- Regulatory Change: emissions mandates, NHTSA/safety rules, EV mandates, recall orders, new automotive regulation
- Capacity Constraint: automotive production cuts, output reductions, idle capacity at OEM or supplier
- Other: only if is_automotive_sc_risk=true but truly none of the above fit

For geo_country and geo_region — ALWAYS infer from available signals, never leave null when inferable:
- Company names: Ford/GM/Tesla/Stellantis/Chrysler=United States; VW/BMW/Mercedes/Bosch/Continental/ZF=Germany; Toyota/Denso/Honda/Aisin/Panasonic=Japan; Hyundai/Kia=South Korea; BYD/CATL/SAIC/Geely/NIO=China; Tata/Mahindra=India; Volvo/Scania=Sweden
- Agency/law names: NHTSA/DOT/EPA/FMCSA/Congress/Senate/White House/Trump=United States; EU Commission/Brussels=European Union→Europe
- Geography: Strait of Hormuz/Red Sea/Persian Gulf/Hormuz/Houthi=Middle East; Suez Canal=Africa/Middle East; "european union"/"EU"=Europe
- Region map: US/Canada/Mexico=North America; Germany/UK/France/Italy/Spain/Poland/Sweden=Europe; China/Japan/Korea/Taiwan=East Asia; India/Pakistan=South Asia; Thailand/Vietnam/Malaysia=Southeast Asia; Saudi/UAE/Iran/Iraq/Yemen/Qatar=Middle East; Brazil/Argentina/Chile=Latin America; Nigeria/South Africa/Egypt=Africa"""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.1,
        )
        raw = _strip_fences((resp.choices[0].message.content or "").strip())
        out = json.loads(raw)
        disruption = out.get("disruption_type") or "Other"
        if disruption not in DISRUPTION_TYPES:
            disruption = "Other"
        is_risk = bool(out.get("is_automotive_sc_risk", True))
        geo_region = out.get("geo_region")
        if geo_region and geo_region not in GEO_REGIONS:
            geo_region = None

        def _clamp_int(val: Any, lo: int, hi: int) -> int | None:
            try:
                v = int(val)
                return max(lo, min(hi, v))
            except (TypeError, ValueError):
                return None

        return {
            "is_automotive_sc_risk": is_risk,
            "disruption_type": disruption,
            "risks_identified": out.get("risks_identified") or "",
            "geo_country": out.get("geo_country"),
            "geo_region": geo_region,
            "impact_1to5": _clamp_int(out.get("impact_1to5"), 1, 5),
            "probability_1to5": _clamp_int(out.get("probability_1to5"), 1, 5),
            "time_sensitivity_1to3": _clamp_int(out.get("time_sensitivity_1to3"), 1, 3),
            "exposure_proxy_1to5": _clamp_int(out.get("exposure_proxy_1to5"), 1, 5),
        }
    except Exception:
        return None


def classify_event_fields(
    title: str,
    summary: str,
    api_key: Optional[str],
    model: str = "llama-3.3-70b-versatile",
) -> Optional[dict[str, Any]]:
    """
    Re-classify an event's disruption_type, geo_country, geo_region, and automotive relevance.
    Used by the reclassify script to fix events stored as 'Other' or 'Unknown'.
    Returns dict with: is_automotive_sc_risk, disruption_type, geo_country, geo_region.
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    prompt = f"""You are an expert automotive supply chain risk analyst. Classify this news article.

STEP 1 — Relevance: Is this a specific, real disruption to the automotive supply chain?
YES: plant shutdowns, strikes at auto plants/suppliers, tariffs on vehicles/parts, disasters hitting auto manufacturing regions, supplier bankruptcies, port/shipping disruptions affecting auto trade, cyberattacks on OEMs/suppliers, emissions/safety regulation changes.
NO: product launches, acquisitions, industry trend reports, forecasts, opinion pieces, general trucking/logistics news unrelated to autos, warehouse automation articles, awards, fundraising announcements, earnings without stated supply impact.

Allowed disruption_type (pick exactly one): {_DISRUPTION_LIST}
Allowed geo_region (pick exactly one): {_GEO_REGION_LIST}, Unknown

Title: {title}
Summary: {summary[:2000]}

Respond with ONLY a single JSON object, no markdown:
{{
  "is_automotive_sc_risk": <true or false>,
  "disruption_type": "<one of the allowed disruption types>",
  "geo_country": "<country name, or null if truly unknown>",
  "geo_region": "<one of the allowed geo regions, or Unknown>"
}}

Classification rules for disruption_type (relevant when is_automotive_sc_risk=true):
- Labor Strike: workers walk out, union dispute, industrial action at auto plant or supplier
- Plant Shutdown: factory/facility closure, fire, or production halt at automotive site
- Logistics Disruption: port congestion, shipping delays, container backlog, Strait of Hormuz/Red Sea/Suez Canal disruption, freight rerouting due to conflict, rail or trucking disruption affecting auto shipments
- Trade Restriction: tariffs on vehicles or auto parts, sanctions, export/import bans, trade war measures
- Cyberattack: ransomware, hacking, IT outage at automotive company or supplier
- Natural Disaster: earthquake, flood, wildfire, hurricane affecting auto manufacturing region
- Supplier Insolvency: bankruptcy, chapter 11, liquidation of auto parts supplier or OEM
- Regulatory Change: emissions mandates, NHTSA/safety rules, EV mandates, recall orders, new automotive regulation
- Capacity Constraint: automotive production cuts, output reductions, idle capacity at OEM or supplier
- Other: only if is_automotive_sc_risk=true but truly none of the above fit

For geo_country and geo_region — ALWAYS infer from available signals, never leave null when inferable:
- Company names: Ford/GM/Tesla/Stellantis/Chrysler=United States; VW/BMW/Mercedes/Bosch/Continental/ZF=Germany; Toyota/Denso/Honda/Aisin/Panasonic=Japan; Hyundai/Kia=South Korea; BYD/CATL/SAIC/Geely/NIO=China; Tata/Mahindra=India; Volvo/Scania=Sweden
- Agency/law names: NHTSA/DOT/EPA/FMCSA/Congress/Senate/White House/Trump=United States; EU Commission/Brussels=European Union→Europe
- Geography: Strait of Hormuz/Red Sea/Persian Gulf/Houthi=Middle East; Suez Canal=Middle East
- Region map: US/Canada/Mexico=North America; Germany/UK/France/Italy/Spain/Poland/Sweden=Europe; China/Japan/Korea/Taiwan=East Asia; India/Pakistan=South Asia; Thailand/Vietnam/Malaysia=Southeast Asia; Saudi/UAE/Iran/Iraq/Yemen/Qatar=Middle East; Brazil/Argentina=Latin America; Nigeria/South Africa=Africa
- IMPORTANT: always return geo_region when geo_country is known; return null only when geo_country is also null"""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.1,
        )
        raw = _strip_fences((resp.choices[0].message.content or "").strip())
        out = json.loads(raw)

        disruption = out.get("disruption_type") or "Other"
        if disruption not in DISRUPTION_TYPES:
            disruption = "Other"

        is_risk = bool(out.get("is_automotive_sc_risk", True))

        geo_country = out.get("geo_country") or "Unknown"
        if not geo_country or str(geo_country).lower() in ("null", "none", "unknown", ""):
            geo_country = "Unknown"

        geo_region = out.get("geo_region") or "Unknown"
        if geo_region not in GEO_REGIONS:
            geo_region = "Unknown"

        return {
            "is_automotive_sc_risk": is_risk,
            "disruption_type": disruption,
            "geo_country": str(geo_country),
            "geo_region": str(geo_region),
        }
    except Exception as e:
        print(f"  [groq classify_event_fields error] {type(e).__name__}: {e}", flush=True)
        return None


def generate_mitigation_text(
    event_title: str,
    event_summary: str,
    reason_flagged: str,
    disruption_type: str,
    geo_country: str,
    component_entities: list[str],
    api_key: Optional[str],
    model: str = "llama-3.3-70b-versatile",
    risk_score: float = 0.0,
    exposure_usd_est: float = 0.0,
    estimated_delay_days: int = 0,
    severity_band: str = "Medium",
) -> Optional[dict[str, Any]]:
    """
    Generate personalized mitigation description and 3 structured action items for this event.
    Returns dict with: mitigation_description (str), mitigation_actions (list[str] of 3).
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    components = ", ".join(component_entities[:5]) if component_entities else "general supply"
    exposure_str = f"${exposure_usd_est:,.0f}" if exposure_usd_est else "unknown"
    delay_str = f"{estimated_delay_days} days" if estimated_delay_days else "unknown"
    prompt = f"""You are a supply chain risk advisor. Write mitigation guidance for this specific event.

Event title: {event_title}
Summary: {event_summary}
Why flagged: {reason_flagged}
Disruption type: {disruption_type}
Location: {geo_country}
Relevant components/supplies: {components}
Risk score: {risk_score:.0f}/100 ({severity_band} severity)
Estimated financial exposure: {exposure_str}
Estimated supply delay: {delay_str}

RULES — follow strictly:
- The mitigation_description must explain what is UNIQUE about this specific event vs. a generic {disruption_type}. Do NOT restate the risk score, severity band, or exposure — those are already shown. Focus on the specific mechanism of disruption and what to watch.
- Each action MUST reference named entities from this event (companies, components, routes, ports, countries). Generic phrases like "engage trade counsel", "activate alternate sourcing", or "develop a contingency plan" without specifics are not acceptable.
- If two disruption events share the same type, their actions must still differ based on the specific context.
- Do not invent facts. Only use information present in the event title, summary, and context above.

Respond with ONLY a single JSON object, no markdown:
{{
  "mitigation_description": "<1-2 sentences: the specific mechanism of disruption and what to prioritize monitoring — do not include risk score, severity band, or exposure figures>",
  "mitigation_actions": {{
    "immediate": "<most urgent action within 24-48 hours — name specific companies, components, or routes from this event>",
    "near_term": "<action within 1-2 weeks — name specific alternatives or contingencies relevant to this event>",
    "strategic": "<longer-term structural change to reduce this specific exposure>"
  }}
}}"""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.15,
        )
        raw = _strip_fences((resp.choices[0].message.content or "").strip())
        out = json.loads(raw)
        desc = out.get("mitigation_description") or "Prioritize supply continuity and monitor impact."
        actions_obj = out.get("mitigation_actions")
        if isinstance(actions_obj, dict):
            actions = [
                str(actions_obj.get("immediate") or ""),
                str(actions_obj.get("near_term") or ""),
                str(actions_obj.get("strategic") or ""),
            ]
            actions = [a for a in actions if a]
        elif isinstance(actions_obj, list) and actions_obj:
            # graceful fallback if model returns a list anyway
            actions = [str(a) for a in actions_obj[:3]]
        else:
            return None
        if not actions:
            return None
        return {"mitigation_description": desc, "mitigation_actions": actions}
    except Exception:
        return None
