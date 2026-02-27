"""Groq LLM client for event classification and personalized mitigation."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from src.config import DISRUPTION_TYPES, GEO_REGIONS, RISK_CATEGORIES

DISRUPTION_LIST = ", ".join(DISRUPTION_TYPES)
RISK_LIST = ", ".join(RISK_CATEGORIES)
GEO_REGION_LIST = ", ".join(r for r in GEO_REGIONS if r != "Unknown")


def _get_client(api_key: Optional[str]):
    if not api_key:
        return None
    try:
        from groq import Groq
        return Groq(api_key=api_key)
    except Exception:
        return None


def classify_disruption_and_risks(
    text: str,
    api_key: Optional[str],
    model: str = "llama-3.1-8b-instant",
) -> Optional[dict[str, Any]]:
    """
    Use Groq to classify disruption type, risk category, and identify risks from article text.
    Returns dict with: disruption_type, risk_category, risks_identified (str), geo_country (optional).
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    prompt = f"""You are an expert in automotive supply chain risk. Classify this news text and extract risks.

Allowed disruption_type (pick exactly one): {DISRUPTION_LIST}
Allowed risk_category (pick exactly one): {RISK_LIST}

Text (title + summary):
---
{text[:4000]}
---

Respond with ONLY a single JSON object, no markdown, no explanation. Use this exact structure:
{{
  "disruption_type": "<one of the allowed list>",
  "risk_category": "<one of the allowed list>",
  "risks_identified": "<short semicolon-separated list of 1-4 specific risks this article describes, e.g. location closure; single-source supplier; delay to OEM>",
  "geo_country": "<country name if clearly mentioned, else null>"
}}
If the text is about a specific location, logistics, or regional disruption but does not fit Labor Strike/Port Congestion/etc., prefer the closest type (e.g. Plant Shutdown for factory closure, Port Congestion for shipping) or "Other" only if truly none fit. Prefer a specific type over "Other" when plausible."""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.2,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Strip markdown code block if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw)
        out = json.loads(raw)
        disruption = out.get("disruption_type") or "Other"
        if disruption not in DISRUPTION_TYPES:
            disruption = "Other"
        risk_cat = out.get("risk_category") or "Operational"
        if risk_cat not in RISK_CATEGORIES:
            risk_cat = "Operational"
        return {
            "disruption_type": disruption,
            "risk_category": risk_cat,
            "risks_identified": out.get("risks_identified") or "",
            "geo_country": out.get("geo_country"),
        }
    except Exception:
        return None


def classify_event_fields(
    title: str,
    summary: str,
    api_key: Optional[str],
    model: str = "llama-3.1-8b-instant",
) -> Optional[dict[str, Any]]:
    """
    Re-classify an event's disruption_type, geo_country, and geo_region using LLM.
    Used to fix events stored as 'Other' disruption type or 'Unknown' geo fields.
    Returns dict with: disruption_type, geo_country, geo_region.
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    prompt = f"""You are an expert in automotive supply chain risk. Classify this news article.

Allowed disruption_type (pick exactly one): {DISRUPTION_LIST}
Allowed geo_region (pick exactly one): {GEO_REGION_LIST}, Unknown

Title: {title}
Summary: {summary[:2000]}

Respond with ONLY a single JSON object, no markdown, no explanation:
{{
  "disruption_type": "<one of the allowed disruption types>",
  "geo_country": "<country name, or null if truly unknown>",
  "geo_region": "<one of the allowed geo regions, or Unknown>"
}}

Classification rules:
- disruption_type: prefer specific types over "Other". Fire at factory = Plant Shutdown. Financial collapse = Supplier Insolvency. Import/export duties or tariffs = Export Restriction. Worker walkout = Labor Strike. Flooding/earthquake/storm = Natural Disaster. Hacking/ransomware = Cyberattack. Government rule change = Regulatory Change. Port delays/backlog = Port Congestion.
- geo_country: infer from company names or events (Ford/GM plant in Michigan = United States, Volkswagen headquarters = Germany, Toyota = Japan, Hyundai/Kia = South Korea, BYD = China). Return null only if truly impossible to determine.
- geo_region: derive from geo_country (United States/Canada/Mexico = North America, Germany/UK/France = Europe, China/Japan/South Korea = East Asia, India/Pakistan = South Asia, Thailand/Vietnam/Malaysia = Southeast Asia, Saudi Arabia/UAE = Middle East, Brazil/Argentina = Latin America, Nigeria/South Africa = Africa)."""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.1,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw)
        out = json.loads(raw)

        disruption = out.get("disruption_type") or "Other"
        if disruption not in DISRUPTION_TYPES:
            disruption = "Other"

        geo_country = out.get("geo_country") or "Unknown"
        if not geo_country or str(geo_country).lower() in ("null", "none", "unknown", ""):
            geo_country = "Unknown"

        geo_region = out.get("geo_region") or "Unknown"
        if geo_region not in GEO_REGIONS:
            geo_region = "Unknown"

        return {
            "disruption_type": disruption,
            "geo_country": str(geo_country),
            "geo_region": str(geo_region),
        }
    except Exception:
        return None


def generate_mitigation_text(
    event_title: str,
    event_summary: str,
    reason_flagged: str,
    disruption_type: str,
    geo_country: str,
    component_entities: list[str],
    api_key: Optional[str],
    model: str = "llama-3.1-8b-instant",
) -> Optional[dict[str, Any]]:
    """
    Generate personalized mitigation description and 3–5 action items for this event.
    Returns dict with: mitigation_description (str), mitigation_actions (list[str]).
    Returns None if API key missing or request fails.
    """
    client = _get_client(api_key)
    if not client:
        return None
    components = ", ".join(component_entities[:5]) if component_entities else "general supply"
    prompt = f"""You are a supply chain risk advisor. For this high-risk event, provide brief, actionable mitigation guidance.

Event title: {event_title}
Summary: {event_summary}
Why flagged: {reason_flagged}
Disruption type: {disruption_type}
Location: {geo_country}
Relevant components/supplies: {components}

Respond with ONLY a single JSON object, no markdown:
{{
  "mitigation_description": "<1-2 sentences: priority and what to monitor>",
  "mitigation_actions": ["<immediate action 1>", "<near-term action 2>", "<optional longer-term action 3>"]
}}
Give 3–5 specific actions (immediate, near-term, longer-term). Do not invent facts; base actions on disruption type and context. Be concise."""

    try:
        resp = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output only valid JSON. No markdown, no code fences."},
                {"role": "user", "content": prompt},
            ],
            model=model,
            temperature=0.3,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw)
        out = json.loads(raw)
        desc = out.get("mitigation_description") or "Prioritize supply continuity and monitor impact."
        actions = out.get("mitigation_actions")
        if not isinstance(actions, list):
            actions = [desc]
        return {"mitigation_description": desc, "mitigation_actions": [str(a) for a in actions[:6]]}
    except Exception:
        return None
