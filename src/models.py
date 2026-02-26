"""Data models and schemas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

RiskCategory = Literal[
    "Political",
    "Economic",
    "Social",
    "Technological",
    "Environmental",
    "Legal",
    "Operational",
]
DisruptionType = Literal[
    "Labor Strike",
    "Plant Shutdown",
    "Port Congestion",
    "Export Restriction",
    "Cyberattack",
    "Natural Disaster",
    "Supplier Insolvency",
    "Regulatory Change",
    "Other",
]
GeoRegion = Literal[
    "North America",
    "Europe",
    "East Asia",
    "South Asia",
    "Southeast Asia",
    "Middle East",
    "Latin America",
    "Africa",
    "Unknown",
]
ConfidenceLevel = Literal["High", "Medium", "Low"]
Criticality = Literal["low", "medium", "high"]


@dataclass(frozen=True)
class RawArticle:
    """Normalized raw article."""

    article_id: str
    article_url: str
    source_name: str
    source_weight: float
    published_at: datetime
    ingested_at: datetime
    title: str
    summary: str
    content: str


class LLMExtraction(BaseModel):
    """LLM structured extraction schema."""

    event_summary: str = Field(..., description="Summary of the event.")
    reason_flagged: str = Field(..., description="Why this is a risk.")
    geo_country: str
    geo_region: GeoRegion
    geo_confidence: ConfidenceLevel
    risk_category: RiskCategory
    disruption_type: DisruptionType
    impact_1to5: int = Field(..., ge=1, le=5)
    probability_1to5: int = Field(..., ge=1, le=5)
    time_sensitivity_1to3: int = Field(..., ge=1, le=3)
    exposure_proxy_1to5: int = Field(..., ge=1, le=5)
    severity_confidence: ConfidenceLevel
    estimated_delay_days: int = Field(..., ge=0)
    delay_confidence: ConfidenceLevel
    delay_rationale: str
    oem_entities: list[str]
    supplier_entities: list[str]
    component_entities: list[str]
    component_criticality: Criticality
    llm_validation_passed: bool
    rejected_reason: Optional[str] = None
    risks_identified: Optional[str] = None  # From Groq when used for classification


@dataclass
class EnrichedEvent:
    """Enriched event stored in the database."""

    event_id: str
    article_url: str
    source_name: str
    source_weight: float
    published_at: datetime
    ingested_at: datetime
    title: str
    event_summary: str
    dashboard_blurb: Optional[str]
    reason_flagged: str
    oem_entities: list[str]
    supplier_entities: list[str]
    component_entities: list[str]
    component_criticality: str
    risk_category: str
    disruption_type: str
    geo_country: str
    geo_region: str
    geo_confidence: str
    impact_1to5: int
    probability_1to5: int
    time_sensitivity_1to3: int
    exposure_proxy_1to5: int
    severity_confidence: str
    risk_score_0to100: float
    severity_band: str
    estimated_delay_days: int
    delay_confidence: str
    delay_rationale: str
    exposure_usd_est: float
    exposure_confidence: str
    exposure_assumptions: str
    mitigation_description: Optional[str]
    mitigation_actions: Optional[list[str]]
    mitigation_generated_at: Optional[datetime]
    llm_validation_passed: bool
    rejected_reason: Optional[str]
    created_at: datetime
