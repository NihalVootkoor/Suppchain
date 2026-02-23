"""App configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

RISK_CATEGORIES = (
    "Political",
    "Economic",
    "Social",
    "Technological",
    "Environmental",
    "Legal",
    "Operational",
)
DISRUPTION_TYPES = (
    "Labor Strike",
    "Plant Shutdown",
    "Port Congestion",
    "Export Restriction",
    "Cyberattack",
    "Natural Disaster",
    "Supplier Insolvency",
    "Regulatory Change",
    "Other",
)
GEO_REGIONS = (
    "North America",
    "Europe",
    "East Asia",
    "South Asia",
    "Southeast Asia",
    "Middle East",
    "Latin America",
    "Africa",
    "Unknown",
)
COUNTRY_MAP = {
    "china": ("China", "East Asia"),
    "japan": ("Japan", "East Asia"),
    "korea": ("Korea", "East Asia"),
    "south korea": ("South Korea", "East Asia"),
    "india": ("India", "South Asia"),
    "germany": ("Germany", "Europe"),
    "france": ("France", "Europe"),
    "uk": ("United Kingdom", "Europe"),
    "united kingdom": ("United Kingdom", "Europe"),
    "mexico": ("Mexico", "North America"),
    "canada": ("Canada", "North America"),
    "united states": ("United States", "North America"),
    "u.s.": ("United States", "North America"),
    "brazil": ("Brazil", "Latin America"),
    "africa": ("Africa", "Africa"),
}
OEMS = [
    "Tesla",
    "Toyota",
    "Ford",
    "GM",
    "General Motors",
    "Stellantis",
    "Volkswagen",
    "Hyundai",
    "Honda",
    "Nissan",
    "BMW",
    "Mercedes",
]
TIER1S = [
    "Bosch",
    "Denso",
    "Magna",
    "Continental",
    "Aptiv",
    "ZF",
    "Valeo",
    "Lear",
    "Yazaki",
]
AUTO_TERMS = [
    "semiconductor",
    "chip",
    "battery",
    "lithium",
    "cathode",
    "anode",
    "wiring harness",
    "ecu",
    "steel",
    "aluminum",
    "motor",
]
DISRUPTION_TRIGGERS = [
    "strike",
    "shutdown",
    "closure",
    "port",
    "congestion",
    "export restriction",
    "export ban",
    "sanctions",
    "tariff",
    "cyberattack",
    "ransomware",
    "hurricane",
    "earthquake",
    "flood",
    "fire",
    "insolvency",
    "bankruptcy",
    "regulatory",
]
NEGATIVE_KEYWORDS = [
    "review",
    "msrp",
    "test drive",
    "horsepower",
    "interior",
    "launch event",
]

@dataclass(frozen=True)
class AppConfig:
    """Configuration for the application."""

    project_root: Path
    db_path: Path
    db_url: Optional[str]
    rss_urls: tuple[str, ...]
    retention_days: int
    enriched_retention_days: int
    source_weights: dict[str, float]


def get_config(project_root: Optional[Path] = None) -> AppConfig:
    """Build the default configuration."""

    root = (project_root or Path(__file__).resolve().parents[1]).resolve()
    data_dir = (root / "data").resolve()
    db_url = (
        _get_secret_db_url()
        or os.environ.get("SUPABASE_DB_URL")
        or os.environ.get("SUPABASE_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
    )
    return AppConfig(
        project_root=root,
        db_path=data_dir / "app.db",
        db_url=db_url,
        rss_urls=(
            "https://news.google.com/rss/search?q=automotive%20supply%20chain%20disruption&hl=en-US&gl=US&ceid=US:en",
            "https://news.google.com/rss/search?q=auto%20plant%20shutdown%20strike&hl=en-US&gl=US&ceid=US:en",
            "https://news.google.com/rss/search?q=automotive%20semiconductor%20shortage&hl=en-US&gl=US&ceid=US:en",
            "https://www.automotiveworld.com/feed/",
            "https://www.just-auto.com/feed/",
            "https://www.freightwaves.com/feed",
        ),
        retention_days=45,
        enriched_retention_days=365,
        source_weights={
            "https://news.google.com/rss/search?q=automotive%20supply%20chain%20disruption&hl=en-US&gl=US&ceid=US:en": 0.7,
            "https://news.google.com/rss/search?q=auto%20plant%20shutdown%20strike&hl=en-US&gl=US&ceid=US:en": 0.7,
            "https://news.google.com/rss/search?q=automotive%20semiconductor%20shortage&hl=en-US&gl=US&ceid=US:en": 0.7,
            "https://www.automotiveworld.com/feed/": 0.75,
            "https://www.just-auto.com/feed/": 0.7,
            "https://www.freightwaves.com/feed": 0.6,
        },
    )


def _get_secret_db_url() -> Optional[str]:
    """Read Supabase connection from Streamlit secrets when available."""

    try:
        import streamlit as st
    except Exception:
        return None
    try:
        secrets = st.secrets
    except Exception:
        return None
    return (
        secrets.get("SUPABASE_DB_URL")
        or secrets.get("SUPABASE_DATABASE_URL")
        or secrets.get("DATABASE_URL")
    )
