"""App configuration."""
from __future__ import annotations

import os
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


def _get_groq_api_key() -> Optional[str]:
    """Groq API key from env, Streamlit secrets, or .streamlit/secrets.toml file."""
    key = os.environ.get("GROQ_API_KEY")
    if isinstance(key, str) and key.strip():
        return key.strip()
    try:
        import streamlit as st
        secrets = getattr(st, "secrets", None)
        if secrets is not None:
            v = getattr(secrets, "GROQ_API_KEY", None)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if callable(getattr(secrets, "get", None)):
                v = secrets.get("GROQ_API_KEY")
                if isinstance(v, str) and v.strip():
                    return v.strip()
                for section in ("groq", "llm"):
                    block = secrets.get(section)
                    if block is not None:
                        ak = block.get("api_key") if isinstance(block, dict) else getattr(block, "api_key", None)
                        if isinstance(ak, str) and ak.strip():
                            return ak.strip()
    except Exception:
        pass
    # Fallback: read .streamlit/secrets.toml from project root (works when st.secrets not ready)
    try:
        root = Path(__file__).resolve().parents[1]
        secrets_file = root / ".streamlit" / "secrets.toml"
        if secrets_file.is_file():
            text = secrets_file.read_text(encoding="utf-8")
            import re
            m = re.search(r'GROQ_API_KEY\s*=\s*["\']([^"\']+)["\']', text)
            if m and m.group(1).strip():
                return m.group(1).strip()
    except Exception:
        pass
    return None


class AppConfig:
    """Configuration for the application."""

    __slots__ = ("project_root", "db_path", "db_url", "rss_urls", "retention_days", "enriched_retention_days", "source_weights", "groq_api_key", "groq_model")

    def __init__(
        self,
        project_root: Path,
        db_path: Path,
        db_url: Optional[str],
        rss_urls: tuple[str, ...],
        retention_days: int,
        enriched_retention_days: int,
        source_weights: dict[str, float],
        groq_api_key: Optional[str] = None,
        groq_model: str = "llama-3.1-8b-instant",
    ) -> None:
        object.__setattr__(self, "project_root", project_root)
        object.__setattr__(self, "db_path", db_path)
        object.__setattr__(self, "db_url", db_url)
        object.__setattr__(self, "rss_urls", rss_urls)
        object.__setattr__(self, "retention_days", retention_days)
        object.__setattr__(self, "enriched_retention_days", enriched_retention_days)
        object.__setattr__(self, "source_weights", source_weights)
        object.__setattr__(self, "groq_api_key", groq_api_key or _get_groq_api_key())
        object.__setattr__(self, "groq_model", os.environ.get("GROQ_MODEL") or groq_model)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("AppConfig is immutable")


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
        groq_api_key=None,
        groq_model="llama-3.1-8b-instant",
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
    # Top-level keys (Streamlit Cloud Secrets UI)
    for key in ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL"):
        val = secrets.get(key)
        if val and isinstance(val, str):
            return val
    # Nested e.g. secrets["database"]["url"]
    for section in ("database", "supabase"):
        block = secrets.get(section)
        if isinstance(block, dict):
            for k in ("url", "SUPABASE_DB_URL", "database_url"):
                val = block.get(k)
                if val and isinstance(val, str):
                    return val
    return None
