"""App configuration."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Optional

RISK_CATEGORIES = (
    "Supply Disruption",
    "Logistics & Transport",
    "Geopolitical & Trade",
    "Natural Disaster & Climate",
    "Cyber & Technology",
    "Labor & Social",
    "Regulatory & Compliance",
)
DISRUPTION_TYPES = (
    "Labor Strike",
    "Plant Shutdown",
    "Logistics Disruption",
    "Trade Restriction",
    "Cyberattack",
    "Natural Disaster",
    "Supplier Insolvency",
    "Regulatory Change",
    "Capacity Constraint",
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
    # North America
    "united states": ("United States", "North America"),
    "u.s.": ("United States", "North America"),
    "usa": ("United States", "North America"),
    "u.s.a.": ("United States", "North America"),
    "american": ("United States", "North America"),
    "trump": ("United States", "North America"),
    "scotus": ("United States", "North America"),
    "white house": ("United States", "North America"),
    "congress": ("United States", "North America"),
    "senate": ("United States", "North America"),
    "nhtsa": ("United States", "North America"),
    "epa": ("United States", "North America"),
    "fmcsa": ("United States", "North America"),
    "detroit": ("United States", "North America"),
    "michigan": ("United States", "North America"),
    "kentucky": ("United States", "North America"),
    "ohio": ("United States", "North America"),
    "tennessee": ("United States", "North America"),
    "alabama": ("United States", "North America"),
    "south carolina": ("United States", "North America"),
    "washington d.c.": ("United States", "North America"),
    "washington, d.c.": ("United States", "North America"),
    "canada": ("Canada", "North America"),
    "canadian": ("Canada", "North America"),
    "mexico": ("Mexico", "North America"),
    "mexican": ("Mexico", "North America"),
    # Europe
    "eu": ("European Union", "Europe"),
    "european union": ("European Union", "Europe"),
    "brussels": ("European Union", "Europe"),
    "germany": ("Germany", "Europe"),
    "german": ("Germany", "Europe"),
    "france": ("France", "Europe"),
    "french": ("France", "Europe"),
    "uk": ("United Kingdom", "Europe"),
    "united kingdom": ("United Kingdom", "Europe"),
    "britain": ("United Kingdom", "Europe"),
    "british": ("United Kingdom", "Europe"),
    "england": ("United Kingdom", "Europe"),
    "italy": ("Italy", "Europe"),
    "italian": ("Italy", "Europe"),
    "spain": ("Spain", "Europe"),
    "spanish": ("Spain", "Europe"),
    "netherlands": ("Netherlands", "Europe"),
    "dutch": ("Netherlands", "Europe"),
    "poland": ("Poland", "Europe"),
    "polish": ("Poland", "Europe"),
    "czech republic": ("Czech Republic", "Europe"),
    "czechia": ("Czech Republic", "Europe"),
    "hungary": ("Hungary", "Europe"),
    "hungarian": ("Hungary", "Europe"),
    "romania": ("Romania", "Europe"),
    "slovakia": ("Slovakia", "Europe"),
    "sweden": ("Sweden", "Europe"),
    "swedish": ("Sweden", "Europe"),
    "belgium": ("Belgium", "Europe"),
    "austria": ("Austria", "Europe"),
    "switzerland": ("Switzerland", "Europe"),
    "portugal": ("Portugal", "Europe"),
    "turkey": ("Turkey", "Middle East"),
    "turkish": ("Turkey", "Middle East"),
    # East Asia
    "china": ("China", "East Asia"),
    "chinese": ("China", "East Asia"),
    "japan": ("Japan", "East Asia"),
    "japanese": ("Japan", "East Asia"),
    "south korea": ("South Korea", "East Asia"),
    "korea": ("South Korea", "East Asia"),
    "korean": ("South Korea", "East Asia"),
    "taiwan": ("Taiwan", "East Asia"),
    "taiwanese": ("Taiwan", "East Asia"),
    # South Asia
    "india": ("India", "South Asia"),
    "indian": ("India", "South Asia"),
    "pakistan": ("Pakistan", "South Asia"),
    "bangladesh": ("Bangladesh", "South Asia"),
    # Southeast Asia
    "thailand": ("Thailand", "Southeast Asia"),
    "thai": ("Thailand", "Southeast Asia"),
    "vietnam": ("Vietnam", "Southeast Asia"),
    "vietnamese": ("Vietnam", "Southeast Asia"),
    "indonesia": ("Indonesia", "Southeast Asia"),
    "indonesian": ("Indonesia", "Southeast Asia"),
    "malaysia": ("Malaysia", "Southeast Asia"),
    "malaysian": ("Malaysia", "Southeast Asia"),
    "philippines": ("Philippines", "Southeast Asia"),
    "filipino": ("Philippines", "Southeast Asia"),
    "singapore": ("Singapore", "Southeast Asia"),
    # Middle East
    "saudi arabia": ("Saudi Arabia", "Middle East"),
    "saudi": ("Saudi Arabia", "Middle East"),
    "uae": ("United Arab Emirates", "Middle East"),
    "united arab emirates": ("United Arab Emirates", "Middle East"),
    "israel": ("Israel", "Middle East"),
    "iran": ("Iran", "Middle East"),
    "iraqi": ("Iraq", "Middle East"),
    "iraq": ("Iraq", "Middle East"),
    "qatar": ("Qatar", "Middle East"),
    "kuwait": ("Kuwait", "Middle East"),
    "oman": ("Oman", "Middle East"),
    "bahrain": ("Bahrain", "Middle East"),
    "jordan": ("Jordan", "Middle East"),
    # Maritime chokepoints — map to Middle East (primary disruption region)
    "strait of hormuz": ("Iran", "Middle East"),
    "hormuz": ("Iran", "Middle East"),
    "red sea": ("Saudi Arabia", "Middle East"),
    "persian gulf": ("Iran", "Middle East"),
    "gulf of oman": ("Oman", "Middle East"),
    "suez canal": ("Egypt", "Africa"),
    "gulf of aden": ("Saudi Arabia", "Middle East"),
    "houthi": ("Yemen", "Middle East"),
    "yemen": ("Yemen", "Middle East"),
    "yemeni": ("Yemen", "Middle East"),
    # Latin America
    "brazil": ("Brazil", "Latin America"),
    "brazilian": ("Brazil", "Latin America"),
    "argentina": ("Argentina", "Latin America"),
    "chile": ("Chile", "Latin America"),
    "colombia": ("Colombia", "Latin America"),
    # Africa
    "south africa": ("South Africa", "Africa"),
    "nigeria": ("Nigeria", "Africa"),
    "kenya": ("Kenya", "Africa"),
    "egypt": ("Egypt", "Africa"),
    "ethiopia": ("Ethiopia", "Africa"),
    # Russia / CIS
    "russia": ("Russia", "Europe"),
    "russian": ("Russia", "Europe"),
    "ukraine": ("Ukraine", "Europe"),
    "ukrainian": ("Ukraine", "Europe"),
    # Oceania (map to nearest region)
    "australia": ("Australia", "Southeast Asia"),
    "australian": ("Australia", "Southeast Asia"),
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
    "Renault",
    "Kia",
    "Rivian",
    "Lucid",
    "BYD",
    "Geely",
    "SAIC",
    "NIO",
    "Volvo",
    "Audi",
    "Porsche",
    "Subaru",
    "Mazda",
    "Mitsubishi",
    "Fiat",
    "Chrysler",
    "Jeep",
    "Ram",
    "Dodge",
    "Skoda",
    "SEAT",
    "Opel",
    "Vauxhall",
    "Peugeot",
    "Citroën",
    "Isuzu",
    "Suzuki",
    "Daihatsu",
    "Chery",
    "Great Wall",
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
    "Faurecia",
    "Forvia",
    "Autoliv",
    "Panasonic",
    "LG Energy",
    "Samsung SDI",
    "CATL",
    "Plastic Omnium",
    "Tenneco",
    "Dana",
    "Sensata",
    "TE Connectivity",
    "Nexperia",
    "STMicroelectronics",
    "Infineon",
    "NXP",
    "Renesas",
    "Aisin",
    "Sumitomo",
    "Toyoda Gosei",
    "Gentex",
    "BorgWarner",
    "Modine",
    "Delphi",
    "Visteon",
    "Novelis",
    "Alcoa",
    # Newly added — critical suppliers with recent incidents
    "First Brands",
    "Autolite",
    "Fram",
    "Raybestos",
    "Trico",
    "Jaguar Land Rover",
    "JLR",
    "Ultium",
    "Marelli",
    "Bridgestone",
    "Aumovio",
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
    "cobalt",
    "nickel",
    "palladium",
    "platinum",
    "rare earth",
    "inverter",
    "powertrain",
    "drivetrain",
    "electric motor",
    "BMS",
    "EV battery",
    "battery cell",
    "battery pack",
    "ADAS",
    "LiDAR",
    "radar sensor",
    "brake",
    "airbag",
    "transmission",
    "axle",
    "tire",
    "PCB",
    "printed circuit",
    "microcontroller",
    "IGBT",
    "power module",
    # Memory / advanced chips — AI data centers competing with automakers for DRAM
    "DRAM",
    "HBM",
    "DDR memory",
    "memory chip",
    "high-bandwidth memory",
    # Critical minerals for EV motors and magnets
    "neodymium",
    "rare earth magnet",
    "permanent magnet",
    "manganese",
]
DISRUPTION_TRIGGERS = [
    "strike",
    "shutdown",
    "shut down",
    "closure",
    "halt",
    "halted",
    "stoppage",
    "shortage",
    "disruption",
    "port congestion",
    "port closure",
    "congestion",
    "shipping delay",
    "freight delay",
    "export restriction",
    "export ban",
    "sanctions",
    "tariff",
    "cyberattack",
    "ransomware",
    "hack",
    "outage",
    "hurricane",
    "earthquake",
    "flood",
    "fire",
    "typhoon",
    "storm",
    "insolvency",
    "bankruptcy",
    "chapter 11",
    "regulatory change",
    "new regulation",
    "recall",
    # Capacity / restructuring signals
    "restructur",
    "job cut",
    "layoff",
    "production cut",
    "capacity cut",
    "plant clos",
    # Geopolitical / trade signals
    "strait of hormuz",
    "red sea",
    "suez canal",
    "trade war",
    "import duty",
    "force majeure",
    # Maritime crisis signals (Hormuz war, tanker attacks)
    "vessel attack",
    "tanker attack",
    "tanker seized",
    "maritime disruption",
    "chokepoint",
    "vessel transit",
    "shipping lane",
    # Chip / memory shortage signals
    "chip shortage",
    "DRAM shortage",
    "memory shortage",
    "allocation shortage",
    # Production pause / idle signals
    "production pause",
    "pausing production",
    "idled",
    "idling plant",
    "temporary layoff",
    # Rare earth / mineral export controls
    "export control",
    "mineral export",
    "rare earth ban",
    "critical mineral",
]
NEGATIVE_KEYWORDS = [
    # "review" removed — too broad; blocks USMCA review, safety review, risk review articles
    "road test",
    "first drive",
    "driving impressions",
    "owner review",
    "msrp",
    "test drive",
    "horsepower",
    "launch event",
    "driver of the year",
    "award",
    "finalists",
    "raises $",
    "raises funding",
    "series a",
    "series b",
    "venture capital",
    "ipo",
    "product launch",
    "introduces new",
    "unveils",
    "autonomous drone",
    "warehouse robot",
    "deskless worker",
    "trucking podcast",
    "driver shortage awareness",
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
            if sys.version_info >= (3, 11):
                import tomllib
                data = tomllib.loads(secrets_file.read_text(encoding="utf-8"))
            else:
                try:
                    import tomli as tomllib  # type: ignore
                    data = tomllib.loads(secrets_file.read_text(encoding="utf-8"))
                except ImportError:
                    # tomli not available — fall back to regex for Python < 3.11
                    text = secrets_file.read_text(encoding="utf-8")
                    m = re.search(r'GROQ_API_KEY\s*=\s*["\']([^"\']+)["\']', text)
                    if m and m.group(1).strip():
                        return m.group(1).strip()
                    data = {}
            key = data.get("GROQ_API_KEY") or (data.get("groq") or {}).get("api_key")
            if isinstance(key, str) and key.strip():
                return key.strip()
    except Exception:
        pass
    return None


class AppConfig:
    """Configuration for the application."""

    __slots__ = (
        "project_root",
        "db_path",
        "db_url",
        "rss_urls",
        "retention_days",
        "enriched_retention_days",
        "source_weights",
        "groq_api_key",
        "groq_model",
        "refresh_interval_hours",
    )

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
        groq_model: str = "llama-3.3-70b-versatile",
        refresh_interval_hours: int = 24,
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
        _env_hours = os.environ.get("REFRESH_INTERVAL_HOURS")
        try:
            _hours = int(_env_hours) if _env_hours else refresh_interval_hours
        except ValueError:
            _hours = refresh_interval_hours
        object.__setattr__(self, "refresh_interval_hours", _hours)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("AppConfig is immutable")


def get_config(project_root: Optional[Path] = None) -> AppConfig:
    """Build the default configuration."""

    root = project_root or Path(__file__).resolve().parents[1]
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
        groq_model="llama-3.3-70b-versatile",
        rss_urls=(
            # Direct industry sources with full article bodies (replacing Google News redirect feeds)
            "https://www.supplychaindive.com/feeds/news/",
            "https://www.dcvelocity.com/rss",
            "https://www.globaltrademag.com/feed/",
            "https://www.automotiveworld.com/feed/",
            "https://www.just-auto.com/feed/",
            "https://www.freightwaves.com/feed",
            # Newly added — OEM-level disruptions, tariff impacts, production news
            "https://www.autonews.com/arc/outboundfeeds/rss/",
        ),
        retention_days=45,
        enriched_retention_days=730,
        source_weights={
            "https://www.supplychaindive.com/feeds/news/": 0.8,
            "https://www.dcvelocity.com/rss": 0.7,
            "https://www.globaltrademag.com/feed/": 0.7,
            "https://www.automotiveworld.com/feed/": 0.75,
            "https://www.just-auto.com/feed/": 0.7,
            "https://www.freightwaves.com/feed": 0.6,
            "https://www.autonews.com/arc/outboundfeeds/rss/": 0.85,
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
    try:
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
    except Exception:
        return None
    return None
