"""Geo coordinates for mapping events (region/country -> lat/lon)."""

from __future__ import annotations

from typing import Tuple

# Approximate region centroids for Pydeck/Deck.gl (lat, lon).
REGION_COORDINATES: dict[str, Tuple[float, float]] = {
    "North America": (39.8283, -98.5795),
    "Europe": (50.0, 10.0),
    "East Asia": (35.0, 105.0),
    "South Asia": (20.0, 77.0),
    "Southeast Asia": (5.0, 115.0),
    "Middle East": (25.0, 45.0),
    "Latin America": (-15.0, -60.0),
    "Africa": (0.0, 20.0),
    "Unknown": (0.0, 0.0),
}

# Optional: country-level coordinates for finer resolution (subset).
COUNTRY_COORDINATES: dict[str, Tuple[float, float]] = {
    "United States": (37.0902, -95.7129),
    "USA": (37.0902, -95.7129),
    "China": (35.8617, 104.1954),
    "Japan": (36.2048, 138.2529),
    "Germany": (51.1657, 10.4515),
    "Mexico": (23.6345, -102.5528),
    "Canada": (56.1304, -106.3468),
    "South Korea": (35.9078, 127.7669),
    "India": (20.5937, 78.9629),
    "Brazil": (-14.2350, -51.9253),
    "United Kingdom": (55.3781, -3.4360),
    "UK": (55.3781, -3.4360),
    "France": (46.2276, 2.2137),
    "Italy": (41.8719, 12.5674),
    "Spain": (40.4637, -3.7492),
    "Netherlands": (52.1326, 5.2913),
    "Thailand": (15.8700, 100.9925),
    "Vietnam": (14.0583, 108.2772),
    "Indonesia": (-0.7893, 113.9213),
    "Malaysia": (4.2105, 101.9758),
    "Taiwan": (23.6978, 120.9605),
    "Russia": (61.5240, 105.3188),
    "Turkey": (38.9637, 35.2433),
    "South Africa": (-30.5595, 22.9375),
    "Egypt": (26.8206, 30.8025),
    "Saudi Arabia": (23.8859, 45.0792),
    "United Arab Emirates": (23.4241, 53.8478),
    "UAE": (23.4241, 53.8478),
    "Argentina": (-38.4161, -63.6167),
    "Chile": (-35.6751, -71.5430),
    "Poland": (51.9194, 19.1451),
    "Czech Republic": (49.8175, 15.4730),
    "Hungary": (47.1625, 19.5033),
    "Romania": (45.9432, 24.9668),
    "Slovakia": (48.6690, 19.6990),
    "Sweden": (60.1282, 18.6435),
    "Belgium": (50.5039, 4.4699),
    "Austria": (47.5162, 14.5501),
    "Switzerland": (46.8182, 8.2275),
    "Portugal": (39.3999, -8.2245),
    "Korea": (35.9078, 127.7669),
    "Pakistan": (30.3753, 69.3451),
    "Bangladesh": (23.6850, 90.3563),
    "Philippines": (12.8797, 121.7740),
    "Singapore": (1.3521, 103.8198),
    "Australia": (-25.2744, 133.7751),
    "Israel": (31.0461, 34.8516),
    "Iran": (32.4279, 53.6880),
    "Iraq": (33.2232, 43.6793),
    "Nigeria": (9.0820, 8.6753),
    "Kenya": (-0.0236, 37.9062),
    "Ethiopia": (9.1450, 40.4897),
    "Colombia": (4.5709, -74.2973),
    "Ukraine": (48.3794, 31.1656),
    "Unknown": (0.0, 0.0),
}


def get_event_coordinates(event: dict) -> Tuple[float, float]:
    """Return (lat, lon) for an event using country first, then region."""
    country = str(event.get("geo_country") or "").strip()
    region = str(event.get("geo_region") or "Unknown").strip()
    if country and country not in ("Unknown", "") and country in COUNTRY_COORDINATES:
        return COUNTRY_COORDINATES[country]
    return REGION_COORDINATES.get(region, REGION_COORDINATES["Unknown"])
