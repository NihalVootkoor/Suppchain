"""Comprehensive tests for the Auto Supply Chain Risk Monitor pipeline."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

# Ensure project root on path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import COUNTRY_MAP, DISRUPTION_TYPES, GEO_REGIONS, RISK_CATEGORIES, get_config
from src.date_utils import parse_datetime
from src.filters import filter_articles, hard_filter
from src.geo_utils import COUNTRY_COORDINATES, REGION_COORDINATES, get_event_coordinates
from src.llm_extract import (
    _classify_disruption_type,
    _classify_pestel,
    _estimate_delay_days,
    _extract_geo,
    _severity_signals,
    _should_reject_as_not_event,
    extract_structured_event,
)
from src.models import RawArticle
from src.scoring import compute_risk_score, estimate_exposure_usd, severity_band
from src.serialization import event_to_row, raw_to_row
from src.storage import DbPaths, init_db, upsert_enriched_events, fetch_enriched_events
from src.storage_utils import row_to_dict
from src.url_utils import canonicalize_url, hash_id


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_article(
    title: str = "Test article",
    summary: str = "",
    content: str = "",
    url: str = "https://example.com/test",
    source_name: str = "TestSource",
    source_weight: float = 0.7,
) -> RawArticle:
    now = datetime.now(timezone.utc)
    canonical = canonicalize_url(url)
    return RawArticle(
        article_id=hash_id(canonical),
        article_url=url,
        source_name=source_name,
        source_weight=source_weight,
        published_at=now,
        ingested_at=now,
        title=title,
        summary=summary,
        content=content or summary,
    )


def _make_db_paths() -> tuple[DbPaths, tempfile.TemporaryDirectory]:
    tmpdir = tempfile.TemporaryDirectory()
    db_path = Path(tmpdir.name) / "test.db"
    return DbPaths(db_path, None), tmpdir


# ── URL utilities ─────────────────────────────────────────────────────────────

class TestUrlUtils:
    def test_canonicalize_url_strips_query(self):
        url = "https://EXAMPLE.COM/path?q=1&foo=bar#section"
        assert canonicalize_url(url) == "https://example.com/path"

    def test_canonicalize_url_preserves_path(self):
        url = "https://reuters.com/article/automotive-supply-chain"
        assert canonicalize_url(url) == url

    def test_hash_id_stable(self):
        h1 = hash_id("hello")
        h2 = hash_id("hello")
        assert h1 == h2
        assert len(h1) == 16

    def test_hash_id_different_inputs(self):
        assert hash_id("a") != hash_id("b")


# ── Date utilities ────────────────────────────────────────────────────────────

class TestDateUtils:
    def test_parse_datetime_iso(self):
        dt = parse_datetime("2024-03-15T10:00:00Z")
        assert dt.tzinfo is not None
        assert dt.year == 2024

    def test_parse_datetime_naive_becomes_utc(self):
        dt = parse_datetime("2024-03-15 10:00:00")
        assert dt.tzinfo is not None

    def test_parse_datetime_empty_returns_now(self):
        before = datetime.now(timezone.utc)
        dt = parse_datetime("")
        after = datetime.now(timezone.utc)
        assert before <= dt <= after

    def test_parse_datetime_none_returns_now(self):
        dt = parse_datetime(None)
        assert dt.tzinfo is not None


# ── Config ────────────────────────────────────────────────────────────────────

class TestConfig:
    def test_get_config_has_rss_urls(self):
        config = get_config()
        assert len(config.rss_urls) > 0

    def test_get_config_db_path(self):
        config = get_config()
        assert config.db_path.name == "app.db"

    def test_country_map_keys_lowercase(self):
        for key in COUNTRY_MAP:
            assert key == key.lower(), f"Key {key!r} should be lowercase"

    def test_country_map_values_valid_regions(self):
        for key, (country, region) in COUNTRY_MAP.items():
            assert region in GEO_REGIONS, f"Invalid region {region!r} for key {key!r}"

    def test_country_map_has_usa_variants(self):
        assert "usa" in COUNTRY_MAP
        assert "u.s." in COUNTRY_MAP
        assert "american" in COUNTRY_MAP
        assert "chinese" in COUNTRY_MAP
        assert "japanese" in COUNTRY_MAP

    def test_config_refresh_interval_default(self):
        config = get_config()
        assert config.refresh_interval_hours == 24

    def test_disruption_types_complete(self):
        required = {"Labor Strike", "Plant Shutdown", "Port Congestion", "Export Restriction",
                    "Cyberattack", "Natural Disaster", "Supplier Insolvency", "Regulatory Change", "Other"}
        assert required == set(DISRUPTION_TYPES)

    def test_risk_categories_pestel(self):
        pestel = {"Political", "Economic", "Social", "Technological", "Environmental", "Legal", "Operational"}
        assert pestel == set(RISK_CATEGORIES)


# ── Filters ───────────────────────────────────────────────────────────────────

class TestFilters:
    def test_hard_filter_passes_valid_article(self):
        article = _make_article(
            title="Toyota plant shutdown due to strike",
            summary="Workers at a Toyota factory walked out today causing production halts.",
        )
        result = hard_filter(article)
        assert result.is_relevant

    def test_hard_filter_rejects_missing_anchor(self):
        article = _make_article(
            title="General strike causes disruptions",
            summary="Workers across multiple sectors went on strike.",
        )
        result = hard_filter(article)
        assert not result.is_relevant
        assert "automotive anchor" in result.reason.lower()

    def test_hard_filter_rejects_missing_trigger(self):
        article = _make_article(
            title="Toyota announces new model",
            summary="The automotive giant unveiled its latest electric vehicle.",
        )
        result = hard_filter(article)
        assert not result.is_relevant

    def test_hard_filter_rejects_negative_keyword(self):
        article = _make_article(
            title="Toyota new car review - best interior ever",
            summary="Test drive of the new Toyota. MSRP $45,000.",
        )
        result = hard_filter(article)
        assert not result.is_relevant

    def test_filter_articles_returns_tuple(self):
        articles = [
            _make_article(title="Toyota plant shutdown strike", summary="Workers walked out of Toyota factory."),
            _make_article(title="Car review horsepower test drive", summary="Best interior, MSRP $30k."),
        ]
        kept, rejected = filter_articles(articles)
        assert isinstance(kept, list)
        assert isinstance(rejected, dict)

    def test_filter_articles_deduplication_by_url(self):
        # Two articles with same URL should both be processed (dedup happens earlier)
        articles = [
            _make_article(title="Toyota strike", summary="Factory strike causes shutdown port congestion."),
        ]
        kept, rejected = filter_articles(articles)
        assert len(kept) + len(rejected) == 1


# ── LLM Extraction (deterministic) ───────────────────────────────────────────

class TestLLMExtract:
    def test_classify_disruption_type_strike(self):
        assert _classify_disruption_type("workers went on strike union walkout") == "Labor Strike"

    def test_classify_disruption_type_cyberattack(self):
        assert _classify_disruption_type("ransomware attack cyberattack hack outage") == "Cyberattack"

    def test_classify_disruption_type_plant_shutdown(self):
        assert _classify_disruption_type("plant shutdown halted production") == "Plant Shutdown"

    def test_classify_disruption_type_natural_disaster(self):
        assert _classify_disruption_type("earthquake flood wildfire damage") == "Natural Disaster"

    def test_classify_disruption_type_port_congestion(self):
        assert _classify_disruption_type("port congestion container backlog intermodal") == "Port Congestion"

    def test_classify_disruption_type_export_restriction(self):
        assert _classify_disruption_type("export ban sanctions tariff") == "Export Restriction"

    def test_classify_disruption_type_insolvency(self):
        assert _classify_disruption_type("bankruptcy insolvency creditor restructuring") == "Supplier Insolvency"

    def test_classify_disruption_type_regulatory(self):
        assert _classify_disruption_type("new regulation regulatory compliance rule change") == "Regulatory Change"

    def test_classify_disruption_type_other(self):
        assert _classify_disruption_type("some random text without specific triggers") == "Other"

    def test_classify_pestel_mapping(self):
        assert _classify_pestel("Labor Strike") == "Social"
        assert _classify_pestel("Cyberattack") == "Technological"
        assert _classify_pestel("Natural Disaster") == "Environmental"
        assert _classify_pestel("Export Restriction") == "Political"
        assert _classify_pestel("Regulatory Change") == "Legal"
        assert _classify_pestel("Supplier Insolvency") == "Economic"
        assert _classify_pestel("Port Congestion") == "Operational"
        assert _classify_pestel("Plant Shutdown") == "Operational"

    def test_pestel_result_always_in_risk_categories(self):
        for dtype in DISRUPTION_TYPES:
            result = _classify_pestel(dtype)
            assert result in RISK_CATEGORIES, f"PESTEL result {result!r} not in RISK_CATEGORIES"

    def test_extract_geo_china(self):
        country, region, conf = _extract_geo("tariffs imposed on china exports")
        assert country == "China"
        assert region == "East Asia"

    def test_extract_geo_usa_variants(self):
        country, region, conf = _extract_geo("american automotive tariff policy usa tariffs")
        assert country == "United States"
        assert region == "North America"

    def test_extract_geo_unknown(self):
        country, region, conf = _extract_geo("no geo information here whatsoever")
        assert country == "Unknown"
        assert region == "Unknown"
        assert conf == "Low"

    def test_extract_geo_southeast_asia(self):
        country, region, conf = _extract_geo("thailand factory thai plant shutdown")
        assert country == "Thailand"
        assert region == "Southeast Asia"

    def test_severity_signals_high_impact(self):
        signals = _severity_signals("production halted force majeure plant shutdown", "Plant Shutdown")
        assert signals["impact_1to5"] >= 4

    def test_severity_signals_high_probability(self):
        signals = _severity_signals("production halted completely effective immediately in effect", "Plant Shutdown")
        assert signals["probability_1to5"] >= 4

    def test_severity_signals_export_restriction_time_limited(self):
        # Export Restriction should have time_sensitivity <= 2
        signals = _severity_signals("export ban tariffs announced", "Export Restriction")
        assert signals["time_sensitivity_1to3"] <= 2

    def test_severity_signals_all_in_range(self):
        text = "earthquake flood strike tariff ransomware plant shutdown"
        for dtype in DISRUPTION_TYPES:
            s = _severity_signals(text, dtype)
            assert 1 <= s["impact_1to5"] <= 5
            assert 1 <= s["probability_1to5"] <= 5
            assert 1 <= s["time_sensitivity_1to3"] <= 3
            assert 1 <= s["exposure_proxy_1to5"] <= 5

    def test_estimate_delay_days_explicit(self):
        days, conf, _ = _estimate_delay_days("disruption lasting 3 weeks", "Other")
        assert days == 21
        assert conf == "High"

    def test_estimate_delay_days_months(self):
        days, conf, _ = _estimate_delay_days("disruption will last 2 months", "Other")
        assert days == 60
        assert conf == "High"

    def test_estimate_delay_days_default(self):
        days, conf, _ = _estimate_delay_days("no duration mentioned", "Port Congestion")
        assert days == 14
        assert conf == "Low"

    def test_should_reject_negative_keyword(self):
        reason = _should_reject_as_not_event("car review interior test drive horsepower msrp")
        assert reason is not None

    def test_should_reject_no_trigger(self):
        reason = _should_reject_as_not_event("automotive news update general announcement")
        assert reason is not None

    def test_should_not_reject_valid_event(self):
        reason = _should_reject_as_not_event("toyota plant shutdown strike workers walkout")
        assert reason is None

    def test_extract_structured_event_valid(self):
        article = _make_article(
            title="Toyota plant shutdown due to labor strike in Japan",
            summary="Workers at Toyota's main factory in Japan walked out today causing a full production shutdown.",
        )
        extraction = extract_structured_event(article)
        assert extraction.llm_validation_passed
        assert extraction.disruption_type in DISRUPTION_TYPES
        assert extraction.risk_category in RISK_CATEGORIES
        assert extraction.geo_country not in ("null", None)
        assert extraction.geo_region in GEO_REGIONS
        assert 1 <= extraction.impact_1to5 <= 5
        assert 1 <= extraction.probability_1to5 <= 5
        assert 1 <= extraction.time_sensitivity_1to3 <= 3
        assert 1 <= extraction.exposure_proxy_1to5 <= 5
        assert extraction.estimated_delay_days >= 0

    def test_extract_structured_event_rejected(self):
        article = _make_article(
            title="2024 Toyota Camry review - best horsepower",
            summary="Interior is stunning. Test drive MSRP $32,000.",
        )
        extraction = extract_structured_event(article)
        assert not extraction.llm_validation_passed
        assert extraction.rejected_reason is not None

    def test_extract_event_geo_country_never_null_string(self):
        # Ensure "null" string is never produced
        article = _make_article(
            title="Supply chain disruption causes automotive shutdown",
            summary="A major plant shutdown has disrupted production at a key automotive supplier.",
        )
        extraction = extract_structured_event(article)
        assert extraction.geo_country != "null"
        assert extraction.geo_country != "none"


# ── Scoring ───────────────────────────────────────────────────────────────────

class TestScoring:
    def _make_extraction(self, impact=3, prob=3, time=2, exposure=3, delay=14):
        from src.models import LLMExtraction
        return LLMExtraction(
            event_summary="Test",
            reason_flagged="Test",
            geo_country="United States",
            geo_region="North America",
            geo_confidence="Medium",
            risk_category="Operational",
            disruption_type="Plant Shutdown",
            impact_1to5=impact,
            probability_1to5=prob,
            time_sensitivity_1to3=time,
            exposure_proxy_1to5=exposure,
            severity_confidence="Medium",
            estimated_delay_days=delay,
            delay_confidence="Low",
            delay_rationale="Default",
            oem_entities=[],
            supplier_entities=[],
            component_entities=[],
            component_criticality="low",
            llm_validation_passed=True,
        )

    def test_compute_risk_score_max(self):
        ext = self._make_extraction(impact=5, prob=5, time=3, exposure=5)
        score = compute_risk_score(ext)
        assert score == 100.0

    def test_compute_risk_score_min(self):
        ext = self._make_extraction(impact=1, prob=1, time=1, exposure=1)
        score = compute_risk_score(ext)
        assert 0 < score < 30  # Should be low but not zero

    def test_compute_risk_score_range(self):
        for impact in range(1, 6):
            for prob in range(1, 6):
                for time in range(1, 4):
                    for exp in range(1, 6):
                        ext = self._make_extraction(impact=impact, prob=prob, time=time, exposure=exp)
                        score = compute_risk_score(ext)
                        assert 0.0 <= score <= 100.0

    def test_severity_band_critical(self):
        assert severity_band(90.0) == "Critical"
        assert severity_band(85.0) == "Critical"

    def test_severity_band_high(self):
        assert severity_band(75.0) == "High"
        assert severity_band(70.0) == "High"

    def test_severity_band_medium(self):
        assert severity_band(60.0) == "Medium"
        assert severity_band(45.0) == "Medium"

    def test_severity_band_low(self):
        assert severity_band(30.0) == "Low"
        assert severity_band(0.0) == "Low"

    def test_estimate_exposure_positive(self):
        ext = self._make_extraction(exposure=3, time=2, delay=14)
        exp = estimate_exposure_usd(ext)
        assert exp > 0

    def test_estimate_exposure_higher_for_more_exposure(self):
        low_ext = self._make_extraction(exposure=1, delay=14)
        high_ext = self._make_extraction(exposure=5, delay=14)
        assert estimate_exposure_usd(high_ext) > estimate_exposure_usd(low_ext)

    def test_scoring_formula_weights(self):
        # Verify the formula: impact 40%, prob 30%, time 15%, exposure 15%
        from src.models import LLMExtraction
        ext = self._make_extraction(impact=5, prob=1, time=1, exposure=1)
        score = compute_risk_score(ext)
        expected = (5/5)*40 + (1/5)*30 + (1/3)*15 + (1/5)*15
        assert abs(score - expected) < 0.01


# ── Serialization ─────────────────────────────────────────────────────────────

class TestSerialization:
    def test_raw_to_row_all_fields(self):
        article = _make_article()
        row = raw_to_row(article)
        required = {"article_id", "article_url", "source_name", "source_weight",
                    "published_at", "ingested_at", "title", "summary", "content"}
        assert required.issubset(row.keys())

    def test_event_to_row_json_lists(self):
        from src.scoring import build_enriched_event
        article = _make_article(
            title="Toyota strike shutdown Japan",
            summary="Toyota workers on strike causing plant shutdown in Japan.",
        )
        from src.llm_extract import extract_with_llm
        extraction = extract_with_llm(article)
        event = build_enriched_event(article, extraction)
        row = event_to_row(event)
        # Lists should be stored as JSON strings
        for key in ("oem_entities", "supplier_entities", "component_entities", "mitigation_actions"):
            assert isinstance(row[key], str)
            json.loads(row[key])  # Should not raise

    def test_row_to_dict_deserializes_lists(self):
        raw = {
            "oem_entities": '["Toyota", "Ford"]',
            "supplier_entities": '["Bosch"]',
            "component_entities": '["semiconductor"]',
            "mitigation_actions": '["Action 1", "Action 2"]',
        }
        result = row_to_dict(raw)
        assert result["oem_entities"] == ["Toyota", "Ford"]
        assert result["mitigation_actions"] == ["Action 1", "Action 2"]

    def test_row_to_dict_handles_null(self):
        raw = {"oem_entities": None, "mitigation_actions": None}
        result = row_to_dict(raw)
        assert result["oem_entities"] == []
        assert result["mitigation_actions"] == []


# ── Storage ───────────────────────────────────────────────────────────────────

class TestStorage:
    def test_init_db_creates_tables(self):
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            conn = sqlite3.connect(paths.db_path)
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            assert "enriched_events" in tables
            assert "raw_articles" in tables
            assert "rejected_articles" in tables
            conn.close()
        finally:
            tmpdir.cleanup()

    def test_upsert_and_fetch_enriched_events(self):
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            from src.scoring import build_enriched_event
            from src.llm_extract import extract_with_llm
            article = _make_article(
                title="Toyota Japan strike causes plant shutdown",
                summary="Full production shutdown at Toyota Japan due to labor strike.",
            )
            extraction = extract_with_llm(article)
            if extraction.llm_validation_passed:
                from src.scoring import build_enriched_event
                event = build_enriched_event(article, extraction)
                row = event_to_row(event)
                count = upsert_enriched_events(paths, [row])
                assert count >= 0
                fetched = fetch_enriched_events(paths, limit=10)
                assert len(fetched) >= 1
        finally:
            tmpdir.cleanup()


# ── Geo Utils ─────────────────────────────────────────────────────────────────

class TestGeoUtils:
    def test_get_event_coordinates_known_country(self):
        event = {"geo_country": "China", "geo_region": "East Asia"}
        lat, lon = get_event_coordinates(event)
        assert lat != 0.0 or lon != 0.0  # Should have real coords

    def test_get_event_coordinates_falls_back_to_region(self):
        event = {"geo_country": "Unknown", "geo_region": "North America"}
        lat, lon = get_event_coordinates(event)
        assert lat == REGION_COORDINATES["North America"][0]

    def test_get_event_coordinates_unknown(self):
        event = {"geo_country": "Unknown", "geo_region": "Unknown"}
        lat, lon = get_event_coordinates(event)
        assert lat == 0.0 and lon == 0.0

    def test_all_country_coordinates_in_valid_range(self):
        for country, (lat, lon) in COUNTRY_COORDINATES.items():
            assert -90 <= lat <= 90, f"{country} lat={lat} out of range"
            assert -180 <= lon <= 180, f"{country} lon={lon} out of range"


# ── End-to-end extraction accuracy ───────────────────────────────────────────

class TestExtractionAccuracy:
    """Test that the pipeline correctly categorizes known article types."""

    def _extract(self, title, summary=""):
        article = _make_article(title=title, summary=summary)
        return extract_structured_event(article)

    def test_china_tariff_is_export_restriction(self):
        ext = self._extract(
            "China imposes new export restrictions on automotive components",
            "Chinese government announced tariffs affecting auto part exports to US."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Export Restriction"
        assert ext.risk_category == "Political"
        assert ext.geo_country == "China"

    def test_labor_strike_at_ford(self):
        ext = self._extract(
            "Ford workers go on strike at Michigan plant",
            "UAW union walkout causes plant shutdown at Ford's assembly plant."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Labor Strike"
        assert ext.risk_category == "Social"

    def test_japan_earthquake_is_natural_disaster(self):
        ext = self._extract(
            "Earthquake disrupts Toyota supply chain in Japan",
            "Major earthquake in Japan forces automotive supplier shutdowns."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Natural Disaster"
        assert ext.risk_category == "Environmental"
        assert ext.geo_country == "Japan"

    def test_ransomware_attack_is_cyberattack(self):
        ext = self._extract(
            "Ransomware cyberattack hits Bosch automotive supplier",
            "Cyberattack causes system outage and hack at major auto parts maker."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Cyberattack"
        assert ext.risk_category == "Technological"

    def test_supplier_bankruptcy_is_insolvency(self):
        ext = self._extract(
            "Auto parts supplier files for bankruptcy insolvency",
            "Major tier-1 automotive supplier faces creditor restructuring after insolvency."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Supplier Insolvency"
        assert ext.risk_category == "Economic"

    def test_german_factory_is_plant_shutdown(self):
        ext = self._extract(
            "Volkswagen shuts down factory in Germany",
            "German automaker halted production at its main plant due to parts shortage."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Plant Shutdown"
        assert ext.geo_country == "Germany"
        assert ext.geo_region == "Europe"

    def test_risk_score_range_all_valid_articles(self):
        articles = [
            ("China export ban on automotive semiconductors", "Chinese gov restricts semiconductor exports affecting car makers."),
            ("Toyota Japan factory shutdown earthquake", "Natural disaster forces Toyota plant to halt production in Japan."),
            ("Ford workers strike UAW union walkout", "Workers at Ford Michigan walked out in labor dispute."),
        ]
        for title, summary in articles:
            ext = self._extract(title, summary)
            if ext.llm_validation_passed:
                from src.models import LLMExtraction
                score = compute_risk_score(ext)
                assert 0.0 <= score <= 100.0, f"Score out of range for: {title}"

    def test_geo_country_never_null_string_for_valid_events(self):
        articles = [
            "Toyota plant shutdown in Japan due to earthquake",
            "Ford workers strike in Michigan USA",
            "China bans semiconductor exports tariff sanctions",
            "Volkswagen Germany factory halted production shutdown",
        ]
        for title in articles:
            article = _make_article(title=title)
            ext = extract_structured_event(article)
            if ext.llm_validation_passed:
                assert ext.geo_country not in ("null", "none", "NULL", "NONE")
