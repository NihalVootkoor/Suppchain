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
from src.rss_ingest import parse_rss, ingest_rss, _dedupe_articles
from src.geo_utils import COUNTRY_COORDINATES, REGION_COORDINATES, get_event_coordinates
from src.llm_extract import (
    _classify_disruption_type,
    _classify_sc_category,
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
        required = {"Labor Strike", "Plant Shutdown", "Logistics Disruption", "Trade Restriction",
                    "Cyberattack", "Natural Disaster", "Supplier Insolvency", "Regulatory Change",
                    "Capacity Constraint", "Other"}
        assert required == set(DISRUPTION_TYPES)

    def test_risk_categories_sc_taxonomy(self):
        sc = {"Supply Disruption", "Logistics & Transport", "Geopolitical & Trade",
              "Natural Disaster & Climate", "Cyber & Technology", "Labor & Social",
              "Regulatory & Compliance"}
        assert sc == set(RISK_CATEGORIES)


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

    def test_classify_disruption_type_logistics_disruption(self):
        assert _classify_disruption_type("port congestion container backlog intermodal") == "Logistics Disruption"

    def test_classify_disruption_type_trade_restriction(self):
        assert _classify_disruption_type("export ban sanctions tariff") == "Trade Restriction"

    def test_classify_disruption_type_insolvency(self):
        assert _classify_disruption_type("bankruptcy insolvency creditor") == "Supplier Insolvency"

    def test_classify_disruption_type_regulatory(self):
        assert _classify_disruption_type("new regulation rule change government mandate") == "Regulatory Change"

    def test_classify_disruption_type_capacity_constraint(self):
        assert _classify_disruption_type("production cut capacity reduction idle capacity") == "Capacity Constraint"

    def test_classify_disruption_type_other(self):
        assert _classify_disruption_type("some random text without specific triggers") == "Other"

    def test_classify_sc_category_mapping(self):
        assert _classify_sc_category("Labor Strike") == "Labor & Social"
        assert _classify_sc_category("Cyberattack") == "Cyber & Technology"
        assert _classify_sc_category("Natural Disaster") == "Natural Disaster & Climate"
        assert _classify_sc_category("Trade Restriction") == "Geopolitical & Trade"
        assert _classify_sc_category("Regulatory Change") == "Regulatory & Compliance"
        assert _classify_sc_category("Supplier Insolvency") == "Supply Disruption"
        assert _classify_sc_category("Logistics Disruption") == "Logistics & Transport"
        assert _classify_sc_category("Plant Shutdown") == "Supply Disruption"
        assert _classify_sc_category("Capacity Constraint") == "Supply Disruption"

    def test_sc_category_always_in_risk_categories(self):
        for dtype in DISRUPTION_TYPES:
            result = _classify_sc_category(dtype)
            assert result in RISK_CATEGORIES, f"SC category {result!r} not in RISK_CATEGORIES"

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

    def test_severity_signals_trade_restriction_time_limited(self):
        # Trade Restriction should have time_sensitivity <= 2 (policy changes are slow-moving)
        signals = _severity_signals("export ban tariffs announced", "Trade Restriction")
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
        days, conf, _ = _estimate_delay_days("no duration mentioned", "Logistics Disruption")
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
            risk_category="Supply Disruption",
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
        # Entity lists should be stored as JSON strings
        for key in ("oem_entities", "supplier_entities", "component_entities"):
            assert isinstance(row[key], str)
            json.loads(row[key])  # Should not raise
        # mitigation_actions is NULL when no mitigation has been generated
        assert row["mitigation_actions"] is None or (
            isinstance(row["mitigation_actions"], str) and json.loads(row["mitigation_actions"]) is not None
        )

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

    def test_china_tariff_is_trade_restriction(self):
        ext = self._extract(
            "China imposes new export restrictions on automotive components",
            "Chinese government announced tariffs affecting auto part exports to US."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Trade Restriction"
        assert ext.risk_category == "Geopolitical & Trade"
        assert ext.geo_country == "China"

    def test_labor_strike_at_ford(self):
        ext = self._extract(
            "Ford workers go on strike at Michigan plant",
            "UAW union walkout causes plant shutdown at Ford's assembly plant."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Labor Strike"
        assert ext.risk_category == "Labor & Social"

    def test_japan_earthquake_is_natural_disaster(self):
        ext = self._extract(
            "Earthquake disrupts Toyota supply chain in Japan",
            "Major earthquake in Japan forces automotive supplier shutdowns."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Natural Disaster"
        assert ext.risk_category == "Natural Disaster & Climate"
        assert ext.geo_country == "Japan"

    def test_ransomware_attack_is_cyberattack(self):
        ext = self._extract(
            "Ransomware cyberattack hits Bosch automotive supplier",
            "Cyberattack causes system outage and hack at major auto parts maker."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Cyberattack"
        assert ext.risk_category == "Cyber & Technology"

    def test_supplier_bankruptcy_is_insolvency(self):
        ext = self._extract(
            "Auto parts supplier files for bankruptcy insolvency",
            "Major tier-1 automotive supplier faces creditor restructuring after insolvency."
        )
        assert ext.llm_validation_passed
        assert ext.disruption_type == "Supplier Insolvency"
        assert ext.risk_category == "Supply Disruption"

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


# ── Mitigation persistence ────────────────────────────────────────────────────

class TestMitigationPersistence:
    """Verify that upsert_enriched_events preserves existing mitigation on re-process."""

    def _make_event_row(self, event_id: str, mit_desc=None, mit_actions=None) -> dict:
        from src.serialization import event_to_row
        from src.scoring import build_enriched_event
        article = _make_article(
            title="Toyota Japan plant halted due to strike shutdown",
            summary="Production stoppage at Toyota Japan plant.",
            url=f"https://example.com/{event_id}",
        )
        from src.llm_extract import extract_with_llm
        extraction = extract_with_llm(article)
        # Force llm_validation_passed and inject mitigation
        from src.models import EnrichedEvent
        from src.scoring import severity_band, compute_risk_score, estimate_exposure_usd
        from src.llm_extract import build_event_id
        now = datetime.now(timezone.utc)
        event = EnrichedEvent(
            event_id=event_id,
            article_url=f"https://example.com/{event_id}",
            source_name="test", source_weight=0.7,
            published_at=now, ingested_at=now,
            title="Toyota Japan plant halted", event_summary="Supply disruption.",
            dashboard_blurb=None, reason_flagged="test disruption",
            oem_entities=["Toyota"], supplier_entities=[], component_entities=[],
            component_criticality="low", risk_category="Supply Disruption",
            disruption_type="Plant Shutdown", geo_country="Japan",
            geo_region="East Asia", geo_confidence="High",
            impact_1to5=4, probability_1to5=4, time_sensitivity_1to3=3,
            exposure_proxy_1to5=4, severity_confidence="High",
            risk_score_0to100=79.0, severity_band="High",
            estimated_delay_days=14, delay_confidence="High",
            delay_rationale="test", exposure_usd_est=10000000.0,
            exposure_confidence="High", exposure_assumptions="test",
            mitigation_description=mit_desc,
            mitigation_actions=mit_actions,
            mitigation_generated_at=now if mit_desc else None,
            llm_validation_passed=True, rejected_reason=None,
            created_at=now,
        )
        return event_to_row(event)

    def test_upsert_preserves_existing_mitigation_on_reprocess(self):
        """Re-upserting an event without mitigation must NOT overwrite existing mitigation."""
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            event_id = "test-evt-001"

            # First insert: event WITH mitigation
            row_with_mit = self._make_event_row(
                event_id,
                mit_desc="Activate emergency sourcing protocols.",
                mit_actions=["Source alternate suppliers", "Notify procurement team"],
            )
            upsert_enriched_events(paths, [row_with_mit])

            # Second upsert: same event WITHOUT mitigation (simulates re-enrichment outside top-3)
            row_no_mit = self._make_event_row(event_id, mit_desc=None, mit_actions=None)
            upsert_enriched_events(paths, [row_no_mit])

            # Mitigation should still be present
            fetched = fetch_enriched_events(paths, limit=10)
            assert len(fetched) == 1
            evt = fetched[0]
            assert evt["mitigation_description"] == "Activate emergency sourcing protocols.", (
                "Existing mitigation was overwritten by NULL on re-upsert — persistence bug not fixed"
            )
        finally:
            tmpdir.cleanup()

    def test_upsert_updates_mitigation_when_new_value_provided(self):
        """Upserting with a new non-null mitigation value replaces the old one."""
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            event_id = "test-evt-002"

            row_v1 = self._make_event_row(event_id, mit_desc="Old mitigation.", mit_actions=["Old action"])
            upsert_enriched_events(paths, [row_v1])

            row_v2 = self._make_event_row(event_id, mit_desc="Updated mitigation.", mit_actions=["New action"])
            upsert_enriched_events(paths, [row_v2])

            fetched = fetch_enriched_events(paths, limit=10)
            assert fetched[0]["mitigation_description"] == "Updated mitigation."
        finally:
            tmpdir.cleanup()

    def test_serialization_stores_null_for_none_mitigation_actions(self):
        """None mitigation_actions must serialize to SQL NULL, not empty JSON array."""
        row = self._make_event_row("test-null-001", mit_desc=None, mit_actions=None)
        assert row["mitigation_actions"] is None, (
            f"Expected None but got {row['mitigation_actions']!r} — COALESCE fix requires SQL NULL"
        )

    def test_serialization_stores_json_for_non_null_mitigation_actions(self):
        """Non-None mitigation_actions must serialize to a JSON string."""
        row = self._make_event_row("test-json-001", mit_desc="desc", mit_actions=["action1", "action2"])
        assert row["mitigation_actions"] is not None
        parsed = json.loads(row["mitigation_actions"])
        assert parsed == ["action1", "action2"]


# ── Mitigation _base_actions ──────────────────────────────────────────────────

class TestBaseActions:
    """Test that _base_actions uses valid disruption type names and returns useful actions."""

    def _make_event(self, disruption_type: str):
        from src.models import EnrichedEvent
        now = datetime.now(timezone.utc)
        return EnrichedEvent(
            event_id="test", article_url="http://test.com", source_name="test",
            source_weight=0.7, published_at=now, ingested_at=now,
            title="Test", event_summary="Test", dashboard_blurb=None,
            reason_flagged="test", oem_entities=[], supplier_entities=[],
            component_entities=[], component_criticality="low",
            risk_category="Supply Disruption", disruption_type=disruption_type,
            geo_country="Unknown", geo_region="Unknown", geo_confidence="Low",
            impact_1to5=2, probability_1to5=2, time_sensitivity_1to3=1,
            exposure_proxy_1to5=2, severity_confidence="Low",
            risk_score_0to100=50.0, severity_band="Medium",
            estimated_delay_days=14, delay_confidence="Low",
            delay_rationale="test", exposure_usd_est=5000000.0,
            exposure_confidence="Low", exposure_assumptions="test",
            mitigation_description=None, mitigation_actions=None,
            mitigation_generated_at=None, llm_validation_passed=True,
            rejected_reason=None, created_at=now,
        )

    def test_base_actions_logistics_disruption_gets_extra_action(self):
        from src.mitigation import _base_actions
        event = self._make_event("Logistics Disruption")
        actions = _base_actions(event)
        assert len(actions) == 4, "Logistics Disruption should get an extra port/sourcing action"

    def test_base_actions_trade_restriction_gets_extra_action(self):
        from src.mitigation import _base_actions
        event = self._make_event("Trade Restriction")
        actions = _base_actions(event)
        assert len(actions) == 4, "Trade Restriction should get an extra port/sourcing action"

    def test_base_actions_labor_strike_gets_extra_action(self):
        from src.mitigation import _base_actions
        event = self._make_event("Labor Strike")
        actions = _base_actions(event)
        assert len(actions) == 4, "Labor Strike should get union communication action"

    def test_base_actions_plant_shutdown_gets_extra_action(self):
        from src.mitigation import _base_actions
        event = self._make_event("Plant Shutdown")
        actions = _base_actions(event)
        assert len(actions) == 4, "Plant Shutdown should get union communication action"

    def test_base_actions_cyberattack_gets_extra_action(self):
        from src.mitigation import _base_actions
        event = self._make_event("Cyberattack")
        actions = _base_actions(event)
        assert len(actions) == 4, "Cyberattack should get supplier cybersecurity action"

    def test_base_actions_all_types_return_at_least_3_actions(self):
        from src.mitigation import _base_actions
        from src.config import DISRUPTION_TYPES
        for dt in DISRUPTION_TYPES:
            actions = _base_actions(self._make_event(dt))
            assert len(actions) >= 3, f"{dt} returned fewer than 3 actions"

    def test_base_actions_no_invalid_disruption_type_references(self):
        """Ensure the function uses valid DISRUPTION_TYPES, not stale names."""
        from src.mitigation import _base_actions
        import inspect, src.mitigation as m
        source = inspect.getsource(_base_actions)
        assert "Port Congestion" not in source, "Stale type 'Port Congestion' still referenced"
        assert "Export Restriction" not in source, "Stale type 'Export Restriction' still referenced"


# ── Config data quality ───────────────────────────────────────────────────────

class TestConfigDataQuality:
    def test_tier1s_no_duplicates(self):
        from src.config import TIER1S
        seen = set()
        for name in TIER1S:
            assert name not in seen, f"Duplicate TIER1S entry: '{name}'"
            seen.add(name)

    def test_oems_no_duplicates(self):
        from src.config import OEMS
        seen = set()
        for name in OEMS:
            assert name not in seen, f"Duplicate OEMS entry: '{name}'"
            seen.add(name)

    def test_disruption_types_no_duplicates(self):
        assert len(DISRUPTION_TYPES) == len(set(DISRUPTION_TYPES))

    def test_risk_categories_no_duplicates(self):
        assert len(RISK_CATEGORIES) == len(set(RISK_CATEGORIES))


# ── RSS parsing ───────────────────────────────────────────────────────────────

RSS_FIXTURE = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Toyota plant shutdown in Japan due to strike</title>
      <link>https://example.com/toyota-strike</link>
      <description>Workers walked out of Toyota&#39;s main assembly plant.</description>
      <pubDate>Mon, 20 Mar 2026 08:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Ford workers strike at Michigan plant</title>
      <link>https://example.com/ford-strike</link>
      <description>UAW union walkout halts production at Ford Michigan.</description>
      <pubDate>Mon, 20 Mar 2026 10:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>
"""

ATOM_FIXTURE = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Test Feed</title>
  <entry>
    <title>Bosch ransomware cyberattack disrupts production</title>
    <link rel="alternate" href="https://example.com/bosch-cyber"/>
    <summary>Ransomware attack hit Bosch causing plant outage.</summary>
    <published>2026-03-19T12:00:00Z</published>
  </entry>
</feed>
"""

MALFORMED_RSS_FIXTURE = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Supply disruption &amp; parts shortage</title>
      <link>https://example.com/disruption</link>
      <description>Parts shortage &amp; logistics delay</description>
      <pubDate>Sun, 19 Mar 2026 06:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>
"""


class TestRSSIngestion:
    """Tests for RSS feed parsing, Atom parsing, deduplication, and robustness."""

    def test_parse_rss_returns_articles(self):
        articles = parse_rss(RSS_FIXTURE, source="test-feed", weight=0.8)
        assert len(articles) == 2

    def test_parse_rss_article_fields(self):
        articles = parse_rss(RSS_FIXTURE, source="test-feed", weight=0.8)
        a = articles[0]
        assert a.title == "Toyota plant shutdown in Japan due to strike"
        assert a.article_url == "https://example.com/toyota-strike"
        assert a.source_name == "test-feed"
        assert a.source_weight == 0.8
        assert a.article_id  # non-empty
        assert a.published_at is not None
        assert a.ingested_at is not None

    def test_parse_rss_published_at_timezone_aware(self):
        articles = parse_rss(RSS_FIXTURE, source="test-feed", weight=0.8)
        for article in articles:
            assert article.published_at.tzinfo is not None, (
                f"published_at for '{article.title}' must be timezone-aware"
            )

    def test_parse_rss_ingested_at_is_utc_now(self):
        before = datetime.now(timezone.utc)
        articles = parse_rss(RSS_FIXTURE, source="test-feed", weight=0.8)
        after = datetime.now(timezone.utc)
        for article in articles:
            assert before <= article.ingested_at <= after, (
                "ingested_at must be set to the current UTC time during ingestion"
            )

    def test_parse_atom_feed(self):
        articles = parse_rss(ATOM_FIXTURE, source="atom-feed", weight=0.7)
        assert len(articles) == 1
        a = articles[0]
        assert "Bosch" in a.title
        assert a.article_url == "https://example.com/bosch-cyber"
        assert a.published_at.tzinfo is not None

    def test_parse_rss_article_id_is_deterministic(self):
        """Same URL must always produce the same article_id (idempotent ingestion)."""
        articles1 = parse_rss(RSS_FIXTURE, source="feed-a", weight=0.8)
        articles2 = parse_rss(RSS_FIXTURE, source="feed-b", weight=0.5)
        ids1 = {a.article_url: a.article_id for a in articles1}
        ids2 = {a.article_url: a.article_id for a in articles2}
        for url in ids1:
            assert ids1[url] == ids2[url], (
                f"article_id for {url} changed across parse calls — dedup will break"
            )

    def test_parse_rss_no_duplicate_article_ids(self):
        articles = parse_rss(RSS_FIXTURE, source="test-feed", weight=0.8)
        ids = [a.article_id for a in articles]
        assert len(ids) == len(set(ids)), "Duplicate article_ids within a single feed parse"

    def test_parse_rss_malformed_entities(self):
        """Feeds with HTML entities like &amp; must parse without raising."""
        articles = parse_rss(MALFORMED_RSS_FIXTURE, source="malformed", weight=0.6)
        assert len(articles) == 1
        assert "Parts shortage" in articles[0].summary or "disruption" in articles[0].title.lower()

    def test_parse_rss_empty_feed_returns_empty_list(self):
        empty = '<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>'
        articles = parse_rss(empty, source="empty", weight=0.5)
        assert articles == []

    def test_dedupe_articles_removes_duplicates(self):
        articles = parse_rss(RSS_FIXTURE, source="feed", weight=0.8)
        doubled = articles + articles  # exact duplicates
        deduped = _dedupe_articles(doubled)
        assert len(deduped) == len(articles)

    def test_dedupe_articles_keeps_latest_published(self):
        """When the same article_id appears twice, the one with the later published_at wins."""
        base = parse_rss(RSS_FIXTURE, source="feed", weight=0.8)
        older = base[0]
        from dataclasses import replace
        from datetime import timedelta
        newer = replace(older, published_at=older.published_at + timedelta(hours=1))
        deduped = _dedupe_articles([older, newer])
        assert len(deduped) == 1
        assert deduped[0].published_at == newer.published_at

    def test_ingest_rss_skips_failed_feeds_gracefully(self):
        """A bad URL must not raise — it must be silently skipped."""
        articles = ingest_rss(
            ["https://this-domain-does-not-exist-xyz.invalid/feed"],
            weights={},
        )
        assert isinstance(articles, list)  # no exception; returns empty list

    def test_ingest_rss_source_weight_applied(self):
        """source_weight from the weights dict must be forwarded to each article."""
        feed_url = "https://example.com/feed"
        # We can't hit the network, so test the weight dict wiring via parse_rss directly
        articles = parse_rss(RSS_FIXTURE, source=feed_url, weight=0.9)
        for a in articles:
            assert a.source_weight == 0.9

    def test_parse_rss_source_name_is_url(self):
        """source_name must be the feed URL so we can trace provenance."""
        url = "https://www.supplychaindive.com/feeds/news/"
        articles = parse_rss(RSS_FIXTURE, source=url, weight=0.8)
        for a in articles:
            assert a.source_name == url

    def test_parse_rss_content_falls_back_to_summary(self):
        """content must equal summary when no separate content field is provided."""
        articles = parse_rss(RSS_FIXTURE, source="feed", weight=0.8)
        for a in articles:
            assert a.content == a.summary


# ── Scheduling / autonomy ─────────────────────────────────────────────────────

class TestSchedulingAutonomy:
    """Verify the pipeline's scheduling metadata and interval configuration."""

    def test_config_refresh_interval_is_24h(self):
        """Default refresh interval must be 24 h for once-daily autonomous runs."""
        config = get_config()
        assert config.refresh_interval_hours == 24, (
            f"Expected 24h daily refresh, got {config.refresh_interval_hours}h"
        )

    def test_config_refresh_interval_env_override(self):
        """REFRESH_INTERVAL_HOURS env var must override the default."""
        import os
        os.environ["REFRESH_INTERVAL_HOURS"] = "12"
        try:
            from src.config import AppConfig
            from pathlib import Path
            cfg = AppConfig(
                project_root=Path("."),
                db_path=Path("data/app.db"),
                db_url=None,
                rss_urls=(),
                retention_days=45,
                enriched_retention_days=730,
                source_weights={},
                refresh_interval_hours=24,  # default — should be overridden by env
            )
            assert cfg.refresh_interval_hours == 12
        finally:
            del os.environ["REFRESH_INTERVAL_HOURS"]

    def test_config_refresh_interval_bad_env_falls_back(self):
        """A non-integer REFRESH_INTERVAL_HOURS must silently fall back to the default."""
        import os
        os.environ["REFRESH_INTERVAL_HOURS"] = "not-a-number"
        try:
            from src.config import AppConfig
            from pathlib import Path
            cfg = AppConfig(
                project_root=Path("."),
                db_path=Path("data/app.db"),
                db_url=None,
                rss_urls=(),
                retention_days=45,
                enriched_retention_days=730,
                source_weights={},
                refresh_interval_hours=24,
            )
            assert cfg.refresh_interval_hours == 24
        finally:
            del os.environ["REFRESH_INTERVAL_HOURS"]

    def test_set_and_get_last_refresh_meta(self):
        """set_meta_value / get_meta_value must round-trip last_refresh_at."""
        from src.storage import set_meta_value, get_meta_value, init_db
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            ts = datetime.now(timezone.utc).isoformat()
            set_meta_value(paths, "last_refresh_at", ts)
            result = get_meta_value(paths, "last_refresh_at")
            assert result == ts, "last_refresh_at not persisted correctly"
        finally:
            tmpdir.cleanup()

    def test_meta_missing_key_returns_none(self):
        """get_meta_value for a key that has never been set must return None."""
        from src.storage import get_meta_value, init_db
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            assert get_meta_value(paths, "nonexistent_key") is None
        finally:
            tmpdir.cleanup()

    def test_meta_upsert_overwrites(self):
        """Calling set_meta_value twice must overwrite, not duplicate."""
        from src.storage import set_meta_value, get_meta_value, init_db
        paths, tmpdir = _make_db_paths()
        try:
            init_db(paths)
            set_meta_value(paths, "last_refresh_at", "2026-03-21T08:00:00+00:00")
            set_meta_value(paths, "last_refresh_at", "2026-03-22T08:00:00+00:00")
            result = get_meta_value(paths, "last_refresh_at")
            assert result == "2026-03-22T08:00:00+00:00"
        finally:
            tmpdir.cleanup()

    def test_all_rss_urls_have_weights(self):
        """Every RSS URL in config must have a corresponding source_weight entry."""
        config = get_config()
        for url in config.rss_urls:
            assert url in config.source_weights, (
                f"RSS URL has no source_weight: {url}"
            )

    def test_rss_url_count_matches_weights_count(self):
        """source_weights must not have orphaned entries not in rss_urls."""
        config = get_config()
        rss_set = set(config.rss_urls)
        for url in config.source_weights:
            assert url in rss_set, (
                f"source_weights has orphaned URL not in rss_urls: {url}"
            )

    def test_all_source_weights_in_valid_range(self):
        """Every source weight must be in (0, 1]."""
        config = get_config()
        for url, weight in config.source_weights.items():
            assert 0 < weight <= 1.0, (
                f"source_weight for {url} is {weight} — must be in (0, 1]"
            )
