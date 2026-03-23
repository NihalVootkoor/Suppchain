"""Storage helpers and migrations for SQLite/Postgres."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

try:  # Optional dependency for Supabase/Postgres
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psycopg2 = None
    RealDictCursor = None

SCHEMA_VERSION = 2

RAW_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS raw_articles ("
    "article_id TEXT PRIMARY KEY, article_url TEXT NOT NULL, "
    "source_name TEXT NOT NULL, source_weight REAL NOT NULL, "
    "published_at TEXT NOT NULL, ingested_at TEXT NOT NULL, "
    "title TEXT NOT NULL, summary TEXT NOT NULL, content TEXT NOT NULL)"
)
ENRICHED_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS enriched_events ("
    "event_id TEXT PRIMARY KEY, article_url TEXT NOT NULL, source_name TEXT NOT NULL, "
    "source_weight REAL NOT NULL, published_at TEXT NOT NULL, ingested_at TEXT NOT NULL, "
    "title TEXT NOT NULL, event_summary TEXT NOT NULL, dashboard_blurb TEXT, "
    "reason_flagged TEXT NOT NULL, oem_entities TEXT NOT NULL, supplier_entities TEXT NOT NULL, "
    "component_entities TEXT NOT NULL, component_criticality TEXT NOT NULL, "
    "risk_category TEXT NOT NULL, disruption_type TEXT NOT NULL, geo_country TEXT NOT NULL, "
    "geo_region TEXT NOT NULL, geo_confidence TEXT NOT NULL, impact_1to5 INTEGER NOT NULL, "
    "probability_1to5 INTEGER NOT NULL, time_sensitivity_1to3 INTEGER NOT NULL, "
    "exposure_proxy_1to5 INTEGER NOT NULL, severity_confidence TEXT NOT NULL, "
    "risk_score_0to100 REAL NOT NULL, severity_band TEXT NOT NULL, "
    "estimated_delay_days INTEGER NOT NULL, delay_confidence TEXT NOT NULL, "
    "delay_rationale TEXT NOT NULL, exposure_usd_est REAL NOT NULL, "
    "exposure_confidence TEXT NOT NULL, exposure_assumptions TEXT NOT NULL, "
    "mitigation_description TEXT, mitigation_actions TEXT, mitigation_generated_at TEXT, "
    "llm_validation_passed INTEGER NOT NULL, rejected_reason TEXT, created_at TEXT NOT NULL)"
)
REJECTED_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS rejected_articles ("
    "article_url TEXT PRIMARY KEY, reason TEXT NOT NULL, created_at TEXT NOT NULL)"
)
LLM_REJECTED_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS llm_rejected_events ("
    "event_id TEXT PRIMARY KEY, article_url TEXT NOT NULL, source_name TEXT NOT NULL, "
    "source_weight REAL NOT NULL, published_at TEXT NOT NULL, ingested_at TEXT NOT NULL, "
    "title TEXT NOT NULL, event_summary TEXT NOT NULL, dashboard_blurb TEXT, "
    "reason_flagged TEXT NOT NULL, oem_entities TEXT NOT NULL, supplier_entities TEXT NOT NULL, "
    "component_entities TEXT NOT NULL, component_criticality TEXT NOT NULL, "
    "risk_category TEXT NOT NULL, disruption_type TEXT NOT NULL, geo_country TEXT NOT NULL, "
    "geo_region TEXT NOT NULL, geo_confidence TEXT NOT NULL, impact_1to5 INTEGER NOT NULL, "
    "probability_1to5 INTEGER NOT NULL, time_sensitivity_1to3 INTEGER NOT NULL, "
    "exposure_proxy_1to5 INTEGER NOT NULL, severity_confidence TEXT NOT NULL, "
    "risk_score_0to100 REAL NOT NULL, severity_band TEXT NOT NULL, "
    "estimated_delay_days INTEGER NOT NULL, delay_confidence TEXT NOT NULL, "
    "delay_rationale TEXT NOT NULL, exposure_usd_est REAL NOT NULL, "
    "exposure_confidence TEXT NOT NULL, exposure_assumptions TEXT NOT NULL, "
    "mitigation_description TEXT, mitigation_actions TEXT, mitigation_generated_at TEXT, "
    "llm_validation_passed INTEGER NOT NULL, rejected_reason TEXT, created_at TEXT NOT NULL)"
)


@dataclass(frozen=True)
class DbPaths:
    """Convenience paths for the database."""

    db_path: Path
    db_url: Optional[str] = None


def _use_postgres(paths: DbPaths) -> bool:
    return bool(paths.db_url)


def _require_postgres() -> None:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for Postgres/Supabase support.")


def get_sqlite_connection(db_path: Path) -> sqlite3.Connection:
    """Create a SQLite connection with row access by name."""

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_pg_connection(db_url: str) -> Any:
    """Create a Postgres connection."""

    _require_postgres()
    return psycopg2.connect(db_url)


def get_connection(paths: DbPaths):
    """Return a database connection for the configured backend."""

    if _use_postgres(paths):
        return get_pg_connection(str(paths.db_url))
    return get_sqlite_connection(paths.db_path)


def _get_schema_version_sqlite(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    return int(row["value"]) if row else 0


def _set_schema_version_sqlite(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
        ("schema_version", str(version)),
    )


def _get_schema_version_pg(cur) -> int:
    cur.execute("SELECT value FROM schema_meta WHERE key = %s", ("schema_version",))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _set_schema_version_pg(cur, version: int) -> None:
    cur.execute(
        "INSERT INTO schema_meta (key, value) VALUES (%s, %s) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        ("schema_version", str(version)),
    )


def init_db(paths: DbPaths) -> None:
    """Initialize database schema and run migrations."""

    if _use_postgres(paths):
        _require_postgres()
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                version = _get_schema_version_pg(cur)
                if version < 1:
                    cur.execute(RAW_TABLE_SQL)
                    cur.execute(ENRICHED_TABLE_SQL)
                    cur.execute(REJECTED_TABLE_SQL)
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_enriched_score ON enriched_events(risk_score_0to100 DESC)"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_enriched_published ON enriched_events(published_at DESC)"
                    )
                if version < SCHEMA_VERSION:
                    cur.execute(LLM_REJECTED_TABLE_SQL)
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_llm_rejected_score ON llm_rejected_events(risk_score_0to100 DESC)"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_llm_rejected_published ON llm_rejected_events(published_at DESC)"
                    )
                    _set_schema_version_pg(cur, SCHEMA_VERSION)
        return

    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    with get_connection(paths) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        version = _get_schema_version_sqlite(conn)
        if version < 1:
            conn.execute(RAW_TABLE_SQL)
            conn.execute(ENRICHED_TABLE_SQL)
            conn.execute(REJECTED_TABLE_SQL)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_enriched_score ON enriched_events(risk_score_0to100 DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_enriched_published ON enriched_events(published_at DESC)"
            )
        if version < SCHEMA_VERSION:
            conn.execute(LLM_REJECTED_TABLE_SQL)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_llm_rejected_score ON llm_rejected_events(risk_score_0to100 DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_llm_rejected_published ON llm_rejected_events(published_at DESC)"
            )
            # Migrate any existing llm_validation_passed=0 rows out of enriched_events
            conn.execute(
                "INSERT OR IGNORE INTO llm_rejected_events "
                "SELECT * FROM enriched_events WHERE llm_validation_passed = 0"
            )
            conn.execute("DELETE FROM enriched_events WHERE llm_validation_passed = 0")
            _set_schema_version_sqlite(conn, SCHEMA_VERSION)


def get_meta_value(paths: DbPaths, key: str) -> str | None:
    """Return a metadata value from schema_meta."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT value FROM schema_meta WHERE key = %s", (key,))
                row = cur.fetchone()
        return str(row["value"]) if row else None

    with get_connection(paths) as conn:
        row = conn.execute("SELECT value FROM schema_meta WHERE key = ?", (key,)).fetchone()
    if not row:
        return None
    return str(row["value"])


def set_meta_value(paths: DbPaths, key: str, value: str) -> None:
    """Upsert a metadata value into schema_meta."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO schema_meta (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (key, value),
                )
        return

    with get_connection(paths) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
            (key, value),
        )


def upsert_raw_articles(paths: DbPaths, rows: Iterable[dict[str, object]]) -> int:
    """Insert raw articles, ignoring existing IDs."""

    prepared = list(rows)
    if not prepared:
        return 0
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO raw_articles (article_id, article_url, source_name, "
                    "source_weight, published_at, ingested_at, title, summary, content) "
                    "VALUES (%(article_id)s, %(article_url)s, %(source_name)s, %(source_weight)s, "
                    "%(published_at)s, %(ingested_at)s, %(title)s, %(summary)s, %(content)s) "
                    "ON CONFLICT (article_id) DO NOTHING",
                    prepared,
                )
        return len(prepared)

    with get_connection(paths) as conn:
        cur = conn.executemany(
            "INSERT OR IGNORE INTO raw_articles (article_id, article_url, source_name, "
            "source_weight, published_at, ingested_at, title, summary, content) "
            "VALUES (:article_id, :article_url, :source_name, :source_weight, "
            ":published_at, :ingested_at, :title, :summary, :content)",
            prepared,
        )
        return cur.rowcount


def upsert_enriched_events(paths: DbPaths, rows: Iterable[dict[str, object]]) -> int:
    """Insert or replace enriched events."""

    prepared = list(rows)
    if not prepared:
        return 0
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO enriched_events (event_id, article_url, source_name, "
                    "source_weight, published_at, ingested_at, title, event_summary, dashboard_blurb, "
                    "reason_flagged, oem_entities, supplier_entities, component_entities, "
                    "component_criticality, risk_category, disruption_type, geo_country, geo_region, "
                    "geo_confidence, impact_1to5, probability_1to5, time_sensitivity_1to3, "
                    "exposure_proxy_1to5, severity_confidence, risk_score_0to100, severity_band, "
                    "estimated_delay_days, delay_confidence, delay_rationale, exposure_usd_est, "
                    "exposure_confidence, exposure_assumptions, mitigation_description, "
                    "mitigation_actions, mitigation_generated_at, llm_validation_passed, "
                    "rejected_reason, created_at) VALUES (%(event_id)s, %(article_url)s, %(source_name)s, "
                    "%(source_weight)s, %(published_at)s, %(ingested_at)s, %(title)s, %(event_summary)s, "
                    "%(dashboard_blurb)s, %(reason_flagged)s, %(oem_entities)s, %(supplier_entities)s, "
                    "%(component_entities)s, %(component_criticality)s, %(risk_category)s, "
                    "%(disruption_type)s, %(geo_country)s, %(geo_region)s, %(geo_confidence)s, "
                    "%(impact_1to5)s, %(probability_1to5)s, %(time_sensitivity_1to3)s, "
                    "%(exposure_proxy_1to5)s, %(severity_confidence)s, %(risk_score_0to100)s, "
                    "%(severity_band)s, %(estimated_delay_days)s, %(delay_confidence)s, "
                    "%(delay_rationale)s, %(exposure_usd_est)s, %(exposure_confidence)s, "
                    "%(exposure_assumptions)s, %(mitigation_description)s, %(mitigation_actions)s, "
                    "%(mitigation_generated_at)s, %(llm_validation_passed)s, %(rejected_reason)s, "
                    "%(created_at)s) ON CONFLICT (event_id) DO UPDATE SET "
                    "article_url = EXCLUDED.article_url, source_name = EXCLUDED.source_name, "
                    "source_weight = EXCLUDED.source_weight, published_at = EXCLUDED.published_at, "
                    "ingested_at = EXCLUDED.ingested_at, title = EXCLUDED.title, "
                    "event_summary = EXCLUDED.event_summary, dashboard_blurb = EXCLUDED.dashboard_blurb, "
                    "reason_flagged = EXCLUDED.reason_flagged, oem_entities = EXCLUDED.oem_entities, "
                    "supplier_entities = EXCLUDED.supplier_entities, component_entities = EXCLUDED.component_entities, "
                    "component_criticality = EXCLUDED.component_criticality, risk_category = EXCLUDED.risk_category, "
                    "disruption_type = EXCLUDED.disruption_type, geo_country = EXCLUDED.geo_country, "
                    "geo_region = EXCLUDED.geo_region, geo_confidence = EXCLUDED.geo_confidence, "
                    "impact_1to5 = EXCLUDED.impact_1to5, probability_1to5 = EXCLUDED.probability_1to5, "
                    "time_sensitivity_1to3 = EXCLUDED.time_sensitivity_1to3, exposure_proxy_1to5 = EXCLUDED.exposure_proxy_1to5, "
                    "severity_confidence = EXCLUDED.severity_confidence, risk_score_0to100 = EXCLUDED.risk_score_0to100, "
                    "severity_band = EXCLUDED.severity_band, estimated_delay_days = EXCLUDED.estimated_delay_days, "
                    "delay_confidence = EXCLUDED.delay_confidence, delay_rationale = EXCLUDED.delay_rationale, "
                    "exposure_usd_est = EXCLUDED.exposure_usd_est, exposure_confidence = EXCLUDED.exposure_confidence, "
                    "exposure_assumptions = EXCLUDED.exposure_assumptions, "
                    "mitigation_description = COALESCE(EXCLUDED.mitigation_description, enriched_events.mitigation_description), "
                    "mitigation_actions = COALESCE(EXCLUDED.mitigation_actions, enriched_events.mitigation_actions), "
                    "mitigation_generated_at = COALESCE(EXCLUDED.mitigation_generated_at, enriched_events.mitigation_generated_at), "
                    "llm_validation_passed = EXCLUDED.llm_validation_passed, rejected_reason = EXCLUDED.rejected_reason, "
                    "created_at = EXCLUDED.created_at",
                    prepared,
                )
        return len(prepared)

    with get_connection(paths) as conn:
        cur = conn.executemany(
            "INSERT INTO enriched_events (event_id, article_url, source_name, "
            "source_weight, published_at, ingested_at, title, event_summary, dashboard_blurb, "
            "reason_flagged, oem_entities, supplier_entities, component_entities, "
            "component_criticality, risk_category, disruption_type, geo_country, geo_region, "
            "geo_confidence, impact_1to5, probability_1to5, time_sensitivity_1to3, "
            "exposure_proxy_1to5, severity_confidence, risk_score_0to100, severity_band, "
            "estimated_delay_days, delay_confidence, delay_rationale, exposure_usd_est, "
            "exposure_confidence, exposure_assumptions, mitigation_description, "
            "mitigation_actions, mitigation_generated_at, llm_validation_passed, "
            "rejected_reason, created_at) VALUES (:event_id, :article_url, :source_name, "
            ":source_weight, :published_at, :ingested_at, :title, :event_summary, :dashboard_blurb, "
            ":reason_flagged, :oem_entities, :supplier_entities, :component_entities, "
            ":component_criticality, :risk_category, :disruption_type, :geo_country, :geo_region, "
            ":geo_confidence, :impact_1to5, :probability_1to5, :time_sensitivity_1to3, "
            ":exposure_proxy_1to5, :severity_confidence, :risk_score_0to100, :severity_band, "
            ":estimated_delay_days, :delay_confidence, :delay_rationale, :exposure_usd_est, "
            ":exposure_confidence, :exposure_assumptions, :mitigation_description, "
            ":mitigation_actions, :mitigation_generated_at, :llm_validation_passed, "
            ":rejected_reason, :created_at) "
            "ON CONFLICT (event_id) DO UPDATE SET "
            "article_url = excluded.article_url, source_name = excluded.source_name, "
            "source_weight = excluded.source_weight, published_at = excluded.published_at, "
            "ingested_at = excluded.ingested_at, title = excluded.title, "
            "event_summary = excluded.event_summary, dashboard_blurb = excluded.dashboard_blurb, "
            "reason_flagged = excluded.reason_flagged, oem_entities = excluded.oem_entities, "
            "supplier_entities = excluded.supplier_entities, "
            "component_entities = excluded.component_entities, "
            "component_criticality = excluded.component_criticality, "
            "risk_category = excluded.risk_category, disruption_type = excluded.disruption_type, "
            "geo_country = excluded.geo_country, geo_region = excluded.geo_region, "
            "geo_confidence = excluded.geo_confidence, impact_1to5 = excluded.impact_1to5, "
            "probability_1to5 = excluded.probability_1to5, "
            "time_sensitivity_1to3 = excluded.time_sensitivity_1to3, "
            "exposure_proxy_1to5 = excluded.exposure_proxy_1to5, "
            "severity_confidence = excluded.severity_confidence, "
            "risk_score_0to100 = excluded.risk_score_0to100, "
            "severity_band = excluded.severity_band, "
            "estimated_delay_days = excluded.estimated_delay_days, "
            "delay_confidence = excluded.delay_confidence, "
            "delay_rationale = excluded.delay_rationale, "
            "exposure_usd_est = excluded.exposure_usd_est, "
            "exposure_confidence = excluded.exposure_confidence, "
            "exposure_assumptions = excluded.exposure_assumptions, "
            "mitigation_description = COALESCE(excluded.mitigation_description, enriched_events.mitigation_description), "
            "mitigation_actions = COALESCE(excluded.mitigation_actions, enriched_events.mitigation_actions), "
            "mitigation_generated_at = COALESCE(excluded.mitigation_generated_at, enriched_events.mitigation_generated_at), "
            "llm_validation_passed = excluded.llm_validation_passed, "
            "rejected_reason = excluded.rejected_reason, "
            "created_at = excluded.created_at",
            prepared,
        )
        return cur.rowcount


def upsert_llm_rejected_events(paths: DbPaths, rows: Iterable[dict[str, object]]) -> int:
    """Insert or replace LLM-rejected enriched events."""

    prepared = list(rows)
    if not prepared:
        return 0
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO llm_rejected_events (event_id, article_url, source_name, "
                    "source_weight, published_at, ingested_at, title, event_summary, dashboard_blurb, "
                    "reason_flagged, oem_entities, supplier_entities, component_entities, "
                    "component_criticality, risk_category, disruption_type, geo_country, geo_region, "
                    "geo_confidence, impact_1to5, probability_1to5, time_sensitivity_1to3, "
                    "exposure_proxy_1to5, severity_confidence, risk_score_0to100, severity_band, "
                    "estimated_delay_days, delay_confidence, delay_rationale, exposure_usd_est, "
                    "exposure_confidence, exposure_assumptions, mitigation_description, "
                    "mitigation_actions, mitigation_generated_at, llm_validation_passed, "
                    "rejected_reason, created_at) VALUES (%(event_id)s, %(article_url)s, %(source_name)s, "
                    "%(source_weight)s, %(published_at)s, %(ingested_at)s, %(title)s, %(event_summary)s, "
                    "%(dashboard_blurb)s, %(reason_flagged)s, %(oem_entities)s, %(supplier_entities)s, "
                    "%(component_entities)s, %(component_criticality)s, %(risk_category)s, "
                    "%(disruption_type)s, %(geo_country)s, %(geo_region)s, %(geo_confidence)s, "
                    "%(impact_1to5)s, %(probability_1to5)s, %(time_sensitivity_1to3)s, "
                    "%(exposure_proxy_1to5)s, %(severity_confidence)s, %(risk_score_0to100)s, "
                    "%(severity_band)s, %(estimated_delay_days)s, %(delay_confidence)s, "
                    "%(delay_rationale)s, %(exposure_usd_est)s, %(exposure_confidence)s, "
                    "%(exposure_assumptions)s, %(mitigation_description)s, %(mitigation_actions)s, "
                    "%(mitigation_generated_at)s, %(llm_validation_passed)s, %(rejected_reason)s, "
                    "%(created_at)s) ON CONFLICT (event_id) DO UPDATE SET "
                    "rejected_reason = EXCLUDED.rejected_reason, created_at = EXCLUDED.created_at",
                    prepared,
                )
        return len(prepared)

    with get_connection(paths) as conn:
        cur = conn.executemany(
            "INSERT INTO llm_rejected_events (event_id, article_url, source_name, "
            "source_weight, published_at, ingested_at, title, event_summary, dashboard_blurb, "
            "reason_flagged, oem_entities, supplier_entities, component_entities, "
            "component_criticality, risk_category, disruption_type, geo_country, geo_region, "
            "geo_confidence, impact_1to5, probability_1to5, time_sensitivity_1to3, "
            "exposure_proxy_1to5, severity_confidence, risk_score_0to100, severity_band, "
            "estimated_delay_days, delay_confidence, delay_rationale, exposure_usd_est, "
            "exposure_confidence, exposure_assumptions, mitigation_description, "
            "mitigation_actions, mitigation_generated_at, llm_validation_passed, "
            "rejected_reason, created_at) VALUES (:event_id, :article_url, :source_name, "
            ":source_weight, :published_at, :ingested_at, :title, :event_summary, :dashboard_blurb, "
            ":reason_flagged, :oem_entities, :supplier_entities, :component_entities, "
            ":component_criticality, :risk_category, :disruption_type, :geo_country, :geo_region, "
            ":geo_confidence, :impact_1to5, :probability_1to5, :time_sensitivity_1to3, "
            ":exposure_proxy_1to5, :severity_confidence, :risk_score_0to100, :severity_band, "
            ":estimated_delay_days, :delay_confidence, :delay_rationale, :exposure_usd_est, "
            ":exposure_confidence, :exposure_assumptions, :mitigation_description, "
            ":mitigation_actions, :mitigation_generated_at, :llm_validation_passed, "
            ":rejected_reason, :created_at) "
            "ON CONFLICT (event_id) DO UPDATE SET "
            "rejected_reason = excluded.rejected_reason, created_at = excluded.created_at",
            prepared,
        )
        return cur.rowcount


def insert_rejections(paths: DbPaths, rows: Iterable[dict[str, object]]) -> int:
    """Insert or replace rejection reasons."""

    prepared = list(rows)
    if not prepared:
        return 0
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO rejected_articles (article_url, reason, created_at) "
                    "VALUES (%(article_url)s, %(reason)s, %(created_at)s) "
                    "ON CONFLICT (article_url) DO UPDATE SET "
                    "reason = EXCLUDED.reason, created_at = EXCLUDED.created_at",
                    prepared,
                )
        return len(prepared)

    with get_connection(paths) as conn:
        cur = conn.executemany(
            "INSERT OR REPLACE INTO rejected_articles (article_url, reason, created_at) "
            "VALUES (:article_url, :reason, :created_at)",
            prepared,
        )
        return cur.rowcount


def purge_old_raw_articles(paths: DbPaths, retention_days: int) -> int:
    """Delete raw articles older than retention window."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw_articles WHERE ingested_at < %s",
                    (cutoff.isoformat(),),
                )
                return cur.rowcount

    with get_connection(paths) as conn:
        cur = conn.execute(
            "DELETE FROM raw_articles WHERE ingested_at < ?",
            (cutoff.isoformat(),),
        )
        return cur.rowcount


def purge_old_rejected_articles(paths: DbPaths, retention_days: int) -> int:
    """Delete rejected articles older than retention window by created_at."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rejected_articles WHERE created_at < %s",
                    (cutoff.isoformat(),),
                )
                return cur.rowcount

    with get_connection(paths) as conn:
        cur = conn.execute(
            "DELETE FROM rejected_articles WHERE created_at < ?",
            (cutoff.isoformat(),),
        )
        return cur.rowcount


def purge_old_enriched_events(paths: DbPaths, retention_days: int) -> int:
    """Delete enriched events older than retention window by published_at."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM enriched_events WHERE published_at < %s",
                    (cutoff.isoformat(),),
                )
                return cur.rowcount

    with get_connection(paths) as conn:
        cur = conn.execute(
            "DELETE FROM enriched_events WHERE published_at < ?",
            (cutoff.isoformat(),),
        )
        return cur.rowcount


def purge_old_llm_rejected_events(paths: DbPaths, retention_days: int) -> int:
    """Delete LLM-rejected events older than retention window by published_at."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM llm_rejected_events WHERE published_at < %s",
                    (cutoff.isoformat(),),
                )
                return cur.rowcount

    with get_connection(paths) as conn:
        cur = conn.execute(
            "DELETE FROM llm_rejected_events WHERE published_at < ?",
            (cutoff.isoformat(),),
        )
        return cur.rowcount


def fetch_enriched_events(paths: DbPaths, limit: int = 500) -> list[dict[str, object]]:
    """Fetch enriched events rows ordered by risk score."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM enriched_events "
                    "ORDER BY risk_score_0to100 DESC, exposure_usd_est DESC, published_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    with get_connection(paths) as conn:
        rows = conn.execute(
            "SELECT * FROM enriched_events "
            "ORDER BY risk_score_0to100 DESC, exposure_usd_est DESC, published_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_pipeline_counts(paths: DbPaths) -> dict[str, int]:
    """Fetch counts for pipeline stages."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_articles")
                raw_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM enriched_events")
                enriched_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM rejected_articles")
                rejected_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM llm_rejected_events")
                llm_rejected_count = cur.fetchone()[0]
    else:
        with get_connection(paths) as conn:
            raw_count = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]
            enriched_count = conn.execute("SELECT COUNT(*) FROM enriched_events").fetchone()[0]
            rejected_count = conn.execute("SELECT COUNT(*) FROM rejected_articles").fetchone()[0]
            llm_rejected_count = conn.execute("SELECT COUNT(*) FROM llm_rejected_events").fetchone()[0]
    return {
        "raw_articles": int(raw_count),
        "enriched_events": int(enriched_count),
        "llm_rejected_events": int(llm_rejected_count),
        "rejected_articles": int(rejected_count),
    }


def fetch_oldest_enriched_event_date(paths: DbPaths) -> str:
    """Return the oldest enriched event published_at date as ISO string."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT MIN(published_at) AS oldest FROM enriched_events")
                row = cur.fetchone()
    else:
        with get_connection(paths) as conn:
            row = conn.execute(
                "SELECT MIN(published_at) AS oldest FROM enriched_events"
            ).fetchone()
    if not row or not row["oldest"]:
        return ""
    return str(row["oldest"])


def fetch_existing_event_ids(paths: DbPaths) -> set[str]:
    """Return set of event_ids already in enriched_events or llm_rejected_events."""

    sql = "SELECT event_id FROM enriched_events UNION SELECT event_id FROM llm_rejected_events"
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    else:
        with get_connection(paths) as conn:
            rows = conn.execute(sql).fetchall()
    return {str(row[0]) for row in rows}


def fetch_rejection_samples(paths: DbPaths, limit: int = 10) -> list[tuple[str, str]]:
    """Fetch recent rejection samples."""

    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT article_url, reason FROM rejected_articles "
                    "ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return [(row["article_url"], row["reason"]) for row in rows]

    with get_connection(paths) as conn:
        rows = conn.execute(
            "SELECT article_url, reason FROM rejected_articles ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [(row["article_url"], row["reason"]) for row in rows]


def fetch_raw_articles_by_ids(paths: DbPaths, article_ids: list[str]) -> list[dict[str, object]]:
    """Fetch raw articles by id list."""

    if not article_ids:
        return []
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM raw_articles WHERE article_id = ANY(%s)",
                    (article_ids,),
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    with get_connection(paths) as conn:
        placeholders = ", ".join("?" for _ in article_ids)
        rows = conn.execute(
            f"SELECT * FROM raw_articles WHERE article_id IN ({placeholders})",
            tuple(article_ids),
        ).fetchall()
    return [dict(row) for row in rows]
