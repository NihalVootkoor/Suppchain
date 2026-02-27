"""Thin pipeline wrapper for deterministic backfill enrichment."""
from __future__ import annotations

from typing import Any, Dict, Optional

from src.config import get_config
from src.date_utils import parse_datetime
from src.llm_extract import extract_with_llm
from src.models import RawArticle
from src.scoring import build_enriched_event
from src.serialization import event_to_row
from src.storage import DbPaths, init_db, upsert_enriched_events
from src.url_utils import canonicalize_url, hash_id

_CONFIG = get_config()
_PATHS = DbPaths(_CONFIG.db_path, _CONFIG.db_url)
_DB_READY = False


def _ensure_db() -> None:
    global _DB_READY
    if not _DB_READY:
        init_db(_PATHS)
        _DB_READY = True


def _row_to_article(row: Dict[str, Any]) -> RawArticle:
    url = str(row.get("url") or row.get("article_url") or "").strip()
    title = str(row.get("title") or "").strip()
    summary = str(row.get("description") or row.get("summary") or "").strip()
    content = str(row.get("content") or "").strip()
    published_at = parse_datetime(str(row.get("published_at") or ""))
    ingested_at = parse_datetime(str(row.get("ingested_at") or ""))
    source_name = str(row.get("source_name") or "Seed").strip()
    source_weight = float(row.get("source_weight") or 0.6)
    canonical = canonicalize_url(url or title)
    article_id = str(row.get("article_id") or hash_id(canonical))
    return RawArticle(
        article_id=article_id,
        article_url=url or canonical,
        source_name=source_name,
        source_weight=source_weight,
        published_at=published_at,
        ingested_at=ingested_at,
        title=title or "Untitled",
        summary=summary or "",
        content=content or summary or "",
    )


def process_candidate_article(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process a single raw candidate row and write to enriched_events."""
    _ensure_db()
    article = _row_to_article(row)
    extraction = extract_with_llm(article)
    if not extraction.llm_validation_passed:
        return None
    event = build_enriched_event(article, extraction)
    enriched_row = event_to_row(event)
    upsert_enriched_events(_PATHS, [enriched_row])
    return enriched_row
