from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple

from dateutil import parser as dtparser

from src.config import get_config
from src.date_utils import parse_datetime
from src.url_utils import canonicalize_url, hash_id
from src.storage import (
    DbPaths,
    fetch_oldest_enriched_event_date,
    fetch_raw_articles_by_ids,
    init_db,
    purge_old_enriched_events,
    purge_old_raw_articles,
    upsert_raw_articles,
)
from src.filters import hard_filter
from src.models import RawArticle


def _parse_dt(value: str) -> str:
    """Return UTC ISO string for published_at; fallback to empty if missing/unparseable."""
    if not value:
        return ""
    try:
        return dtparser.parse(value).astimezone(timezone.utc).isoformat()
    except Exception:
        return ""


@dataclass
class SeedRow:
    article_url: str
    source_name: str = "Seed"
    published_at: str = ""
    title: str = ""
    summary: str = ""
    content: str = ""


def read_seeds_csv(csv_path: str) -> List[SeedRow]:
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Seeds file not found: {csv_path}")

    seeds: List[SeedRow] = []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Seeds CSV has no header row.")

        for r in reader:
            url = (r.get("article_url") or r.get("url") or "").strip()
            if not url:
                continue
            seeds.append(
                SeedRow(
                    article_url=url,
                    source_name=(r.get("source_name") or "Seed").strip(),
                    published_at=_parse_dt((r.get("published_at") or "").strip()),
                    title=(r.get("title") or "").strip(),
                    summary=(r.get("summary") or r.get("description") or "").strip(),
                    content=(r.get("content") or "").strip(),
                )
            )
    return seeds


def seeds_to_raw_articles(paths: DbPaths, seeds: List[SeedRow]) -> Tuple[int, List[str]]:
    """Insert seeds into raw_articles (dedupe handled by article_id hash)."""
    prepared: List[Dict[str, Any]] = []
    article_ids: List[str] = []
    now = datetime.now(timezone.utc)
    for s in seeds:
        url = canonicalize_url(s.article_url)
        article_id = hash_id(url)
        article_ids.append(article_id)
        published_at = parse_datetime(s.published_at) if s.published_at else now
        prepared.append(
            {
                "article_id": article_id,
                "article_url": url,
                "source_name": s.source_name or "Seed",
                "source_weight": 0.6,
                "published_at": published_at.isoformat(),
                "ingested_at": now.isoformat(),
                "title": s.title or "Seed event",
                "summary": s.summary or "",
                "content": s.content or s.summary or "",
            }
        )

    return upsert_raw_articles(paths, prepared), article_ids


def _fetch_raw_articles_by_ids(paths: DbPaths, article_ids: List[str]) -> List[Dict[str, Any]]:
    return fetch_raw_articles_by_ids(paths, article_ids)


def enrich_from_raw_rows(paths: DbPaths, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Minimal backfill enrichment pass:
    - Pull raw_articles
    - Hard filter to candidates
    - Call your existing enrichment pipeline for candidates

    Replace the placeholder `process_candidate(...)` with your actual pipeline logic.
    """
    try:
        from src.pipeline import process_candidate_article  # type: ignore
    except Exception as e:
        raise ImportError(
            "Missing src/pipeline.py with function process_candidate_article(row).\n"
            "Create it (or change this import) to match your project."
        ) from e

    total_raw = len(rows)

    candidates: List[Dict[str, Any]] = []
    rejected_hard = 0

    for r in rows:
        title = str(r.get("title") or "")
        summary = str(r.get("summary") or "")
        content = str(r.get("content") or "")
        article_url = str(r.get("article_url") or "")
        canonical = canonicalize_url(article_url or title)
        article = RawArticle(
            article_id=str(r.get("article_id") or hash_id(canonical)),
            article_url=article_url or canonical,
            source_name=str(r.get("source_name") or "Seed"),
            source_weight=float(r.get("source_weight") or 0.6),
            published_at=parse_datetime(str(r.get("published_at") or "")),
            ingested_at=parse_datetime(str(r.get("ingested_at") or "")),
            title=title or "Untitled",
            summary=summary,
            content=content or summary,
        )
        result = hard_filter(article)
        if not result.is_relevant:
            rejected_hard += 1
            continue
        candidates.append(
            {
                "article_id": article.article_id,
                "title": article.title,
                "summary": article.summary,
                "content": article.content,
                "article_url": article.article_url,
                "source_name": article.source_name,
                "published_at": article.published_at.isoformat(),
                "ingested_at": article.ingested_at.isoformat(),
                "source_weight": article.source_weight,
                "reason_passed": result.reason or "passed_hard_filter",
            }
        )

    enriched = 0
    rejected_llm = 0

    for c in candidates:
        evt = process_candidate_article(c)
        if evt is None:
            rejected_llm += 1
            continue
        enriched += 1

    return {
        "raw_rows_seen": total_raw,
        "candidates": len(candidates),
        "rejected_by_hard_filter": rejected_hard,
        "enriched_written": enriched,
        "rejected_after_validation": rejected_llm,
    }


def run_seed_backfill(csv_path: str) -> Dict[str, object]:
    config = get_config()
    paths = DbPaths(config.db_path, config.db_url)
    init_db(paths)
    purge_old_raw_articles(paths, config.retention_days)
    purge_old_enriched_events(paths, config.enriched_retention_days)

    seeds = read_seeds_csv(csv_path)
    raw_upserts, article_ids = seeds_to_raw_articles(paths, seeds)
    rows = _fetch_raw_articles_by_ids(paths, article_ids)

    stats = enrich_from_raw_rows(paths, rows)
    stats["seeds_loaded"] = len(seeds)
    stats["raw_upserts"] = raw_upserts
    stats["oldest_event_date"] = fetch_oldest_enriched_event_date(paths)
    return stats


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Seed backfill importer for Automotive Risk Monitor")
    parser.add_argument("csv_path", help="Path to seeds.csv")
    args = parser.parse_args()

    out = run_seed_backfill(args.csv_path)
    print(json.dumps(out, indent=2))
