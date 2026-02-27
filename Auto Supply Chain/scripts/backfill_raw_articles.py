#!/usr/bin/env python3
"""
Re-process existing raw_articles that were never enriched (or were rejected).

Useful after broadening AUTOMOTIVE_ANCHORS / DISRUPTION_TRIGGERS, or after
adding new seeds, to backfill enriched_events without re-fetching RSS feeds.

Usage:
    python scripts/backfill_raw_articles.py              # process all unenriched
    python scripts/backfill_raw_articles.py --dry-run    # count only, no writes
    python scripts/backfill_raw_articles.py --limit 50   # cap at N articles
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import get_config
from src.filters import filter_articles
from src.llm_extract import extract_with_llm
from src.mitigation import generate_mitigation
from src.models import RawArticle
from src.scoring import build_enriched_event
from src.serialization import event_to_row
from src.storage import DbPaths, get_sqlite_connection, insert_rejections, upsert_enriched_events
from src.date_utils import parse_datetime


def _load_unenriched(db_path: Path, limit: int = 0) -> list[RawArticle]:
    conn = get_sqlite_connection(db_path)
    query = """
        SELECT r.article_id, r.article_url, r.source_name, r.source_weight,
               r.published_at, r.ingested_at, r.title, r.summary, r.content
        FROM raw_articles r
        WHERE LENGTH(r.content) > 80
          AND r.content NOT LIKE '<a href=%'
          AND NOT EXISTS (
              SELECT 1 FROM enriched_events e WHERE e.article_url = r.article_url
          )
        ORDER BY r.published_at DESC
    """
    if limit > 0:
        query += f" LIMIT {limit}"
    rows = conn.execute(query).fetchall()
    conn.close()

    now = datetime.now(timezone.utc)
    articles = []
    for row in rows:
        articles.append(RawArticle(
            article_id=row["article_id"],
            article_url=row["article_url"],
            source_name=row["source_name"] or "backfill",
            source_weight=float(row["source_weight"] or 0.5),
            published_at=parse_datetime(row["published_at"]) or now,
            ingested_at=parse_datetime(row["ingested_at"]) or now,
            title=row["title"] or "",
            summary=row["summary"] or "",
            content=row["content"] or "",
        ))
    return articles


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill enriched events from existing raw_articles.")
    parser.add_argument("--dry-run", action="store_true", help="Count articles without writing.")
    parser.add_argument("--limit", type=int, default=0, help="Max articles to process (0 = all).")
    args = parser.parse_args()

    config = get_config(REPO_ROOT)
    paths = DbPaths(config.db_path, config.db_url)

    print(f"Loading unenriched raw_articles from {config.db_path} ...")
    articles = _load_unenriched(config.db_path, limit=args.limit)
    print(f"  Found {len(articles)} unenriched articles with content.")

    kept, rejected = filter_articles(articles)
    print(f"  Filter: {len(kept)} kept, {len(rejected)} rejected.")

    if args.dry_run:
        print("DRY RUN — no writes. Articles that would be enriched:")
        for a in kept:
            print(f"  {a.title[:80]}")
        return 0

    # Store new rejection records for articles that still fail
    rejection_rows = [
        {"article_url": url, "reason": reason, "created_at": datetime.now(timezone.utc).isoformat()}
        for url, reason in rejected.items()
    ]
    insert_rejections(paths, rejection_rows)

    enriched_events = []
    llm_rejections = []
    for i, article in enumerate(kept, 1):
        print(f"  [{i}/{len(kept)}] {article.title[:70]}")
        extraction = extract_with_llm(article)
        if not extraction.llm_validation_passed:
            reason = extraction.rejected_reason or "LLM rejected."
            print(f"    -> LLM rejected: {reason}")
            llm_rejections.append({
                "article_url": article.article_url,
                "reason": reason,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            continue
        event = build_enriched_event(article, extraction)
        print(f"    -> {event.disruption_type} / {event.geo_country} / score={event.risk_score_0to100:.0f}")
        enriched_events.append(event)

    insert_rejections(paths, llm_rejections)

    if enriched_events:
        # Generate mitigation for top 3 new events by risk score
        top3 = sorted(enriched_events, key=lambda e: e.risk_score_0to100, reverse=True)[:3]
        for event in top3:
            generate_mitigation(event)
        upsert_enriched_events(paths, [event_to_row(e) for e in enriched_events])

    print()
    print(f"Done.  Enriched: {len(enriched_events)}  LLM-rejected: {len(llm_rejections)}  Filter-rejected: {len(rejected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
