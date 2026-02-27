#!/usr/bin/env python3
"""
Re-classify enriched events using the Groq LLM.

By default only touches events with Unknown geo_country, Unknown geo_region,
or 'Other' disruption_type. Use --all to reprocess every event (useful when
the LLM previously misclassified a country/disruption type that isn't 'Unknown').

Usage (run from project root):
    python scripts/reclassify_unknowns.py           # only Unknown/Other events
    python scripts/reclassify_unknowns.py --all     # reprocess every event
    python scripts/reclassify_unknowns.py --dry-run # preview without writing
    python scripts/reclassify_unknowns.py --limit 20 --dry-run

Options:
    --all        Re-classify every event, not just Unknown/Other ones.
    --dry-run    Print proposed changes without writing to the database.
    --limit N    Process at most N events (default: all).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import get_config
from src.groq_client import classify_event_fields
from src.llm_extract import _classify_pestel  # derive risk_category from disruption_type


def _needs_reclassification(row: sqlite3.Row) -> bool:
    return (
        row["geo_country"] == "Unknown"
        or row["geo_region"] == "Unknown"
        or row["disruption_type"] == "Other"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-classify events via Groq LLM.")
    parser.add_argument("--all", action="store_true", help="Reprocess every event, not just Unknown/Other.")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB.")
    parser.add_argument("--limit", type=int, default=0, help="Max events to process (0 = all).")
    args = parser.parse_args()

    config = get_config()

    if not config.groq_api_key:
        print("ERROR: GROQ_API_KEY not configured.")
        print("  Set it in .streamlit/secrets.toml:  GROQ_API_KEY = \"gsk_...\"")
        sys.exit(1)

    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row

    # Join with raw_articles to get original article text (much richer than the generated event_summary stub)
    base_query = """
        SELECT e.event_id, e.title, e.event_summary, e.disruption_type, e.geo_country, e.geo_region,
               COALESCE(r.summary, '') AS raw_summary, COALESCE(r.content, '') AS raw_content
        FROM enriched_events e
        LEFT JOIN raw_articles r ON e.article_url = r.article_url
    """
    if args.all:
        rows = conn.execute(base_query + "ORDER BY e.risk_score_0to100 DESC").fetchall()
    else:
        rows = conn.execute(
            base_query +
            "WHERE e.geo_country = 'Unknown' OR e.geo_region = 'Unknown' OR e.disruption_type = 'Other' "
            "ORDER BY e.risk_score_0to100 DESC"
        ).fetchall()

    if args.limit > 0:
        rows = rows[: args.limit]

    total = len(rows)
    print(f"Found {total} events to re-classify.")
    if args.dry_run:
        print("DRY RUN — no changes will be written.\n")

    updated = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(rows, 1):
        event_id = row["event_id"]
        title = row["title"] or ""
        # Prefer raw article content over the generated summary stub for better Groq accuracy
        raw = " ".join(filter(None, [row["raw_summary"], row["raw_content"]]))
        summary = raw if len(raw) > len(row["event_summary"] or "") else (row["event_summary"] or "")
        old_disruption = row["disruption_type"]
        old_country = row["geo_country"]
        old_region = row["geo_region"]

        print(f"[{i}/{total}] {title[:70]}")
        print(f"  Before: disruption={old_disruption!r}  country={old_country!r}  region={old_region!r}")

        result = classify_event_fields(
            title=title,
            summary=summary,
            api_key=config.groq_api_key,
            model=config.groq_model,
        )

        if not result:
            print("  -> LLM call failed, skipping.\n")
            failed += 1
            time.sleep(1.0)
            continue

        new_disruption = result["disruption_type"]
        new_country = result["geo_country"]
        new_region = result["geo_region"]

        print(f"  After:  disruption={new_disruption!r}  country={new_country!r}  region={new_region!r}")

        # Decide which fields to update
        updates: dict[str, str] = {}
        if args.all:
            # In --all mode: update any field where LLM returns a more specific value
            if new_disruption != "Other" and new_disruption != old_disruption:
                updates["disruption_type"] = new_disruption
                updates["risk_category"] = _classify_pestel(new_disruption)
            if new_country != "Unknown" and new_country != old_country:
                updates["geo_country"] = new_country
            if new_region != "Unknown" and new_region != old_region:
                updates["geo_region"] = new_region
        else:
            # Default: only fill in Unknown/Other fields
            if old_disruption == "Other" and new_disruption != "Other":
                updates["disruption_type"] = new_disruption
                updates["risk_category"] = _classify_pestel(new_disruption)
            if old_country in ("Unknown", "null", "") and new_country not in ("Unknown", "null", ""):
                updates["geo_country"] = new_country
            if old_region in ("Unknown", "") and new_region not in ("Unknown", ""):
                updates["geo_region"] = new_region

        if not updates:
            print("  -> No improvement found, skipping.\n")
            skipped += 1
        else:
            print(f"  -> Updating: {list(updates.keys())}")
            if not args.dry_run:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [event_id]
                conn.execute(
                    f"UPDATE enriched_events SET {set_clause} WHERE event_id = ?", values
                )
                conn.commit()
            updated += 1
            print()

        # Respect Groq free-tier rate limit (~30 RPM)
        time.sleep(0.5)

    conn.close()

    print("─" * 60)
    print(f"Done.  Updated: {updated}  Skipped (no change): {skipped}  Failed: {failed}  Total: {total}")
    if args.dry_run:
        print("(DRY RUN — nothing was written to the database.)")


if __name__ == "__main__":
    main()
