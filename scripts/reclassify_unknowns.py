#!/usr/bin/env python3
"""
Re-classify enriched events using the Groq LLM.

By default only touches events with Unknown geo_country, Unknown geo_region,
or 'Other' disruption_type. Use --all to reprocess every event (useful when
the LLM previously misclassified a country/disruption type that isn't 'Unknown').

Targets local SQLite by default. Use --supabase to run against Supabase/Postgres.

Usage (run from project root):
    python scripts/reclassify_unknowns.py                      # SQLite, Unknown/Other only
    python scripts/reclassify_unknowns.py --all                # SQLite, all events
    python scripts/reclassify_unknowns.py --supabase --all     # Supabase, all events
    python scripts/reclassify_unknowns.py --dry-run            # preview without writing
    python scripts/reclassify_unknowns.py --limit 20 --dry-run

Options:
    --supabase   Target Supabase/Postgres instead of local SQLite.
    --all        Re-classify every event, not just Unknown/Other ones.
    --dry-run    Print proposed changes without writing to the database.
    --limit N    Process at most N events (default: all).
    --model M    Groq model (default: llama-3.1-8b-instant for bulk; use
                 llama-3.3-70b-versatile for higher accuracy in small batches).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import get_config
from src.groq_client import classify_event_fields
from src.llm_extract import _classify_sc_category

# Columns allowed in the dynamic UPDATE to prevent SQL injection
_ALLOWED_UPDATE_COLS = frozenset({
    "disruption_type", "risk_category", "geo_country", "geo_region",
    "llm_validation_passed", "rejected_reason",
})

_BASE_SELECT = """
    SELECT e.event_id, e.title, e.event_summary, e.disruption_type, e.geo_country, e.geo_region,
           COALESCE(r.summary, '') AS raw_summary, COALESCE(r.content, '') AS raw_content
    FROM enriched_events e
    LEFT JOIN raw_articles r ON e.article_url = r.article_url
"""


def _fetch_rows(use_pg: bool, conn, fetch_all: bool) -> list[dict]:
    """Fetch events to reclassify from whichever backend is active."""
    if use_pg:
        from psycopg2.extras import RealDictCursor
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if fetch_all:
                cur.execute(_BASE_SELECT + "ORDER BY e.risk_score_0to100 DESC")
            else:
                cur.execute(
                    _BASE_SELECT +
                    "WHERE e.geo_country = %s OR e.geo_region = %s OR e.disruption_type = %s "
                    "ORDER BY e.risk_score_0to100 DESC",
                    ("Unknown", "Unknown", "Other"),
                )
            return [dict(r) for r in cur.fetchall()]

    # SQLite
    if fetch_all:
        rows = conn.execute(_BASE_SELECT + "ORDER BY e.risk_score_0to100 DESC").fetchall()
    else:
        rows = conn.execute(
            _BASE_SELECT +
            "WHERE e.geo_country = 'Unknown' OR e.geo_region = 'Unknown' OR e.disruption_type = 'Other' "
            "ORDER BY e.risk_score_0to100 DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def _write_rejection(use_pg: bool, conn, event_id: str) -> None:
    if use_pg:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE enriched_events SET llm_validation_passed = 0, rejected_reason = %s WHERE event_id = %s",
                ("Not an automotive supply chain risk", event_id),
            )
        conn.commit()
    else:
        conn.execute(
            "UPDATE enriched_events SET llm_validation_passed = 0, rejected_reason = ? WHERE event_id = ?",
            ("Not an automotive supply chain risk", event_id),
        )
        conn.commit()


def _write_updates(use_pg: bool, conn, updates: dict, event_id: str) -> None:
    safe_keys = [k for k in updates if k in _ALLOWED_UPDATE_COLS]
    if not safe_keys:
        return
    if use_pg:
        set_clause = ", ".join(f"{k} = %s" for k in safe_keys)
        values = [updates[k] for k in safe_keys] + [event_id]
        with conn.cursor() as cur:
            cur.execute(f"UPDATE enriched_events SET {set_clause} WHERE event_id = %s", values)
        conn.commit()
    else:
        set_clause = ", ".join(f"{k} = ?" for k in safe_keys)
        values = [updates[k] for k in safe_keys] + [event_id]
        conn.execute(f"UPDATE enriched_events SET {set_clause} WHERE event_id = ?", values)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-classify events via Groq LLM.")
    parser.add_argument("--supabase", action="store_true", help="Target Supabase/Postgres instead of local SQLite.")
    parser.add_argument("--all", action="store_true", help="Reprocess every event, not just Unknown/Other.")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB.")
    parser.add_argument("--limit", type=int, default=0, help="Max events to process (0 = all).")
    parser.add_argument(
        "--model",
        default="llama-3.1-8b-instant",
        help="Groq model (default: llama-3.1-8b-instant). Use llama-3.3-70b-versatile for higher accuracy.",
    )
    args = parser.parse_args()

    config = get_config()

    if not config.groq_api_key:
        print("ERROR: GROQ_API_KEY not configured.")
        print("  Set it in .streamlit/secrets.toml:  GROQ_API_KEY = \"gsk_...\"")
        sys.exit(1)

    use_pg = args.supabase
    if use_pg:
        if not config.db_url:
            print("ERROR: --supabase requires SUPABASE_DB_URL (or DATABASE_URL) to be configured.")
            sys.exit(1)
        try:
            import psycopg2
        except ImportError:
            print("ERROR: psycopg2 is required for Supabase support. Install it with: pip install psycopg2-binary")
            sys.exit(1)
        conn = psycopg2.connect(config.db_url)
        backend_label = "Supabase/Postgres"
    else:
        conn = sqlite3.connect(config.db_path)
        conn.row_factory = sqlite3.Row
        backend_label = f"SQLite ({config.db_path.name})"

    _is_large_model = "70b" in args.model or "405b" in args.model
    sleep_secs = 10.0 if _is_large_model else 0.5

    try:
        rows = _fetch_rows(use_pg, conn, args.all)
        if args.limit > 0:
            rows = rows[: args.limit]

        total = len(rows)
        print(f"Backend: {backend_label}")
        print(f"Model:   {args.model}  (sleep={sleep_secs}s between calls)")
        print(f"Found {total} events to re-classify.")
        if args.dry_run:
            print("DRY RUN — no changes will be written.\n")

        updated = 0
        skipped = 0
        failed = 0

        for i, row in enumerate(rows, 1):
            event_id = row["event_id"]
            title = row["title"] or ""
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
                model=args.model,
            )

            if not result:
                print("  -> LLM call failed, skipping.\n")
                failed += 1
                time.sleep(1.0)
                continue

            new_disruption = result["disruption_type"]
            new_country = result["geo_country"]
            new_region = result["geo_region"]
            is_risk = result.get("is_automotive_sc_risk", True)

            print(f"  After:  disruption={new_disruption!r}  country={new_country!r}  region={new_region!r}  is_risk={is_risk}")

            # Relevance gate: mark non-automotive events as rejected.
            if not is_risk:
                print("  -> Marking as rejected (not an automotive supply chain risk).")
                if not args.dry_run:
                    _write_rejection(use_pg, conn, event_id)
                updated += 1
                print()
                time.sleep(sleep_secs)
                continue

            # Decide which fields to update.
            updates: dict[str, str] = {}
            if args.all:
                if new_disruption != "Other" and new_disruption != old_disruption:
                    updates["disruption_type"] = new_disruption
                    updates["risk_category"] = _classify_sc_category(new_disruption)
                if new_country != "Unknown" and new_country != old_country:
                    updates["geo_country"] = new_country
                if new_region != "Unknown" and new_region != old_region:
                    updates["geo_region"] = new_region
            else:
                if old_disruption == "Other" and new_disruption != "Other":
                    updates["disruption_type"] = new_disruption
                    updates["risk_category"] = _classify_sc_category(new_disruption)
                if old_country in ("Unknown", "null", "") and new_country not in ("Unknown", "null", ""):
                    updates["geo_country"] = new_country
                if old_region in ("Unknown", "null", "") and new_region not in ("Unknown", "null", ""):
                    updates["geo_region"] = new_region

            if not updates:
                print("  -> No improvement found, skipping.\n")
                skipped += 1
            else:
                print(f"  -> Updating: {list(updates.keys())}")
                if not args.dry_run:
                    _write_updates(use_pg, conn, updates, event_id)
                updated += 1
                print()

            time.sleep(sleep_secs)

    finally:
        conn.close()

    print("─" * 60)
    print(f"Done.  Updated: {updated}  Skipped (no change): {skipped}  Failed: {failed}  Total: {total}")
    if args.dry_run:
        print("(DRY RUN — nothing was written to the database.)")


if __name__ == "__main__":
    main()
