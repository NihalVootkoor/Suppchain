"""One-time cleanup: delete seed/backfill records published before the pipeline go-live date.

Usage:
  # Dry run (preview only, no deletes)
  python scripts/purge_pre_pipeline_data.py

  # Actually delete
  python scripts/purge_pre_pipeline_data.py --confirm
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import get_config
from src.storage import DbPaths, get_connection, _use_postgres

# ── Cutoff date — data published BEFORE this date will be deleted ─────────────
PIPELINE_LIVE_FROM = "2026-01-27T00:00:00+00:00"


def _count_rows(paths: DbPaths, cutoff: str) -> dict[str, int]:
    """Return counts of rows that would be deleted."""
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM enriched_events WHERE published_at < %s",
                    (cutoff,),
                )
                enriched = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM raw_articles WHERE published_at < %s",
                    (cutoff,),
                )
                raw = cur.fetchone()[0]
        return {"enriched_events": int(enriched), "raw_articles": int(raw)}

    import sqlite3
    with get_connection(paths) as conn:
        enriched = conn.execute(
            "SELECT COUNT(*) FROM enriched_events WHERE published_at < ?", (cutoff,)
        ).fetchone()[0]
        raw = conn.execute(
            "SELECT COUNT(*) FROM raw_articles WHERE published_at < ?", (cutoff,)
        ).fetchone()[0]
    return {"enriched_events": int(enriched), "raw_articles": int(raw)}


def _delete_rows(paths: DbPaths, cutoff: str) -> dict[str, int]:
    """Delete rows and return counts of deleted rows."""
    if _use_postgres(paths):
        with get_connection(paths) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM enriched_events WHERE published_at < %s",
                    (cutoff,),
                )
                enriched_deleted = cur.rowcount
                cur.execute(
                    "DELETE FROM raw_articles WHERE published_at < %s",
                    (cutoff,),
                )
                raw_deleted = cur.rowcount
        return {"enriched_events": int(enriched_deleted), "raw_articles": int(raw_deleted)}

    with get_connection(paths) as conn:
        enriched_deleted = conn.execute(
            "DELETE FROM enriched_events WHERE published_at < ?", (cutoff,)
        ).rowcount
        raw_deleted = conn.execute(
            "DELETE FROM raw_articles WHERE published_at < ?", (cutoff,)
        ).rowcount
    return {"enriched_events": int(enriched_deleted), "raw_articles": int(raw_deleted)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge pre-pipeline seed/backfill data")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually delete rows. Without this flag, runs as a dry-run (preview only).",
    )
    parser.add_argument(
        "--cutoff",
        default=PIPELINE_LIVE_FROM,
        help=f"ISO date cutoff (default: {PIPELINE_LIVE_FROM}). Rows with published_at before this are deleted.",
    )
    args = parser.parse_args()

    config = get_config()
    paths = DbPaths(config.db_path, config.db_url)

    backend = "Supabase/Postgres" if _use_postgres(paths) else f"SQLite ({config.db_path})"
    print(f"Backend : {backend}")
    print(f"Cutoff  : {args.cutoff}")
    print()

    counts = _count_rows(paths, args.cutoff)
    print("Rows to be deleted:")
    print(f"  enriched_events : {counts['enriched_events']}")
    print(f"  raw_articles    : {counts['raw_articles']}")
    print()

    if counts["enriched_events"] == 0 and counts["raw_articles"] == 0:
        print("Nothing to delete. Database is already clean.")
        return

    if not args.confirm:
        print("DRY RUN — no changes made.")
        print("Run with --confirm to perform the deletion.")
        return

    print("Deleting...")
    deleted = _delete_rows(paths, args.cutoff)
    print(f"  Deleted from enriched_events : {deleted['enriched_events']}")
    print(f"  Deleted from raw_articles    : {deleted['raw_articles']}")
    print()
    print("Done. Refresh your dashboard to see the cleaned trend chart.")


if __name__ == "__main__":
    main()
