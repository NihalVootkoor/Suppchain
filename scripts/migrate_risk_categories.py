#!/usr/bin/env python3
"""
Migrate existing enriched_events to the new supply chain risk taxonomy.

Works against both SQLite (local) and Supabase/Postgres.

What this does (deterministically, no LLM calls):
  1. Renames disruption_type values:
       Port Congestion    → Logistics Disruption
       Export Restriction → Trade Restriction
  2. Recalculates risk_category for every event using the new SC mapping:
       Labor Strike        → Labor & Social
       Plant Shutdown      → Supply Disruption
       Logistics Disruption → Logistics & Transport
       Trade Restriction   → Geopolitical & Trade
       Cyberattack         → Cyber & Technology
       Natural Disaster    → Natural Disaster & Climate
       Supplier Insolvency → Supply Disruption
       Regulatory Change   → Regulatory & Compliance
       Capacity Constraint → Supply Disruption
       Other               → Supply Disruption

Usage (run from project root):
    python scripts/migrate_risk_categories.py            # dry-run preview
    python scripts/migrate_risk_categories.py --confirm  # write to DB
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import get_config
from src.llm_extract import _classify_sc_category
from src.storage import DbPaths, _use_postgres, get_connection

# Disruption types that need renaming
_RENAMES: dict[str, str] = {
    "Port Congestion": "Logistics Disruption",
    "Export Restriction": "Trade Restriction",
}


def _run_migration(paths: DbPaths, confirm: bool) -> None:
    postgres = _use_postgres(paths)
    ph = "%s" if postgres else "?"  # placeholder style differs between backends

    with get_connection(paths) as conn:
        if postgres:
            cur = conn.cursor()
            cur.execute("SELECT event_id, disruption_type, risk_category FROM enriched_events")
            raw_rows = cur.fetchall()
            rows = [{"event_id": r[0], "disruption_type": r[1], "risk_category": r[2]} for r in raw_rows]
        else:
            import sqlite3
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT event_id, disruption_type, risk_category FROM enriched_events"
            ).fetchall()
            rows = [dict(r) for r in rows]

        total = len(rows)
        renamed = 0
        recategorized = 0
        unchanged = 0

        for row in rows:
            event_id = row["event_id"]
            old_disruption = row["disruption_type"]
            old_category = row["risk_category"]

            new_disruption = _RENAMES.get(old_disruption, old_disruption)
            new_category = _classify_sc_category(new_disruption)

            if new_disruption == old_disruption and new_category == old_category:
                unchanged += 1
                continue

            changes = []
            if new_disruption != old_disruption:
                changes.append(f"disruption_type: {old_disruption!r} → {new_disruption!r}")
                renamed += 1
            if new_category != old_category:
                changes.append(f"risk_category: {old_category!r} → {new_category!r}")
                recategorized += 1

            print(f"  {str(event_id)[:20]}… {' | '.join(changes)}")

            if confirm:
                sql = (
                    f"UPDATE enriched_events "
                    f"SET disruption_type = {ph}, risk_category = {ph} "
                    f"WHERE event_id = {ph}"
                )
                if postgres:
                    cur.execute(sql, (new_disruption, new_category, event_id))
                else:
                    conn.execute(sql, (new_disruption, new_category, event_id))

        if confirm:
            if postgres:
                conn.commit()
            else:
                conn.commit()

    print()
    print("─" * 60)
    print(f"Backend       : {'Supabase/Postgres' if postgres else f'SQLite ({paths.db_path})'}")
    print(f"Total events  : {total}")
    print(f"Type renames  : {renamed}   (Port Congestion→Logistics Disruption, Export Restriction→Trade Restriction)")
    print(f"Category updates: {recategorized}   (all events recategorized to new SC taxonomy)")
    print(f"Unchanged     : {unchanged}")
    if not confirm:
        print("\nDRY RUN — nothing written. Run with --confirm to apply.")
    else:
        print("\nMigration complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate enriched_events to new SC risk taxonomy.")
    parser.add_argument("--confirm", action="store_true", help="Write changes to DB. Without this, runs as dry-run.")
    args = parser.parse_args()

    config = get_config()
    paths = DbPaths(config.db_path, config.db_url)

    if not args.confirm:
        print("DRY RUN — no changes will be written. Pass --confirm to apply.\n")

    _run_migration(paths, confirm=args.confirm)


if __name__ == "__main__":
    main()
