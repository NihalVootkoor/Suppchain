"""Re-score all saved enriched events with the corrected formula, then sync to Supabase.

Usage:
    python scripts/rescore_and_sync.py              # rescore SQLite + sync to Supabase if configured
    python scripts/rescore_and_sync.py --sqlite-only # rescore SQLite only, skip Supabase
    python scripts/rescore_and_sync.py --dry-run    # print what would change, write nothing
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import get_config  # noqa: E402
from src.storage import DbPaths, get_sqlite_connection, upsert_enriched_events  # noqa: E402


def _read_db_url_from_toml(repo_root: Path) -> str | None:
    """Read DB URL from .streamlit/secrets.toml when Streamlit is not running."""
    import re
    secrets_file = repo_root / ".streamlit" / "secrets.toml"
    if not secrets_file.is_file():
        return None
    try:
        text = secrets_file.read_text(encoding="utf-8")
        for key in ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL"):
            m = re.search(rf'{key}\s*=\s*["\']([^"\']+)["\']', text)
            if m:
                return m.group(1).strip()
    except Exception:
        pass
    return None


# --------------------------------------------------------------------------- #
# Scoring logic — must stay in sync with src/scoring.py                       #
# --------------------------------------------------------------------------- #

def _new_score(impact: int, prob: int, time_sens: int, exposure: int) -> float:
    return round(
        (impact / 5) * 40.0
        + (prob / 5) * 30.0
        + (time_sens / 3) * 15.0
        + (exposure / 5) * 15.0,
        2,
    )


def _severity_band(score: float) -> str:
    if score >= 85:
        return "Critical"
    if score >= 70:
        return "High"
    if score >= 45:
        return "Medium"
    return "Low"


# --------------------------------------------------------------------------- #
# SQLite rescore                                                               #
# --------------------------------------------------------------------------- #

def rescore_sqlite(db_path: Path, dry_run: bool = False) -> tuple[int, Counter, Counter]:
    """Re-score all events in SQLite in-place.

    Returns (rows_changed, old_band_counts, new_band_counts).
    """
    conn = get_sqlite_connection(db_path)
    rows = conn.execute(
        "SELECT event_id, impact_1to5, probability_1to5, time_sensitivity_1to3, "
        "exposure_proxy_1to5, risk_score_0to100, severity_band "
        "FROM enriched_events"
    ).fetchall()

    updates: list[tuple[float, str, str]] = []
    old_bands: Counter = Counter()
    new_bands: Counter = Counter()

    for row in rows:
        old_bands[row["severity_band"]] += 1
        new_score = _new_score(
            row["impact_1to5"],
            row["probability_1to5"],
            row["time_sensitivity_1to3"],
            row["exposure_proxy_1to5"],
        )
        new_band = _severity_band(new_score)
        new_bands[new_band] += 1
        updates.append((new_score, new_band, row["event_id"]))

    if not dry_run:
        conn.executemany(
            "UPDATE enriched_events SET risk_score_0to100 = ?, severity_band = ? WHERE event_id = ?",
            updates,
        )
        conn.commit()

    conn.close()
    return len(updates), old_bands, new_bands


# --------------------------------------------------------------------------- #
# Supabase sync                                                                #
# --------------------------------------------------------------------------- #

def sync_to_supabase(db_path: Path, db_url: str, dry_run: bool = False) -> int:
    """Push all SQLite enriched_events to Supabase. Returns event count."""
    conn = get_sqlite_connection(db_path)
    rows = conn.execute("SELECT * FROM enriched_events").fetchall()
    conn.close()

    if not rows:
        return 0

    batch = [dict(r) for r in rows]

    if dry_run:
        return len(batch)

    pg_paths = DbPaths(db_path, db_url)
    return upsert_enriched_events(pg_paths, batch)


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(description="Re-score events and sync to Supabase.")
    parser.add_argument("--sqlite-only", action="store_true", help="Skip Supabase sync.")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing.")
    args = parser.parse_args()

    config = get_config(REPO_ROOT)
    db_path = config.db_path

    if not db_path.exists():
        print(f"ERROR: SQLite DB not found at {db_path}")
        return 1

    dry_tag = " [DRY RUN]" if args.dry_run else ""

    # ── 1. Re-score SQLite ──────────────────────────────────────────────────
    print(f"\nRe-scoring events in {db_path}{dry_tag} ...")
    total, old_bands, new_bands = rescore_sqlite(db_path, dry_run=args.dry_run)
    print(f"  {total} events processed.")

    band_order = ["Critical", "High", "Medium", "Low"]
    print("\n  Severity distribution:")
    print(f"  {'Band':<12} {'Before':>8} {'After':>8}")
    print(f"  {'-'*12} {'-'*8} {'-'*8}")
    for band in band_order:
        before = old_bands.get(band, 0)
        after = new_bands.get(band, 0)
        arrow = " <-" if before != after else ""
        print(f"  {band:<12} {before:>8} {after:>8}{arrow}")

    if args.dry_run:
        print("\n  (no changes written — re-run without --dry-run to apply)")

    # ── 2. Sync to Supabase ─────────────────────────────────────────────────
    if args.sqlite_only:
        print("\nSkipping Supabase sync (--sqlite-only).")
        return 0

    db_url = config.db_url or _read_db_url_from_toml(REPO_ROOT)
    if not db_url:
        print(
            "\nNo Supabase URL found. Set SUPABASE_DB_URL (or DATABASE_URL) as an env var "
            "or in .streamlit/secrets.toml to enable sync."
        )
        return 0

    print(f"\nSyncing to Supabase{dry_tag} ...")
    try:
        synced = sync_to_supabase(db_path, db_url, dry_run=args.dry_run)
        print(f"  Upserted {synced} events to Supabase.")
    except Exception as exc:
        print(f"  ERROR during Supabase sync: {exc}")
        return 1

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
