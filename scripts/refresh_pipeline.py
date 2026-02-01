"""External scheduler entrypoint for pipeline refresh."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import get_config  # noqa: E402
from src.rss_ingest import run_pipeline  # noqa: E402
from src.storage import DbPaths, init_db, set_meta_value  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh RSS pipeline.")
    parser.add_argument(
        "--interval-hours",
        type=int,
        default=3,
        help="Interval hours (only used for logging).",
    )
    parser.add_argument(
        "--log-json",
        action="store_true",
        help="Emit stats as a JSON line.",
    )
    args = parser.parse_args()

    config = get_config(REPO_ROOT)
    paths = DbPaths(config.db_path, config.db_url)
    init_db(paths)
    stats = run_pipeline(config)
    set_meta_value(paths, "last_refresh_at", datetime.now(timezone.utc).isoformat())

    if args.log_json:
        print(
            json.dumps(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "interval_hours": args.interval_hours,
                    "stats": stats,
                }
            )
        )
    else:
        print(f"Refresh complete at {datetime.now(timezone.utc).isoformat()} UTC")
        print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
