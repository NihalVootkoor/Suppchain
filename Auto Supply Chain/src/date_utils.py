"""Date parsing helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from dateutil import parser


def parse_datetime(value: Optional[str]) -> datetime:
    """Parse a datetime string into a timezone-aware UTC datetime."""

    if not value:
        return datetime.now(timezone.utc)
    parsed = parser.parse(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
