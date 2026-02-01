"""Debug data helpers."""

from __future__ import annotations

from dataclasses import dataclass

from src.storage import DbPaths, fetch_pipeline_counts, fetch_rejection_samples


@dataclass(frozen=True)
class DebugData:
    """Pipeline debug payload."""

    counts: dict[str, int]
    rejections: list[tuple[str, str]]


def get_debug_data(paths: DbPaths) -> DebugData:
    """Collect pipeline debug data."""

    return DebugData(
        counts=fetch_pipeline_counts(paths),
        rejections=fetch_rejection_samples(paths),
    )
