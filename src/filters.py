"""Deterministic filters for supply-chain relevance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.config import DISRUPTION_TRIGGERS as _CONFIG_TRIGGERS, NEGATIVE_KEYWORDS as _CONFIG_NEGATIVES, OEMS
from src.models import RawArticle


@dataclass(frozen=True)
class FilterResult:
    """Result for a filter check."""

    is_relevant: bool
    reason: Optional[str]


AUTOMOTIVE_ANCHORS: tuple[str, ...] = tuple(
    sorted(
        {
            "automotive",
            "auto maker",
            "automaker",
            "automobile",
            "car",
            "carmaker",
            "vehicle",
            "ev",
            "electric vehicle",
            "oem",
            *[oem.lower() for oem in OEMS],
        }
    )
)
_DISRUPTION_TRIGGERS: tuple[str, ...] = tuple(t.lower() for t in _CONFIG_TRIGGERS)
_NEGATIVE_KEYWORDS: tuple[str, ...] = tuple(k.lower() for k in _CONFIG_NEGATIVES)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    """Return True if any keyword is contained in the text."""

    return any(keyword in text for keyword in keywords)


def hard_filter(article: RawArticle) -> FilterResult:
    """Apply hard filter: automotive anchor + disruption trigger, no negatives."""

    text = f"{article.title} {article.summary} {article.content}".lower()
    if _contains_any(text, _NEGATIVE_KEYWORDS):
        return FilterResult(False, "Negative keyword match.")
    if not _contains_any(text, AUTOMOTIVE_ANCHORS):
        return FilterResult(False, "Missing automotive anchor.")
    if not _contains_any(text, _DISRUPTION_TRIGGERS):
        return FilterResult(False, "Missing disruption trigger.")
    return FilterResult(True, None)


def filter_articles(
    articles: list[RawArticle],
) -> tuple[list[RawArticle], dict[str, str]]:
    """Filter articles and return rejections keyed by URL."""

    kept: list[RawArticle] = []
    rejected: dict[str, str] = {}
    for article in articles:
        result = hard_filter(article)
        if result.is_relevant:
            kept.append(article)
        else:
            rejected[article.article_url] = result.reason or "Rejected."
    return kept, rejected
