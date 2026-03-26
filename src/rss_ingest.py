"""RSS ingestion and pipeline orchestration."""

from __future__ import annotations

import csv
import gzip
import logging
import re
import zlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.request import Request, urlopen

from src.config import AppConfig
from src.date_utils import parse_datetime
from src.filters import filter_articles
from src.llm_extract import build_event_id, extract_with_llm
from src.mitigation import generate_mitigation
from src.models import EnrichedEvent, RawArticle
from src.scoring import build_enriched_event
from src.serialization import event_to_row, raw_to_row
from src.storage import (
    DbPaths,
    init_db,
    insert_rejections,
    purge_old_enriched_events,
    purge_old_llm_rejected_events,
    purge_old_raw_articles,
    purge_old_rejected_articles,
    fetch_existing_event_ids,
    upsert_enriched_events,
    upsert_llm_rejected_events,
    upsert_raw_articles,
)
from src.url_utils import canonicalize_url, hash_id

logger = logging.getLogger(__name__)


def fetch_rss(url: str) -> str:
    """Fetch RSS XML from a URL."""

    headers = {
        "User-Agent": "AutoSupplyChainMonitor/1.0 (+https://example.com)",
        "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
    }
    _MAX_FEED_BYTES = 10 * 1024 * 1024  # 10 MB hard limit
    request = Request(url, headers=headers)
    with urlopen(request, timeout=20) as response:
        payload = response.read(_MAX_FEED_BYTES + 1)
        if len(payload) > _MAX_FEED_BYTES:
            raise ValueError(f"RSS feed too large (>{_MAX_FEED_BYTES // 1024 // 1024} MB): {url}")
        encoding = (response.headers.get("Content-Encoding") or "").lower()
        if "gzip" in encoding:
            payload = gzip.decompress(payload)
        elif "deflate" in encoding:
            payload = zlib.decompress(payload)
        charset = response.headers.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace")


def _find_text(item: ET.Element, tags: Iterable[str]) -> str:
    for tag in tags:
        value = item.findtext(tag)
        if value:
            return value.strip()
    return ""


def _sanitize_xml(xml_text: str) -> str:
    """Best-effort sanitization for ill-formed feeds."""

    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", xml_text)
    return re.sub(r"&(?!(amp|lt|gt|quot|apos);)", "&amp;", cleaned)


def _extract_atom_link(entry: ET.Element) -> str:
    for link in entry.findall("{http://www.w3.org/2005/Atom}link"):
        rel = link.attrib.get("rel")
        href = link.attrib.get("href")
        if href and (rel in (None, "", "alternate")):
            return href.strip()
    return ""


def _clean_content(text: str) -> str:
    """Strip content that is just an HTML redirect link (e.g. Google News feeds)."""
    stripped = text.strip()
    # Google News and similar aggregators emit '<a href="...">' with no real text
    if re.match(r"^<a\s+href=", stripped, re.IGNORECASE):
        return ""
    return stripped


def parse_rss(xml_text: str, source: str, weight: float) -> list[RawArticle]:
    """Parse RSS or Atom XML into RawArticle entries."""

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        root = ET.fromstring(_sanitize_xml(xml_text))
    items = root.findall(".//item")
    now = datetime.now(timezone.utc)
    articles: list[RawArticle] = []

    for item in items:
        title = _find_text(item, ["title"]) or "Untitled"
        link = _find_text(item, ["link", "guid"])
        summary = _clean_content(_find_text(
            item,
            [
                "description",
                "{http://purl.org/rss/1.0/modules/content/}encoded",
            ],
        ))
        published_at = parse_datetime(item.findtext("pubDate"))
        canonical = canonicalize_url(link or title)
        articles.append(
            RawArticle(
                article_id=hash_id(canonical),
                article_url=link or canonical,
                source_name=source,
                source_weight=weight,
                published_at=published_at,
                ingested_at=now,
                title=title,
                summary=summary,
                content=summary,
            )
        )

    if articles:
        return articles

    entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    for entry in entries:
        title = _find_text(entry, ["{http://www.w3.org/2005/Atom}title"]) or "Untitled"
        link = _extract_atom_link(entry)
        summary = _clean_content(_find_text(
            entry,
            [
                "{http://www.w3.org/2005/Atom}summary",
                "{http://www.w3.org/2005/Atom}content",
            ],
        ))
        published_at = parse_datetime(
            _find_text(
                entry,
                [
                    "{http://www.w3.org/2005/Atom}published",
                    "{http://www.w3.org/2005/Atom}updated",
                ],
            )
        )
        canonical = canonicalize_url(link or title)
        articles.append(
            RawArticle(
                article_id=hash_id(canonical),
                article_url=link or canonical,
                source_name=source,
                source_weight=weight,
                published_at=published_at,
                ingested_at=now,
                title=title,
                summary=summary,
                content=summary,
            )
        )

    return articles


def ingest_rss(
    urls: Iterable[str],
    weights: dict[str, float],
    progress_cb: Callable[[str], None] | None = None,
) -> list[RawArticle]:
    """Fetch and parse RSS feeds."""

    url_list = list(urls)
    articles: list[RawArticle] = []
    for i, url in enumerate(url_list, 1):
        domain = url.split("/")[2] if "//" in url else url
        if progress_cb:
            progress_cb(f"Fetching feed {i}/{len(url_list)}: {domain}...")
        try:
            articles.extend(parse_rss(fetch_rss(url), source=url, weight=weights.get(url, 0.5)))
        except Exception as exc:
            logger.warning("Failed to ingest %s: %s", url, exc)
    return articles


def _load_seed_articles(seed_path: Path) -> list[RawArticle]:
    """Load seed articles from seeds.csv if present."""

    if not seed_path.exists():
        return []
    now = datetime.now(timezone.utc)
    articles: list[RawArticle] = []
    with seed_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            title = (row.get("title") or "Seed event").strip()
            link = (row.get("url") or "").strip()
            summary = (row.get("summary") or "").strip()
            content = (row.get("content") or summary).strip()
            published_at = parse_datetime(row.get("published_at"))
            source = (row.get("source_name") or "seed").strip()
            weight = float(row.get("source_weight") or 0.6)
            canonical = canonicalize_url(link or title)
            articles.append(
                RawArticle(
                    article_id=hash_id(canonical),
                    article_url=link or canonical,
                    source_name=source,
                    source_weight=weight,
                    published_at=published_at,
                    ingested_at=now,
                    title=title,
                    summary=summary,
                    content=content,
                )
            )
    return articles


def _dedupe_articles(articles: list[RawArticle]) -> list[RawArticle]:
    """Deduplicate articles by article_id."""

    deduped: dict[str, RawArticle] = {}
    for article in articles:
        existing = deduped.get(article.article_id)
        if not existing or article.published_at > existing.published_at:
            deduped[article.article_id] = article
    return list(deduped.values())


def run_pipeline(
    config: AppConfig,
    progress_cb: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Run ingestion pipeline and store enriched events."""

    def _progress(msg: str) -> None:
        logger.info(msg)
        if progress_cb:
            progress_cb(msg)

    paths = DbPaths(config.db_path, config.db_url)
    init_db(paths)

    articles = ingest_rss(config.rss_urls, config.source_weights, progress_cb=_progress)
    seed_path = config.project_root / "data" / "seeds.csv"
    articles.extend(_load_seed_articles(seed_path))
    articles = _dedupe_articles(articles)
    upsert_raw_articles(paths, [raw_to_row(article) for article in articles])

    _progress(f"Filtering {len(articles)} articles...")
    kept, rejected = filter_articles(articles)
    rejection_rows = [
        {"article_url": url, "reason": reason, "created_at": datetime.now(timezone.utc).isoformat()}
        for url, reason in rejected.items()
    ]
    insert_rejections(paths, rejection_rows)

    existing_event_ids = fetch_existing_event_ids(paths)
    new_articles = [
        a for a in kept
        if build_event_id(a.article_url, a.published_at) not in existing_event_ids
    ]
    skipped = len(kept) - len(new_articles)
    skip_note = f", {skipped} already processed" if skipped else ""
    _progress(f"Running AI extraction on {len(new_articles)} new articles{skip_note}...")

    enriched_events: list[EnrichedEvent] = []
    llm_rejections: list[dict[str, object]] = []
    llm_rejected_events: list[EnrichedEvent] = []
    for i, article in enumerate(new_articles, 1):
        _progress(f"Extracting article {i}/{len(new_articles)}: {article.title[:60]}...")
        extraction = extract_with_llm(article)
        if not extraction.llm_validation_passed:
            llm_rejections.append(
                {
                    "article_url": article.article_url,
                    "reason": extraction.rejected_reason or "LLM rejected.",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            llm_rejected_events.append(build_enriched_event(article, extraction))
            continue
        enriched_events.append(build_enriched_event(article, extraction))

    insert_rejections(paths, llm_rejections)
    upsert_llm_rejected_events(paths, [event_to_row(event) for event in llm_rejected_events])
    enriched_events.sort(
        key=lambda event: (event.risk_score_0to100, event.exposure_usd_est, event.published_at),
        reverse=True,
    )

    if enriched_events:
        _progress(f"Generating mitigations for top {min(3, len(enriched_events))} events...")
        for event in enriched_events[:3]:
            generate_mitigation(event)

    _progress("Saving to database...")
    upsert_enriched_events(paths, [event_to_row(event) for event in enriched_events])

    _progress("Cleaning up old data...")
    purge_old_raw_articles(paths, config.retention_days)
    purge_old_enriched_events(paths, config.enriched_retention_days)
    purge_old_llm_rejected_events(paths, config.enriched_retention_days)
    purge_old_rejected_articles(paths, config.retention_days)

    return {
        "ingested": len(articles),
        "kept": len(kept),
        "new": len(new_articles),
        "skipped": skipped,
        "enriched": len(enriched_events),
        "rejected": len(rejected) + len(llm_rejections),
    }
