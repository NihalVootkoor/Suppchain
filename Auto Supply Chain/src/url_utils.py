"""URL normalization helpers."""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse, urlunparse


def canonicalize_url(url: str) -> str:
    """Normalize URLs by removing query/fragment and lowercasing scheme/host."""

    parsed = urlparse(url.strip())
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        query="",
        fragment="",
    )
    return urlunparse(normalized)


def hash_id(value: str) -> str:
    """Generate a stable short hash for IDs."""

    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest[:16]
