"""Shared HTTP conditional-request helpers (XIN-98 / XIN-104).

``feeds.py`` must not import ``developer.py`` (the polling contract builds
on the feed helpers, never the other way around), so the ETag parser both
need lives here instead.
"""

from __future__ import annotations


def parse_if_none_match(value: str) -> set[str]:
    """Parse an ``If-None-Match`` header into comparable tags.

    Strips the ``W/`` weak-validator prefix so a weak validator for the
    same opaque tag still matches.
    """
    tags = set()
    for part in value.split(","):
        part = part.strip()
        if part.startswith("W/"):
            part = part[2:].strip()
        if part:
            tags.add(part)
    return tags
