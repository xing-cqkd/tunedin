"""Shared HTTP conditional-request helpers (XIN-98 / XIN-104).

``feeds.py`` must not import ``developer.py`` (the polling contract builds
on the feed helpers, never the other way around), so the conditional
logic both need lives here instead.
"""

from __future__ import annotations

from email.utils import format_datetime, parsedate_to_datetime

from fastapi import Request, Response

from backend.api.rss import ensure_aware


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


def check_conditional(
    request: Request, *, etag: str, last_modified
) -> Response | None:
    """Evaluate conditional headers; return a 304 Response when not modified.

    ``If-None-Match`` wins: when it is present but matches nothing,
    ``If-Modified-Since`` is ignored entirely (RFC 9110 13.1.4). A
    malformed ``If-Modified-Since`` is ignored and the body is served
    (returns ``None``). The returned 304 carries the same ETag /
    Last-Modified headers as the 200 would.
    """
    headers = {
        "ETag": etag,
        "Last-Modified": format_datetime(ensure_aware(last_modified)),
    }
    inm = request.headers.get("if-none-match")
    if inm is not None:
        if inm.strip() == "*" or etag in parse_if_none_match(inm):
            return Response(status_code=304, headers=headers)
        # INM present but non-matching: IMS MUST be ignored — serve.
        return None
    ims = request.headers.get("if-modified-since")
    if ims:
        try:
            if ensure_aware(last_modified) <= parsedate_to_datetime(ims):
                return Response(status_code=304, headers=headers)
        except (TypeError, ValueError):
            pass  # malformed date: ignore and serve the body
    return None
