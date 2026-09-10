"""Public feed routes for published curated playlists (XIN-98).

``GET /f/<slug>`` serves RSS 2.0 + iTunes when the client looks like a
podcatcher (``Accept: application/rss+xml`` or a known podcatcher
User-Agent) and a minimal HTML stub otherwise — true content negotiation on
one URL. ``GET /f/<slug>/feed.xml`` always serves RSS.

Auth model (from the XIN-97 publish state):
  * ``visibility='public'`` — no token needed.
  * ``visibility='unlisted'`` (default) — requires ``?t=<token>`` matching
    the playlist token (constant-time compare). A missing, wrong, or
    rotated token returns HTTP 410 with the friendly "revoked by the
    curator" page — never a bare 404.
  * Unknown slug (or a playlist that was never published and therefore has
    no slug) — HTTP 404.

Caching: strong ETag over the rendered feed bytes plus ``Last-Modified``
from the newest playlist content; ``If-None-Match`` / ``If-Modified-Since``
yield 304. ``Cache-Control: public, max-age=900`` for public feeds keeps
CDN caches at or under the 15-minute polling contract. Unlisted
(token-gated) feeds emit ``Cache-Control: private, max-age=900`` instead:
a CDN that drops the query string from its cache key must never serve a
cached 200 to a missing/invalid-token request (which must be a 410), so
unlisted responses are never stored in shared caches.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import re
from datetime import datetime
from email.utils import format_datetime, parsedate_to_datetime

from fastapi import APIRouter, Depends, Query, Request, Response

from backend.api.rss import build_rss, ensure_aware, rfc2822
from backend.persistence.models import CuratedPlaylist
from backend.persistence.repositories import Store

router = APIRouter()


# Podcatcher / feed-reader User-Agent sniffing for content negotiation.
# Browser UAs never contain these tokens; generic HTTP clients (curl,
# wget, httpx, …) fetching the URL almost certainly want the feed, so they
# are included deliberately.
_PODCATCHER_UA = re.compile(
    r"podcast|overcast|pocket.?casts|castro|podcastaddict|antennapod|"
    r"downcast|itunes|applecoremedia|gpodder|podlove|feedburner|feedly|"
    r"inoreader|newsblur|\brss\b|curl|wget|python-urllib|httpx|"
    r"go-http-client|okhttp|java/",
    re.IGNORECASE,
)

# Only the explicit RSS MIME type counts: real browsers send
# ``Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8``
# and must land on the HTML page, not the feed.
_RSS_ACCEPT = ("application/rss+xml",)


def _wants_rss(request: Request) -> bool:
    accept = request.headers.get("accept", "").lower()
    if any(mime in accept for mime in _RSS_ACCEPT):
        return True
    return bool(_PODCATCHER_UA.search(request.headers.get("user-agent", "")))


async def get_store(request: Request):
    """Per-request Store: ``async with`` commits on clean exit; closed after."""
    store: Store = request.app.state.store_factory()
    try:
        async with store:
            yield store
    finally:
        await store.close()


def _revoked_html() -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Link revoked — TuneIn</title></head><body>"
        "<h1>This link was revoked by the curator</h1>"
        "<p>Ask the curator for a fresh link to this feed.</p>"
        "</body></html>"
    )


def _not_found_html(slug: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Feed not found — TuneIn</title></head><body>"
        f"<h1>Feed not found</h1><p>No published feed at /f/{html.escape(slug)}.</p>"
        "</body></html>"
    )


def _html_stub(*, title: str, feed_url: str) -> str:
    """Minimal landing-page stub. The full page + subscribe UX is XIN-99;
    this only guarantees the title and the RSS autodiscovery link."""
    safe_title = html.escape(title)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{safe_title} — TuneIn</title>"
        f"<link rel='alternate' type='application/rss+xml' title='{safe_title}' href='{html.escape(feed_url)}'>"
        "</head><body>"
        f"<h1>{safe_title}</h1>"
        "<p>A curated podcast feed. The full landing page is coming soon — "
        "subscribe with the RSS link below.</p>"
        f"<p><a href='{html.escape(feed_url)}'>RSS feed</a></p>"
        "</body></html>"
    )


async def _resolve_playlist(
    store: Store, slug: str, token: str | None
) -> CuratedPlaylist | Response:
    """Return the playlist, or an error Response (404 / 410)."""
    playlist = await store.playlists.get_by_slug(slug)
    if playlist is None or not playlist.slug:
        return Response(
            content=_not_found_html(slug),
            status_code=404,
            media_type="text/html; charset=utf-8",
        )
    if playlist.visibility == "public":
        return playlist
    # Unlisted: the token URL is the only auth. Constant-time compare so
    # token validity isn't oracle-able byte-by-byte.
    if (
        not token
        or not playlist.token
        or not hmac.compare_digest(token, playlist.token)
    ):
        return Response(
            content=_revoked_html(),
            status_code=410,
            media_type="text/html; charset=utf-8",
        )
    return playlist


def _rss_response(
    *,
    playlist: CuratedPlaylist,
    entries: list,
    request: Request,
    page_url: str,
    feed_url: str,
) -> Response:
    body = build_rss(playlist, entries, page_url=page_url, feed_url=feed_url)
    etag = '"' + hashlib.sha256(body).hexdigest() + '"'

    instants = [ensure_aware(e.added_at) for e in entries if e.added_at]
    created = ensure_aware(playlist.created_at)
    if created is not None:
        instants.append(created)
    last_modified = max(instants) if instants else datetime.now().astimezone()

    # Unlisted (token-gated) feeds must not be stored by shared caches: a
    # CDN that drops the query string from its cache key could otherwise
    # serve a cached 200 to an invalid-token request (which must be 410).
    cache_scope = "private" if playlist.visibility != "public" else "public"
    headers = {
        "ETag": etag,
        "Last-Modified": format_datetime(last_modified),
        "Cache-Control": f"{cache_scope}, max-age=900",
    }

    # Conditional requests: If-None-Match wins over If-Modified-Since.
    inm = request.headers.get("if-none-match")
    if inm is not None and (inm.strip() == "*" or etag in _parse_if_none_match(inm)):
        return Response(status_code=304, headers=headers)
    ims = request.headers.get("if-modified-since")
    if ims:
        try:
            ims_dt = parsedate_to_datetime(ims)
            if last_modified <= ims_dt:
                return Response(status_code=304, headers=headers)
        except (TypeError, ValueError):
            pass  # malformed date: ignore and serve the feed

    return Response(
        content=body,
        media_type="application/rss+xml; charset=utf-8",
        headers=headers,
    )


def _parse_if_none_match(value: str) -> set[str]:
    """Parse an ``If-None-Match`` header into comparable tags, stripping the
    ``W/`` weak-validator prefix so a weak validator still matches."""
    tags = set()
    for part in value.split(","):
        part = part.strip()
        if part.startswith("W/"):
            part = part[2:].strip()
        if part:
            tags.add(part)
    return tags


def _base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


@router.get("/f/{slug}/feed.xml")
async def feed_xml(
    slug: str,
    request: Request,
    t: str | None = Query(default=None),
    store: Store = Depends(get_store),
):
    """Always serve the RSS feed (explicit alias for podcatchers)."""
    resolved = await _resolve_playlist(store, slug, t)
    if isinstance(resolved, Response):
        return resolved
    base = _base(request)
    entries = await store.playlists.list_entries(resolved.playlist_id)
    return _rss_response(
        playlist=resolved,
        entries=entries,
        request=request,
        page_url=f"{base}/f/{resolved.slug}",
        feed_url=f"{base}/f/{resolved.slug}/feed.xml",
    )


@router.get("/f/{slug}")
async def feed_page(
    slug: str,
    request: Request,
    t: str | None = Query(default=None),
    store: Store = Depends(get_store),
):
    """Content-negotiated feed URL: RSS for podcatchers, HTML stub otherwise."""
    resolved = await _resolve_playlist(store, slug, t)
    if isinstance(resolved, Response):
        return resolved
    base = _base(request)
    page_url = f"{base}/f/{resolved.slug}"
    feed_url = f"{base}/f/{resolved.slug}/feed.xml"
    if _wants_rss(request):
        entries = await store.playlists.list_entries(resolved.playlist_id)
        return _rss_response(
            playlist=resolved,
            entries=entries,
            request=request,
            page_url=page_url,
            feed_url=feed_url,
        )
    return Response(
        content=_html_stub(title=resolved.title, feed_url=feed_url),
        media_type="text/html; charset=utf-8",
    )
