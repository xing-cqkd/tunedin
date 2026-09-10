"""Developer polling contract for curated feeds (XIN-104, v1).

This module is the whole v1 polling contract — there are deliberately no
webhooks (see the spec's "Polling contract, no webhooks (v1)" decision):

  * ``GET /api/v1/playlists/{id}/feed`` — feed metadata: playlist id,
    title, slug, visibility, the subscribable RSS URL and landing-page URL
    (tokenized with ``?t=`` for unlisted feeds), episode count, and
    ``last_modified``. The playlist id is a random 128-bit UUID, so the id
    itself is the capability: no token is required to read metadata, but
    the *content* endpoints (``/f/<slug>``) still enforce the token for
    unlisted feeds.
  * ``POST /api/v1/playlists/{id}/feed/rotate-token`` — rotate the unlisted
    token (reuses the XIN-97 ``rotate_token`` repository method); returns
    the new subscribable URLs. Rotation is meaningful for unlisted feeds;
    it is accepted (harmless) for public feeds.
  * ``GET /api/v1/playlists/{id}/feed/changes?since=<ISO8601>`` — changelog
    since the given instant: ``added`` (episodes with ``added_at`` after
    ``since``), ``removed``, ``reordered``, the full ordered ``items``
    snapshot, and ``last_modified`` (use as the next ``since``).

Caching contract (all three endpoints): strong ETag over the response body
plus ``Last-Modified``; ``If-None-Match`` / ``If-Modified-Since`` yield 304
on the GETs. ``Cache-Control: public, max-age=900`` documents the 15-minute
TTL — clients should poll no more often than that. (The POST sets the same
headers on its response but is never answered 304: it always mutates.)

Changelog design choice (documented honestly, per the issue):
``removed`` and ``reordered`` are *reserved* and always ``[]`` in v1. The
repository protocol has no episode-removal operation, so removals are
impossible today; and ``PlaylistEpisode.position`` changes carry no
timestamp, so the server cannot know *when* a reorder happened. Clients
detect reorders by diffing the ``items`` snapshot (full ordered list of
episode id / position / added_at) between polls — that is the supported v1
mechanism, and the issue's acceptance ("detect an add/remove/reorder within
the documented TTL") is met through it.

Rate limiting: generous per-IP sliding window (default 600 requests per 15
minutes — far above any sane polling cadence), 429 with ``Retry-After``
when exceeded. The limiter is in-memory and therefore single-process; a
Redis-backed limiter is the follow-up when this API runs on more than one
process. There is deliberately no podcatcher User-Agent allowlist.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import format_datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from backend.api.feeds import get_store, playlist_last_modified
from backend.api.rss import ensure_aware
from backend.persistence.models import CuratedPlaylist
from backend.persistence.repositories import Store


class RateLimiter:
    """In-memory per-IP sliding-window rate limiter.

    Single-process only: each worker keeps its own counters, so limits are
    per-process under multi-worker serving. A Redis-backed limiter replaces
    this when the API scales past one process.
    """

    def __init__(self, *, limit: int = 600, window_seconds: int = 900):
        self.limit = limit
        self.window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = {}

    def check(self, ip: str) -> float | None:
        """Record a hit; return seconds until retry when over the limit."""
        now = time.monotonic()
        cutoff = now - self.window_seconds
        hits = self._hits.setdefault(ip, deque())
        while hits and hits[0] <= cutoff:
            hits.popleft()
        if len(hits) >= self.limit:
            return max(0.0, hits[0] + self.window_seconds - now)
        hits.append(now)
        return None


async def rate_limited(request: Request):
    """FastAPI dependency: 429 + Retry-After when the IP is over the limit."""
    limiter: RateLimiter = request.app.state.rate_limiter
    client = request.client
    ip = client.host if client else "unknown"
    retry_after = limiter.check(ip)
    if retry_after is not None:
        seconds = int(retry_after) + 1
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limit_exceeded", "retry_after": seconds},
            headers={"Retry-After": str(seconds)},
        )


router = APIRouter(prefix="/api/v1", dependencies=[Depends(rate_limited)])


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="Feed not found")


async def _published_playlist(store: Store, playlist_id: str) -> CuratedPlaylist:
    """Resolve a playlist id to a published playlist, or raise 404.

    "Published" means the playlist has a slug (XIN-97 assigns one on
    publish). Unknown ids and never-published playlists both 404 so the
    endpoint doesn't distinguish them.
    """
    try:
        pid = UUID(playlist_id)
    except (ValueError, AttributeError, TypeError):
        raise _not_found()
    playlist = await store.playlists.get_by_id(pid)
    if playlist is None or not playlist.slug:
        raise _not_found()
    return playlist


def _base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _feed_url(base: str, playlist: CuratedPlaylist) -> str:
    url = f"{base}/f/{playlist.slug}/feed.xml"
    if playlist.visibility == "unlisted" and playlist.token:
        url += f"?t={playlist.token}"
    return url


def _landing_url(base: str, playlist: CuratedPlaylist) -> str:
    url = f"{base}/f/{playlist.slug}"
    if playlist.visibility == "unlisted" and playlist.token:
        url += f"?t={playlist.token}"
    return url


def _json_response(
    *, request: Request, payload: dict, last_modified: datetime
) -> Response:
    """JSON body with ETag / Last-Modified / 15-min Cache-Control.

    ``If-None-Match`` wins over ``If-Modified-Since``; a malformed
    ``If-Modified-Since`` is ignored and the body is served.
    """
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    etag = '"' + hashlib.sha256(body).hexdigest() + '"'
    headers = {
        "ETag": etag,
        "Last-Modified": format_datetime(ensure_aware(last_modified)),
        "Cache-Control": "public, max-age=900",
    }
    inm = request.headers.get("if-none-match")
    if inm is not None and (inm.strip() == "*" or etag in _etag_set(inm)):
        return Response(status_code=304, headers=headers)
    ims = request.headers.get("if-modified-since")
    if ims:
        try:
            from email.utils import parsedate_to_datetime

            if ensure_aware(last_modified) <= parsedate_to_datetime(ims):
                return Response(status_code=304, headers=headers)
        except (TypeError, ValueError):
            pass
    return Response(
        content=body, media_type="application/json", headers=headers
    )


def _etag_set(value: str) -> set[str]:
    return {part.strip() for part in value.split(",") if part.strip()}


def _parse_since(value: str) -> datetime:
    """Parse an ISO8601 instant; naive values are assumed UTC."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(
            status_code=400, detail=f"Invalid ISO8601 'since': {value!r}"
        )
    return ensure_aware(dt)


@router.get("/playlists/{playlist_id}/feed")
async def feed_metadata(
    playlist_id: str,
    request: Request,
    store: Store = Depends(get_store),
):
    """Feed metadata for polling clients: URLs + last-modified."""
    playlist = await _published_playlist(store, playlist_id)
    base = _base(request)
    entries = await store.playlists.list_entries(playlist.playlist_id)
    last_modified = playlist_last_modified(playlist, entries)
    # A token rotation changes the subscribable URL the client got from
    # this endpoint, so it advances the metadata's last-modified too.
    revoked_at = ensure_aware(playlist.token_revoked_at)
    if revoked_at is not None and revoked_at > last_modified:
        last_modified = revoked_at
    payload = {
        "playlist_id": str(playlist.playlist_id),
        "title": playlist.title,
        "slug": playlist.slug,
        "visibility": playlist.visibility,
        "feed_url": _feed_url(base, playlist),
        "landing_url": _landing_url(base, playlist),
        "episode_count": len(entries),
        "last_modified": last_modified.isoformat(),
    }
    return _json_response(
        request=request, payload=payload, last_modified=last_modified
    )


@router.post("/playlists/{playlist_id}/feed/rotate-token")
async def rotate_feed_token(
    playlist_id: str,
    request: Request,
    store: Store = Depends(get_store),
):
    """Rotate the unlisted token; returns the new subscribable URLs.

    Always mutates, so the response is never 304 — but it still carries
    ETag / Last-Modified / Cache-Control like the rest of the contract.
    """
    playlist = await _published_playlist(store, playlist_id)
    new_token = await store.playlists.rotate_token(playlist.playlist_id)
    if new_token is None:  # cannot happen: playlist exists; defensive
        raise _not_found()
    playlist = await store.playlists.get_by_id(playlist.playlist_id)
    base = _base(request)
    last_modified = ensure_aware(playlist.token_revoked_at) or datetime.now(
        timezone.utc
    )
    payload = {
        "playlist_id": str(playlist.playlist_id),
        "feed_url": _feed_url(base, playlist),
        "landing_url": _landing_url(base, playlist),
        "token_revoked_at": last_modified.isoformat(),
    }
    return _json_response(
        request=request, payload=payload, last_modified=last_modified
    )


@router.get("/playlists/{playlist_id}/feed/changes")
async def feed_changes(
    playlist_id: str,
    since: str,
    request: Request,
    store: Store = Depends(get_store),
):
    """Changelog of episode membership since an ISO8601 instant.

    ``added`` holds episodes with ``added_at`` after ``since``.
    ``removed`` / ``reordered`` are reserved (always ``[]`` in v1 — see the
    module docstring); clients detect reorders by diffing ``items``.
    """
    playlist = await _published_playlist(store, playlist_id)
    since_dt = _parse_since(since)
    entries = await store.playlists.list_entries(playlist.playlist_id)
    last_modified = playlist_last_modified(playlist, entries)

    def _entry(e) -> dict:
        return {
            "episode_id": str(e.episode.episode_id),
            "position": e.position,
            "added_at": ensure_aware(e.added_at).isoformat(),
        }

    items = [_entry(e) for e in entries]
    added = [
        item
        for item, e in zip(items, entries)
        if ensure_aware(e.added_at) > since_dt
    ]
    payload = {
        "playlist_id": str(playlist.playlist_id),
        "since": since_dt.isoformat(),
        "last_modified": last_modified.isoformat(),
        "added": added,
        "removed": [],
        "reordered": [],
        "items": items,
    }
    return _json_response(
        request=request, payload=payload, last_modified=last_modified
    )
