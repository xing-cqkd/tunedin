"""Tests for the developer polling contract (XIN-104).

Backend-agnostic like ``test_feeds.py``: the same tests run against the
SQLAlchemy (in-memory SQLite) and DynamoDB (moto) backends — the shared
scaffolding (backend contexts, the parametrized ``api_client`` fixture)
lives in ``conftest.py``; only the per-file ``_seed`` stays here. All
tests are synchronous: seeding runs via ``asyncio.run`` and HTTP
assertions go through FastAPI's ``TestClient``.

NOTE: these tests are written but not run here — Chester runs the suite
himself. Only ``python -m py_compile`` sanity checks were done at author
time.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from uuid import uuid4

from starlette.requests import Request

from backend.api.developer import RateLimiter, feed_changes
from backend.api.rss import ensure_aware
from backend.persistence.repositories import PlaylistEpisodeEntry
from backend.persistence.models import (
    CuratedPlaylist,
    Episode,
    Feed,
    User,
)


async def _seed(factory) -> dict:
    async with factory() as store:
        user = await store.users.save(User(email=f"{uuid4().hex}@example.com"))
        feed = await store.feeds.save(
            Feed(
                rss_url=f"https://example.com/{uuid4().hex}.xml",
                title="Seed Feed",
                sync_status="pending",
            )
        )
        ep1 = await store.episodes.save(
            Episode(
                feed_id=feed.feed_id,
                title="Ep One",
                audio_url="https://example.com/audio1.mp3",
                published_at=datetime(2020, 5, 4, 12, 0, tzinfo=timezone.utc),
                guid="orig-guid-1",
            )
        )
        ep2 = await store.episodes.save(
            Episode(
                feed_id=feed.feed_id,
                title="Ep Two",
                audio_url="https://example.com/audio2.m4a",
                published_at=datetime(2021, 8, 9, 12, 0, tzinfo=timezone.utc),
            )
        )
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="My Mix")
        )
        await store.playlists.add_episode(pl.playlist_id, ep1.episode_id, 1)
        await store.playlists.add_episode(pl.playlist_id, ep2.episode_id, 0)
        published = await store.playlists.publish(pl.playlist_id, "unlisted")
        return {
            "playlist": published,
            "feed_id": feed.feed_id,
            "ep1": ep1,
            "ep2": ep2,
        }


async def _add_episode(factory, feed_id, title, position, playlist_id):
    async with factory() as store:
        ep = await store.episodes.save(
            Episode(
                feed_id=feed_id,
                title=title,
                audio_url=f"https://example.com/{uuid4().hex}.mp3",
                published_at=datetime(2022, 1, 1, tzinfo=timezone.utc),
            )
        )
        await store.playlists.add_episode(playlist_id, ep.episode_id, position)
        return ep


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_feed_metadata(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    resp = client.get(f"/api/v1/playlists/{pl.playlist_id}/feed")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    assert "max-age=900" in resp.headers.get("cache-control", "")
    assert resp.headers.get("etag")
    assert resp.headers.get("last-modified")

    body = resp.json()
    assert body["playlist_id"] == str(pl.playlist_id)
    assert body["title"] == "My Mix"
    assert body["slug"] == pl.slug
    assert body["visibility"] == "unlisted"
    assert body["episode_count"] == 2
    assert body["last_modified"]
    # Unlisted: the subscribable URLs carry the token; canonical shape kept.
    assert body["feed_url"].endswith(f"/f/{pl.slug}/feed.xml?t={pl.token}")
    assert body["landing_url"].endswith(f"/f/{pl.slug}?t={pl.token}")


def test_feed_metadata_404(api_client):
    client, seed = api_client
    # Unknown id.
    assert (
        client.get(f"/api/v1/playlists/{uuid4()}/feed").status_code == 404
    )
    # Malformed id.
    assert client.get("/api/v1/playlists/not-a-uuid/feed").status_code == 404
    # Never-published playlist (no slug).
    factory = client.app.state.store_factory

    async def make_unpublished():
        async with factory() as store:
            user = await store.users.save(
                User(email=f"{uuid4().hex}@example.com")
            )
            return await store.playlists.save(
                CuratedPlaylist(user_id=user.user_id, title="Draft")
            )

    draft = asyncio.run(make_unpublished())
    assert (
        client.get(f"/api/v1/playlists/{draft.playlist_id}/feed").status_code
        == 404
    )


def test_feed_metadata_conditional(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed"
    first = client.get(url)
    assert first.status_code == 200
    etag = first.headers["etag"]

    assert client.get(url, headers={"If-None-Match": etag}).status_code == 304
    ims = client.get(
        url, headers={"If-Modified-Since": "Wed, 01 Jan 2030 00:00:00 GMT"}
    )
    assert ims.status_code == 304


def test_rotate_token_flow(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    old_token = pl.token
    url = f"/api/v1/playlists/{pl.playlist_id}/feed/rotate-token"

    resp = client.post(url)
    assert resp.status_code == 200
    body = resp.json()
    assert body["playlist_id"] == str(pl.playlist_id)
    assert body["token_revoked_at"]
    assert f"?t={old_token}" not in body["feed_url"]
    new_token = body["feed_url"].split("?t=")[1]
    assert new_token and new_token != old_token
    assert body["landing_url"].endswith(f"/f/{pl.slug}?t={new_token}")

    # Old token URL is dead on the content endpoint; new one serves RSS.
    assert (
        client.get(f"/f/{pl.slug}/feed.xml?t={old_token}").status_code == 410
    )
    assert (
        client.get(f"/f/{pl.slug}/feed.xml?t={new_token}").status_code == 200
    )

    # Metadata reflects the rotated token and its last_modified advanced.
    meta = client.get(f"/api/v1/playlists/{pl.playlist_id}/feed").json()
    assert meta["feed_url"].endswith(f"?t={new_token}")
    assert meta["last_modified"] >= body["token_revoked_at"]

    # Unknown playlist -> 404.
    assert (
        client.post(f"/api/v1/playlists/{uuid4()}/feed/rotate-token").status_code
        == 404
    )


def test_changes_detects_add(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    factory = client.app.state.store_factory
    base = f"/api/v1/playlists/{pl.playlist_id}/feed/changes"

    # Everything added since the epoch shows up.
    first = client.get(base, params={"since": "2020-01-01T00:00:00Z"})
    assert first.status_code == 200
    body = first.json()
    assert body["playlist_id"] == str(pl.playlist_id)
    assert body["last_modified"]
    assert {a["episode_id"] for a in body["added"]} == {
        str(seed["ep1"].episode_id),
        str(seed["ep2"].episode_id),
    }
    # Full ordered snapshot: ep2 (position 0) first; reserved fields empty.
    assert [i["episode_id"] for i in body["items"]] == [
        str(seed["ep2"].episode_id),
        str(seed["ep1"].episode_id),
    ]
    assert body["removed"] == []
    assert body["reordered"] == []

    # A later add is detected on the next poll; nothing else changes.
    # position=-1 keeps the ordering deterministic: both backends order by
    # (position ASC, episode_id ASC) and episode_ids are random UUIDs, so
    # two entries at position 0 would make items[0] a coin flip.
    ep3 = asyncio.run(
        _add_episode(
            factory, seed["feed_id"], "Ep Three", -1, pl.playlist_id
        )
    )
    second = client.get(base, params={"since": body["last_modified"]})
    added_ids = [a["episode_id"] for a in second.json()["added"]]
    assert added_ids == [str(ep3.episode_id)]
    assert [i["episode_id"] for i in second.json()["items"]][0] == str(
        ep3.episode_id
    )

    # Polling again with the fresh last_modified yields no changes.
    third = client.get(
        base, params={"since": second.json()["last_modified"]}
    )
    assert third.json()["added"] == []
    assert third.json()["last_modified"] == second.json()["last_modified"]


def test_changes_since_invalid(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    resp = client.get(
        f"/api/v1/playlists/{pl.playlist_id}/feed/changes",
        params={"since": "not-a-date"},
    )
    assert resp.status_code == 400


def test_changes_conditional(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed/changes"
    params = {"since": "2020-01-01T00:00:00Z"}
    first = client.get(url, params=params)
    assert first.status_code == 200
    etag = first.headers["etag"]

    again = client.get(url, params=params, headers={"If-None-Match": etag})
    assert again.status_code == 304


def test_rate_limit(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    # Tighten the limiter for this test only (default is 600/15min).
    client.app.state.rate_limiter = RateLimiter(limit=2, window_seconds=60)
    url = f"/api/v1/playlists/{pl.playlist_id}/feed"

    assert client.get(url).status_code == 200
    assert client.get(url).status_code == 200
    limited = client.get(url)
    assert limited.status_code == 429
    assert limited.headers.get("retry-after")
    assert limited.json()["detail"]["error"] == "rate_limit_exceeded"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _make_draft(factory):
    """A never-published playlist (no slug) — resolves to 404 everywhere."""
    async with factory() as store:
        user = await store.users.save(
            User(email=f"{uuid4().hex}@example.com")
        )
        return await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Draft")
        )


async def _publish_public(factory, playlist_id):
    async with factory() as store:
        await store.playlists.publish(playlist_id, "public")


# ---------------------------------------------------------------------------
# XIN-116: cache-poisoning and conditional-request correctness
# ---------------------------------------------------------------------------


def test_rotate_token_never_304(api_client):
    """POST rotate-token always mutates: even ``If-None-Match: *`` (or a
    matching ETag) must 200 — never 304 — and the response is
    ``Cache-Control: no-store`` (documented contract, previously
    untested)."""
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed/rotate-token"

    first = client.post(url)
    assert first.status_code == 200
    assert first.headers["cache-control"] == "no-store"
    first_token = first.json()["feed_url"].split("?t=")[1]

    # The bug: the old conditional logic 304'd AFTER rotating the token.
    second = client.post(url, headers=[("If-None-Match", "*")])
    assert second.status_code == 200
    assert second.headers["cache-control"] == "no-store"
    second_token = second.json()["feed_url"].split("?t=")[1]
    assert second_token and second_token != first_token

    # A matching ETag must not 304 either.
    third = client.post(url, headers={"If-None-Match": first.headers["etag"]})
    assert third.status_code == 200


def test_ims_ignored_when_inm_present(api_client):
    """RFC 9110 13.1.4 on the JSON surface: If-Modified-Since MUST be
    ignored when If-None-Match is present but matches nothing."""
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed"
    resp = client.get(
        url,
        headers={
            "If-None-Match": '"no-such-etag"',
            "If-Modified-Since": "Wed, 01 Jan 2030 00:00:00 GMT",
        },
    )
    assert resp.status_code == 200


def test_changes_cache_scope(api_client):
    """The changes endpoint applies the same private/public Cache-Control
    scoping as the feed metadata (unlisted URLs embed the ?t= token)."""
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed/changes"
    params = {"since": "2020-01-01T00:00:00Z"}

    unlisted = client.get(url, params=params)
    assert unlisted.status_code == 200
    assert unlisted.headers["cache-control"].split(",")[0].strip() == "private"

    asyncio.run(
        _publish_public(client.app.state.store_factory, pl.playlist_id)
    )
    public = client.get(url, params=params)
    assert public.status_code == 200
    cc = public.headers["cache-control"]
    assert cc.split(",")[0].strip() == "public"
    assert "private" not in cc


# ---------------------------------------------------------------------------
# XIN-117: /changes None-handling
# ---------------------------------------------------------------------------


def test_changes_added_at_none(api_client):
    """``added_at=None`` entries must not 500 /changes: they fall back to
    the playlist creation date (mirrors the RSS pubDate fallback)."""
    client, seed = api_client
    pl = seed["playlist"]

    async def _entries():
        factory = client.app.state.store_factory
        async with factory() as store:
            return await store.playlists.list_entries(pl.playlist_id)

    real = asyncio.run(_entries())
    assert len(real) == 2
    no_dates = [
        PlaylistEpisodeEntry(
            episode=e.episode, position=e.position, added_at=None
        )
        for e in real
    ]

    class _FakePlaylists:
        async def get_by_id(self, pid):
            return pl

        async def list_entries(self, pid):
            return no_dates

    class _FakeStore:
        def __init__(self):
            self.playlists = _FakePlaylists()

    def _request():
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [],
                "server": ("testserver", 80),
                "scheme": "http",
                "query_string": b"",
            }
        )

    resp = asyncio.run(
        feed_changes(
            str(pl.playlist_id), "2020-01-01T00:00:00Z", _request(),
            _FakeStore(),
        )
    )
    assert resp.status_code == 200
    body = json.loads(resp.body)
    fallback = ensure_aware(pl.created_at).isoformat()
    assert len(body["items"]) == 2
    assert all(i["added_at"] == fallback for i in body["items"])
    # since (2020) predates the fallback: both episodes count as added.
    assert len(body["added"]) == 2

    # A since after the fallback yields no additions — no TypeError from
    # the ``added`` filter either.
    later = asyncio.run(
        feed_changes(
            str(pl.playlist_id), "2030-01-01T00:00:00Z", _request(),
            _FakeStore(),
        )
    )
    assert later.status_code == 200
    assert json.loads(later.body)["added"] == []


# ---------------------------------------------------------------------------
# XIN-118: coverage gaps
# ---------------------------------------------------------------------------


def test_changes_404(api_client):
    """The changes endpoint 404s like the metadata endpoint does: unknown
    id, malformed UUID, never-published playlist."""
    client, _ = api_client
    params = {"since": "2020-01-01T00:00:00Z"}
    base = "/api/v1/playlists/{}/feed/changes"
    assert client.get(base.format(uuid4()), params=params).status_code == 404
    assert (
        client.get(base.format("not-a-uuid"), params=params).status_code
        == 404
    )
    draft = asyncio.run(_make_draft(client.app.state.store_factory))
    assert (
        client.get(base.format(draft.playlist_id), params=params).status_code
        == 404
    )


def test_feed_metadata_public_playlist(api_client):
    """Public playlist: ``_cache_scope``'s public branch and tokenless
    ``_playlist_urls`` (``test_feed_metadata`` only seeds unlisted)."""
    client, seed = api_client
    pl = seed["playlist"]
    asyncio.run(
        _publish_public(client.app.state.store_factory, pl.playlist_id)
    )

    resp = client.get(f"/api/v1/playlists/{pl.playlist_id}/feed")
    assert resp.status_code == 200
    cc = resp.headers["cache-control"]
    assert cc.split(",")[0].strip() == "public"
    assert "private" not in cc
    body = resp.json()
    assert body["visibility"] == "public"
    assert body["feed_url"].endswith(f"/f/{pl.slug}/feed.xml")
    assert body["landing_url"].endswith(f"/f/{pl.slug}")
    assert "?t=" not in body["feed_url"]
    assert "?t=" not in body["landing_url"]


def test_json_conditional_variants(api_client):
    """Conditional-request coverage on the JSON surface: ``If-None-Match:
    *`` -> 304, weak validators match via the shared parser, multi-value
    headers match, and malformed If-Modified-Since is ignored (200)."""
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/api/v1/playlists/{pl.playlist_id}/feed"
    etag = client.get(url).headers["etag"]

    assert client.get(url, headers={"If-None-Match": "*"}).status_code == 304
    assert (
        client.get(url, headers={"If-None-Match": f"W/{etag}"}).status_code
        == 304
    )
    multi = client.get(url, headers={"If-None-Match": f'"zzz", {etag}'})
    assert multi.status_code == 304
    assert (
        client.get(url, headers={"If-None-Match": '"aaa", "bbb"'}).status_code
        == 200
    )
    malformed = client.get(url, headers={"If-Modified-Since": "garbage"})
    assert malformed.status_code == 200


def test_changes_since_equals_added_at_boundary(api_client):
    """``added`` uses strict ``>``: ``since == added_at`` excludes the
    episode. Pins the boundary semantics."""
    client, seed = api_client
    pl = seed["playlist"]

    async def _entries():
        factory = client.app.state.store_factory
        async with factory() as store:
            return await store.playlists.list_entries(pl.playlist_id)

    entries = asyncio.run(_entries())
    target = entries[0]
    since = ensure_aware(target.added_at).isoformat()
    body = client.get(
        f"/api/v1/playlists/{pl.playlist_id}/feed/changes",
        params={"since": since},
    ).json()
    assert str(target.episode.episode_id) not in {
        a["episode_id"] for a in body["added"]
    }


def test_rate_limiter_retry_after_value():
    """Unit: Retry-After seconds stay within the configured window."""
    limiter = RateLimiter(limit=2, window_seconds=60)
    assert limiter.check("1.2.3.4") is None
    assert limiter.check("1.2.3.4") is None
    retry = limiter.check("1.2.3.4")
    assert retry is not None
    assert 0 < retry <= 60


def test_rate_limiter_sliding_window_expiry(monkeypatch):
    """Unit: hits slide out of the window; the bucket recovers."""
    now = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    limiter = RateLimiter(limit=1, window_seconds=60)
    assert limiter.check("9.9.9.9") is None
    assert limiter.check("9.9.9.9") is not None
    now[0] += 61  # the first hit slides out of the window
    assert limiter.check("9.9.9.9") is None


def test_rate_limiter_per_ip_isolation():
    """Unit: buckets are per-IP — one IP's traffic never limits another."""
    limiter = RateLimiter(limit=1, window_seconds=60)
    assert limiter.check("1.1.1.1") is None
    assert limiter.check("1.1.1.1") is not None  # over the limit
    assert limiter.check("2.2.2.2") is None  # separate bucket


def test_rate_limit_retry_after_agrees_with_body(api_client):
    """HTTP: the Retry-After header matches the body's retry_after."""
    client, seed = api_client
    pl = seed["playlist"]
    client.app.state.rate_limiter = RateLimiter(limit=1, window_seconds=60)
    url = f"/api/v1/playlists/{pl.playlist_id}/feed"
    assert client.get(url).status_code == 200
    limited = client.get(url)
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == str(
        limited.json()["detail"]["retry_after"]
    )
