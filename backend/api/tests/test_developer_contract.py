"""Tests for the developer polling contract (XIN-104).

Backend-agnostic like ``test_feeds.py``: the same tests run against the
SQLAlchemy (in-memory SQLite) and DynamoDB (moto) backends. All tests are
synchronous: seeding runs via ``asyncio.run`` and HTTP assertions go
through FastAPI's ``TestClient``.

NOTE: these tests are written but not run here — Chester runs the suite
himself. Only ``python -m py_compile`` sanity checks were done at author
time.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.api import create_app
from backend.api.developer import RateLimiter
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.models import (
    Base,
    CuratedPlaylist,
    Episode,
    Feed,
    User,
)
from backend.persistence.sqlalchemy_store import SQLAlchemyStore


# ---------------------------------------------------------------------------
# Backend contexts (mirrors backend/api/tests/test_feeds.py)
# ---------------------------------------------------------------------------


class _SqliteBackend:
    async def setup(self) -> None:
        self._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._factory = async_sessionmaker(
            bind=self._engine, class_=AsyncSession, expire_on_commit=False
        )

    def store_factory(self):
        return lambda: SQLAlchemyStore(self._factory)

    async def teardown(self) -> None:
        await self._engine.dispose()


class _DynamoDBBackend:
    async def setup(self) -> None:
        self._mock = mock_aws()
        self._mock.start()
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        self._client = AsyncBoto3Client(sync)
        self._table_name = f"api-dev-{uuid4().hex}"
        await ensure_table(self._client, table_name=self._table_name)

    def store_factory(self):
        return lambda: DynamoDBStore(
            client=self._client, table_name=self._table_name
        )

    async def teardown(self) -> None:
        await self._client.close()
        self._mock.stop()


_BACKENDS = {"sqlalchemy": _SqliteBackend, "dynamodb": _DynamoDBBackend}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(params=sorted(_BACKENDS))
def api_client(request):
    """A TestClient with a seeded, published playlist; yields (client, seed)."""
    backend = _BACKENDS[request.param]()
    _run(backend.setup())
    try:
        factory = backend.store_factory()
        seed = _run(_seed(factory))
        app = create_app(store_factory=factory)
        with TestClient(app) as client:
            yield client, seed
    finally:
        _run(backend.teardown())


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

    draft = _run(make_unpublished())
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
    ep3 = _run(
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
