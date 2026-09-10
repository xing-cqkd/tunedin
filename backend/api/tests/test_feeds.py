"""Tests for the public RSS feed endpoints (XIN-98).

Backend-agnostic: the same tests run against the SQLAlchemy (in-memory
SQLite) and DynamoDB (moto) backends — the API layer only talks to the
``Store``/repository protocol. All tests are synchronous: seeding runs via
``asyncio.run`` and HTTP assertions go through FastAPI's ``TestClient``,
so no event-loop nesting is involved.
"""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import format_datetime
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
from backend.api.rss import ITUNES_NS, TUNEDIN_NS, build_rss, ensure_aware, rfc2822
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.repositories import PlaylistEpisodeEntry
from backend.persistence.models import (
    Base,
    CuratedPlaylist,
    Episode,
    Feed,
    User,
)
from backend.persistence.sqlalchemy_store import SQLAlchemyStore


# ---------------------------------------------------------------------------
# Backend contexts (mirrors backend/persistence/tests/test_repository_conformance.py)
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
        self._table_name = f"api-feeds-{uuid4().hex}"
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
                duration=3725,  # 1:02:05
                published_at=datetime(2020, 5, 4, 12, 0, tzinfo=timezone.utc),
                summary="First episode summary",
                guid="orig-guid-1",
                image_url="https://example.com/art1.png",
                explicit=False,
            )
        )
        ep2 = await store.episodes.save(
            Episode(
                feed_id=feed.feed_id,
                title="Ep Two",
                audio_url="https://example.com/audio2.m4a",
                # duration None, explicit True, no guid/image on purpose
                published_at=datetime(2021, 8, 9, 12, 0, tzinfo=timezone.utc),
                summary="Second episode summary",
                explicit=True,
            )
        )
        pl = await store.playlists.save(
            CuratedPlaylist(
                user_id=user.user_id,
                title="My Mix",
                description="A test mix",
            )
        )
        # ep1 at position 1, ep2 at position 0 -> feed order is [ep2, ep1]
        await store.playlists.add_episode(pl.playlist_id, ep1.episode_id, 1)
        await store.playlists.add_episode(pl.playlist_id, ep2.episode_id, 0)
        published = await store.playlists.publish(pl.playlist_id, "unlisted")
        entries = await store.playlists.list_entries(pl.playlist_id)
        return {
            "playlist": published,
            "ep1": ep1,
            "ep2": ep2,
            "entries": {e.episode.episode_id: e for e in entries},
        }


def _rss(client, slug, token=None):
    url = f"/f/{slug}/feed.xml"
    if token:
        url += f"?t={token}"
    return client.get(url)


def _parse(body: bytes) -> ET.Element:
    return ET.fromstring(body)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_feed_xml_structure_and_order(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    resp = _rss(client, pl.slug, token=pl.token)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/rss+xml")

    root = _parse(resp.content)
    assert root.tag == "rss" and root.attrib["version"] == "2.0"
    channel = root.find("channel")
    assert channel.find("title").text == "My Mix"
    assert channel.find("description").text == "A test mix"

    items = channel.findall("item")
    assert len(items) == 2
    # Curator position order: ep2 (position 0) first.
    assert items[0].find("title").text == "Ep Two"
    assert items[1].find("title").text == "Ep One"

    guids = [i.find("guid").text for i in items]
    assert guids[0] == f"tunedin:{pl.playlist_id}:{seed['ep2'].episode_id}"
    assert guids[1] == f"tunedin:{pl.playlist_id}:{seed['ep1'].episode_id}"
    assert all(i.find("guid").attrib["isPermaLink"] == "false" for i in items)

    # Original episode GUID in the tunedin:sourceGuid extension tag.
    ns = {"tunedin": TUNEDIN_NS}
    assert items[1].find("tunedin:sourceGuid", ns).text == "orig-guid-1"
    assert items[0].find("tunedin:sourceGuid", ns) is None  # ep2 has no guid

    # Enclosures redirect to the publisher audio URL — never proxied.
    enc = [i.find("enclosure") for i in items]
    assert enc[0].attrib["url"] == "https://example.com/audio2.m4a"
    assert enc[0].attrib["type"] == "audio/x-m4a"
    assert enc[1].attrib["url"] == "https://example.com/audio1.mp3"
    assert enc[1].attrib["type"] == "audio/mpeg"


def test_pubdate_is_added_date_and_description_has_original_date(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    resp = _rss(client, pl.slug, token=pl.token)
    items = _parse(resp.content).find("channel").findall("item")

    # item[0] is ep2: pubDate == its added-to-playlist date ...
    added = seed["entries"][seed["ep2"].episode_id].added_at
    if added.tzinfo is None:
        added = added.replace(tzinfo=timezone.utc)
    assert items[0].find("pubDate").text == format_datetime(added)
    # ... and the ORIGINAL episode publish date is in the description.
    desc = items[0].find("description").text
    assert "Originally published:" in desc
    assert "2021" in desc  # ep2's published_at, not the added date


def test_content_negotiation(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/f/{pl.slug}?t={pl.token}"

    rss = client.get(url, headers={"Accept": "application/rss+xml"})
    assert rss.headers["content-type"].startswith("application/rss+xml")

    pod = client.get(url, headers={"User-Agent": "Overcast/2024.10 (podcast)"})
    assert pod.headers["content-type"].startswith("application/rss+xml")

    browser = client.get(
        url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}
    )
    assert browser.status_code == 200
    assert browser.headers["content-type"].startswith("text/html")
    assert 'rel="alternate"' in browser.text or "rel='alternate'" in browser.text
    assert "application/rss+xml" in browser.text

    # Realistic browser Accept headers include application/xml at q=0.9 —
    # those clients must still land on the HTML page, not the feed.
    real_browser = client.get(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        },
    )
    assert real_browser.status_code == 200
    assert real_browser.headers["content-type"].startswith("text/html")
    assert "application/rss+xml" not in real_browser.headers["content-type"]


def test_feed_page_token_gating(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    browser_ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}

    revoked = client.get(f"/f/{pl.slug}?t=wrong-token", headers=browser_ua)
    assert revoked.status_code == 410
    assert revoked.headers["content-type"].startswith("text/html")
    assert "revoked by the curator" in revoked.text

    unknown = client.get("/f/no-such-slug", headers=browser_ua)
    assert unknown.status_code == 404


def test_unlisted_token_gating(api_client):
    client, seed = api_client
    pl = seed["playlist"]

    no_token = client.get(f"/f/{pl.slug}/feed.xml")
    assert no_token.status_code == 410
    assert "revoked by the curator" in no_token.text

    wrong = client.get(f"/f/{pl.slug}/feed.xml?t=wrong-token")
    assert wrong.status_code == 410
    assert "revoked by the curator" in wrong.text

    ok = client.get(f"/f/{pl.slug}/feed.xml?t={pl.token}")
    assert ok.status_code == 200


def test_public_feed_needs_no_token(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    _run(_make_public(client, seed))
    resp = client.get(f"/f/{pl.slug}/feed.xml")
    assert resp.status_code == 200


async def _make_public(client, seed):
    # Re-publish as public through a throwaway store on the same backend.
    factory = client.app.state.store_factory
    async with factory() as store:
        await store.playlists.publish(seed["playlist"].playlist_id, "public")


def test_rotated_token_invalidates_old(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    old_token = pl.token

    async def rotate():
        factory = client.app.state.store_factory
        async with factory() as store:
            return await store.playlists.rotate_token(pl.playlist_id)

    new_token = _run(rotate())
    assert new_token != old_token

    assert client.get(f"/f/{pl.slug}/feed.xml?t={old_token}").status_code == 410
    assert (
        client.get(f"/f/{pl.slug}/feed.xml?t={new_token}").status_code == 200
    )


def test_unknown_slug_404(api_client):
    client, _ = api_client
    resp = client.get("/f/no-such-slug/feed.xml")
    assert resp.status_code == 404


def test_etag_and_conditional_requests(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/f/{pl.slug}/feed.xml?t={pl.token}"

    first = client.get(url)
    assert first.status_code == 200
    etag = first.headers.get("etag")
    assert etag
    assert "max-age=900" in first.headers.get("cache-control", "")
    assert first.headers.get("last-modified")

    not_modified = client.get(url, headers={"If-None-Match": etag})
    assert not_modified.status_code == 304

    ims = client.get(
        url, headers={"If-Modified-Since": "Wed, 01 Jan 2030 00:00:00 GMT"}
    )
    assert ims.status_code == 304

    # Non-matching validators must NOT yield 304 — serve the feed.
    wrong_etag = client.get(url, headers={"If-None-Match": '"no-such-etag"'})
    assert wrong_etag.status_code == 200
    stale_ims = client.get(
        url, headers={"If-Modified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"}
    )
    assert stale_ims.status_code == 200


def test_cache_control_privacy_for_unlisted_feeds(api_client):
    client, seed = api_client
    pl = seed["playlist"]

    # Unlisted (token-gated) feeds must be private: a shared cache must
    # never serve a cached 200 to an invalid-token request (410).
    unlisted = _rss(client, pl.slug, token=pl.token)
    cc = unlisted.headers["cache-control"]
    assert cc.split(",")[0].strip() == "private"
    assert "max-age=900" in cc

    # Public feeds stay publicly cacheable.
    _run(_make_public(client, seed))
    public = _rss(client, pl.slug)
    cc = public.headers["cache-control"]
    assert cc.split(",")[0].strip() == "public"
    assert "private" not in cc


def test_itunes_tags_and_fallbacks(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    root = _parse(_rss(client, pl.slug, token=pl.token).content)
    channel = root.find("channel")
    ns = {"itunes": ITUNES_NS}

    # Channel fallbacks (documented in backend/api/rss.py).
    assert channel.find("itunes:author", ns).text == "TuneIn curator"
    category = channel.find("itunes:category", ns)
    assert category is not None
    assert category.attrib["text"] == "Society & Culture"
    owner = channel.find("itunes:owner", ns)
    assert owner is not None
    assert owner.find("itunes:name", ns).text == "TuneIn"
    assert owner.find("itunes:email", ns).text == "noreply@tunedin.app"
    assert channel.find("itunes:summary", ns).text == "A test mix"
    # ep2 is explicit -> channel-level "yes".
    assert channel.find("itunes:explicit", ns).text == "yes"
    # Channel image falls back to the first episode with artwork (ep1).
    assert (
        channel.find("itunes:image", ns).attrib["href"]
        == "https://example.com/art1.png"
    )

    items = channel.findall("item")
    # ep1 (index 1): 3725s -> 1:02:05, explicit False -> "no".
    assert items[1].find("itunes:duration", ns).text == "1:02:05"
    assert items[1].find("itunes:explicit", ns).text == "no"
    assert (
        items[1].find("itunes:image", ns).attrib["href"]
        == "https://example.com/art1.png"
    )
    # ep2 (index 0): no duration -> tag omitted; explicit True -> "yes".
    assert items[0].find("itunes:duration", ns) is None
    assert items[0].find("itunes:explicit", ns).text == "yes"


def _browser_page(client, slug, token=None):
    url = f"/f/{slug}"
    if token:
        url += f"?t={token}"
    return client.get(
        url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}
    )


def test_html_stub_cache_control_scoping(api_client):
    client, seed = api_client
    pl = seed["playlist"]

    # Unlisted (token-gated) stub must be private: a shared cache must
    # never serve a cached 200 to an invalid-token request (410).
    unlisted = _browser_page(client, pl.slug, token=pl.token)
    assert unlisted.status_code == 200
    cc = unlisted.headers["cache-control"]
    assert cc.split(",")[0].strip() == "private"
    assert "max-age=900" in cc

    # Public stubs stay publicly cacheable.
    _run(_make_public(client, seed))
    public = _browser_page(client, pl.slug)
    assert public.status_code == 200
    cc = public.headers["cache-control"]
    assert cc.split(",")[0].strip() == "public"
    assert "private" not in cc


def test_weak_etag_if_none_match(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    url = f"/f/{pl.slug}/feed.xml?t={pl.token}"

    etag = client.get(url).headers["etag"]
    assert etag.startswith('"') and etag.endswith('"')

    # A weak validator for the same opaque tag must still yield 304 —
    # proves the W/ stripping in _parse_if_none_match.
    weak = client.get(url, headers={"If-None-Match": f"W/{etag}"})
    assert weak.status_code == 304


def test_pubdate_falls_back_to_channel_date(api_client):
    client, seed = api_client
    pl = seed["playlist"]
    entry = seed["entries"][seed["ep1"].episode_id]
    entry_no_date = PlaylistEpisodeEntry(
        episode=entry.episode, position=entry.position, added_at=None
    )

    # Must render without raising; the item gets the channel date instead.
    body = build_rss(
        pl,
        [entry_no_date],
        page_url=f"http://testserver/f/{pl.slug}",
        feed_url=f"http://testserver/f/{pl.slug}/feed.xml",
    )
    items = _parse(body).find("channel").findall("item")
    assert len(items) == 1
    pub_date = items[0].find("pubDate").text
    assert pub_date is not None
    channel_date = ensure_aware(pl.created_at)
    assert pub_date == rfc2822(channel_date)


def test_html_stub_autodiscovery_token(api_client):
    client, seed = api_client
    pl = seed["playlist"]

    def alternate_href(resp):
        m = re.search(
            r"<link rel='alternate'[^>]*href='([^']+)'", resp.text
        )
        assert m, "no rel=alternate autodiscovery link in stub"
        return m.group(1)

    # Unlisted: the stub's autodiscovery link must carry the token so the
    # browser user can subscribe without hitting a 410.
    unlisted = _browser_page(client, pl.slug, token=pl.token)
    assert unlisted.status_code == 200
    assert f"?t={pl.token}" in alternate_href(unlisted)

    # Public: no token is needed, so the bare URL stays clean.
    _run(_make_public(client, seed))
    public = _browser_page(client, pl.slug)
    assert public.status_code == 200
    href = alternate_href(public)
    assert "?t=" not in href
    assert href.endswith(f"/f/{pl.slug}/feed.xml")
