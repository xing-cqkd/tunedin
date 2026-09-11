"""Tests for feed URL canonicalization (XIN-44)."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.canonicalize import canonicalize_feed_url
from backend.ingestion.discovery import DiscoveryService
from backend.ingestion.errors import FeedValidationError
from backend.ingestion.models import Podcast
from backend.persistence.models.base import Base
from backend.persistence.sqlalchemy_store import SQLAlchemyStore


@pytest.fixture
async def in_memory_store():
    """SQLAlchemyStore backed by an in-memory SQLite db."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    store = SQLAlchemyStore(session_factory)
    yield store

    await store.close()
    await engine.dispose()


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Case and default-port normalization.
        (
            "HTTPS://Feeds.Megaphone.fm:443/hubermanlab",
            "https://feeds.megaphone.fm/hubermanlab",
        ),
        (
            "http://example.com:80/feed.xml",
            "http://example.com/feed.xml",
        ),
        # Non-default ports are preserved.
        (
            "https://example.com:8443/feed.xml",
            "https://example.com:8443/feed.xml",
        ),
        # Bare "/" collapses; deeper paths keep case but lose trailing slashes.
        ("https://example.com/", "https://example.com"),
        ("https://example.com", "https://example.com"),
        ("https://example.com/Feed.XML", "https://example.com/Feed.XML"),
        ("https://example.com/Feed.XML/", "https://example.com/Feed.XML"),
        # Tracking parameters are stripped; the rest are sorted and kept.
        (
            "https://example.com/feed?utm_source=x&b=2&a=1",
            "https://example.com/feed?a=1&b=2",
        ),
        (
            "https://example.com/feed?fbclid=abc&gclid=def",
            "https://example.com/feed",
        ),
        (
            "https://example.com/feed?ref=podcast&source=itunes",
            "https://example.com/feed",
        ),
        # Fragments are dropped.
        (
            "https://example.com/feed#top",
            "https://example.com/feed",
        ),
        # Blank values and duplicates survive re-encoding.
        (
            "https://example.com/feed?flag&b=%2F",
            "https://example.com/feed?b=%2F&flag=",
        ),
    ],
)
def test_canonicalize_feed_url_cases(raw: str, expected: str) -> None:
    assert canonicalize_feed_url(raw) == expected


def test_canonicalize_feed_url_is_idempotent() -> None:
    raw = "HTTPS://Example.COM:443/a/b/?utm_source=x&z=1#frag"
    once = canonicalize_feed_url(raw)
    assert canonicalize_feed_url(once) == once


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "not a url",
        "ftp://example.com/feed.xml",
        "//example.com/feed.xml",
        "https://",
        "https:///feed.xml",
        123,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
    ],
)
def test_canonicalize_feed_url_rejects_invalid(raw) -> None:
    with pytest.raises(FeedValidationError):
        canonicalize_feed_url(raw)


class TestFeedIdentityCanonicalization:
    """XIN-44 regression: equivalent URL spellings share one feed row."""

    @pytest.mark.asyncio
    async def test_equivalent_urls_share_one_feed_row(self, in_memory_store):
        discovery = DiscoveryService()
        variants = [
            "https://feeds.megaphone.fm/hubermanlab",
            "HTTPS://FEEDS.MEGAPHONE.FM:443/hubermanlab?utm_source=x#top",
            "https://feeds.megaphone.fm/hubermanlab/",
        ]
        feeds = [
            await discovery.save_podcast(
                in_memory_store, Podcast(title=f"Show {i}", feed_url=url)
            )
            for i, url in enumerate(variants)
        ]
        first_id = feeds[0].feed_id
        assert all(f.feed_id == first_id for f in feeds)
        assert all(
            f.rss_url == "https://feeds.megaphone.fm/hubermanlab" for f in feeds
        )
        assert len(await in_memory_store.feeds.list_all()) == 1

    @pytest.mark.asyncio
    async def test_distinct_urls_stay_distinct(self, in_memory_store):
        discovery = DiscoveryService()
        a = await discovery.save_podcast(
            in_memory_store,
            Podcast(title="A", feed_url="https://example.com/a.xml"),
        )
        b = await discovery.save_podcast(
            in_memory_store,
            Podcast(title="B", feed_url="https://example.com/b.xml"),
        )
        assert a.feed_id != b.feed_id
