"""Tests for backend.ingestion.http_util (XIN-49, XIN-56, XIN-59, XIN-62, XIN-75)."""

import httpx
import pytest

from backend.ingestion import http_util
from backend.ingestion.http_util import (
    FeedTooLargeError,
    fetch_limited,
    get_with_retry,
    maybe_client,
    validate_feed_url,
)


def _public_dns(monkeypatch, ip="93.184.216.34"):
    """Fake DNS: every host resolves to a public IP (sandbox DNS is intercepted)."""
    monkeypatch.setattr(
        http_util.socket,
        "getaddrinfo",
        lambda host, port: [(2, 1, 6, "", (ip, 0))],
    )


# --- XIN-49: maybe_client ----------------------------------------------------

async def test_maybe_client_reuses_passed_client():
    async with httpx.AsyncClient(trust_env=False) as client:
        async with maybe_client(client) as c:
            assert c is client
        assert not client.is_closed  # not ours to close


async def test_maybe_client_closes_created_client(monkeypatch):
    # Sandbox no_proxy contains bracketed IPv6 this httpx version can't parse;
    # strip proxy env so client construction is hermetic.
    for var in list(__import__("os").environ):
        if "proxy" in var.lower():
            monkeypatch.delenv(var, raising=False)
    async with maybe_client() as c:
        assert isinstance(c, httpx.AsyncClient)
    assert c.is_closed


# --- XIN-56: get_with_retry --------------------------------------------------

async def test_get_with_retry_retries_429_then_succeeds():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(429, text="slow down")
        return httpx.Response(200, text="ok")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        resp = await get_with_retry(client, "https://example.com/", max_retries=3)
    assert resp.status_code == 200
    assert len(calls) == 3


async def test_get_with_retry_terminal_404_no_retry():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(404, text="nope")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await get_with_retry(client, "https://example.com/", max_retries=3)
    assert len(calls) == 1  # terminal: no retry


# --- XIN-59: fetch_limited ---------------------------------------------------

async def test_fetch_limited_rejects_declared_oversize():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"Content-Length": str(10**9)}, content=b"x"
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FeedTooLargeError):
            await fetch_limited(client, "https://example.com/feed", max_bytes=100)


async def test_fetch_limited_rejects_streamed_oversize():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 1000)  # no Content-Length

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FeedTooLargeError):
            await fetch_limited(client, "https://example.com/feed", max_bytes=100)


async def test_fetch_limited_returns_body_under_cap():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"ETag": '"abc"'}, content=b"<rss/>")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        body, headers, status = await fetch_limited(
            client, "https://example.com/feed", max_bytes=100
        )
    assert body == b"<rss/>"
    assert status == 200
    assert headers["ETag"] == '"abc"'


# --- XIN-62: validate_feed_url -----------------------------------------------

def test_validate_rejects_non_http_scheme():
    with pytest.raises(ValueError):
        validate_feed_url("ftp://example.com/feed")


def test_validate_strips_userinfo(monkeypatch):
    _public_dns(monkeypatch)
    assert (
        validate_feed_url("https://user:pass@example.com/feed")
        == "https://example.com/feed"
    )


def test_validate_rejects_private_ip(monkeypatch):
    _public_dns(monkeypatch, ip="192.168.1.10")
    with pytest.raises(ValueError):
        validate_feed_url("https://example.com/feed")


def test_validate_rejects_metadata_endpoint(monkeypatch):
    _public_dns(monkeypatch, ip="169.254.169.254")
    with pytest.raises(ValueError):
        validate_feed_url("http://169.254.169.254/latest/meta-data/")


def test_validate_rejects_unresolvable(monkeypatch):
    def boom(host, port):
        raise http_util.socket.gaierror("nope")

    monkeypatch.setattr(http_util.socket, "getaddrinfo", boom)
    with pytest.raises(ValueError):
        validate_feed_url("https://does-not-exist.invalid/feed")


def test_validate_accepts_public(monkeypatch):
    _public_dns(monkeypatch)
    assert validate_feed_url("https://example.com/feed.xml") == "https://example.com/feed.xml"


# --- XIN-59 + XIN-75 through fetch_and_parse ---------------------------------

async def test_fetch_and_parse_rejects_oversize_feed(monkeypatch):
    from backend.ingestion.parser import PodcastFeedParser

    monkeypatch.setattr(http_util, "DEFAULT_MAX_FEED_BYTES", 100)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 5000)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FeedTooLargeError):
            await PodcastFeedParser.fetch_and_parse(
                "https://example.com/feed", client=client
            )


def test_validate_rejects_loopback_literal():
    # No DNS needed: literal IPs are checked without resolution.
    with pytest.raises(ValueError):
        validate_feed_url("http://127.0.0.1:9999/feed")
