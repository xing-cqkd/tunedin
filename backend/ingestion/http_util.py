"""Shared HTTP helpers for the ingestion layer.

Covers XIN-49 (one client-lifecycle idiom), XIN-56 (shared GET-with-retry),
XIN-59 (response size cap), and XIN-62 (SSRF gate on feed URLs).
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import socket
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import httpx

logger = logging.getLogger(__name__)

#: Feeds larger than this are rejected before parsing (XIN-59).
DEFAULT_MAX_FEED_BYTES = 15 * 1024 * 1024

#: Statuses worth one more attempt (with backoff); everything else 4xx is terminal.
RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}

#: Terminal client errors — raised immediately, no retry (XIN-56).
TERMINAL_STATUSES = {400, 401, 403, 404, 405, 410}

#: Redirects are followed but capped (XIN-62).
MAX_REDIRECTS = 5


class FeedTooLargeError(ValueError):
    """Raised when a feed exceeds the max download size (XIN-59). Terminal."""


@asynccontextmanager
async def maybe_client(
    client: Optional[httpx.AsyncClient] = None,
    *,
    timeout: float = 30.0,
) -> AsyncIterator[httpx.AsyncClient]:
    """Yield ``client``, or a fresh ``AsyncClient`` closed on exit (XIN-49).

    Replaces the copy-pasted "if client is None: create; close_client=True;
    finally: close" idiom. Usage::

        async with maybe_client(client, timeout=timeout) as c:
            ...
    """
    if client is not None:
        yield client
        return
    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, max_redirects=MAX_REDIRECTS
    ) as c:
        yield c


async def _backoff(attempt: int, reason: str, max_retries: int) -> None:
    delay = (2 ** attempt) * 0.5 + random.uniform(0, 0.25)
    logger.warning(
        "%s Retrying in %.2fs (attempt %d/%d)...",
        reason,
        delay,
        attempt + 1,
        max_retries,
    )
    await asyncio.sleep(delay)


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
) -> httpx.Response:
    """Buffered GET with retry on 429/5xx and network errors (XIN-56).

    Terminal statuses (400/404/410/...) raise immediately without retry so
    callers can mark the feed errored only for terminal failures.
    """
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url, headers=headers, params=params)
            if response.status_code in TERMINAL_STATUSES:
                response.raise_for_status()
            if response.status_code in RETRYABLE_STATUSES and attempt < max_retries:
                await _backoff(attempt, f"HTTP {response.status_code} from {url}.", max_retries)
                continue
            response.raise_for_status()
            return response
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            if attempt < max_retries:
                await _backoff(attempt, f"Network error fetching {url} ({exc}).", max_retries)
                continue
            raise
    raise AssertionError("unreachable")  # pragma: no cover


async def fetch_limited(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    max_bytes: Optional[int] = None,
    max_retries: int = 3,
) -> Tuple[bytes, httpx.Headers, int]:
    """Streamed GET with retry and a hard response-size cap (XIN-56, XIN-59).

    Returns ``(body, headers, status_code)``. Raises :class:`FeedTooLargeError`
    (terminal) when Content-Length or the streamed body exceeds ``max_bytes``
    (defaults to ``DEFAULT_MAX_FEED_BYTES``; resolved at call time so tests
    can monkeypatch the module constant).
    """
    if max_bytes is None:
        max_bytes = DEFAULT_MAX_FEED_BYTES
    for attempt in range(max_retries + 1):
        try:
            async with client.stream("GET", url, headers=headers) as response:
                # 304 is not an error — return it to the caller before any
                # raise_for_status (some httpx versions reject 304 there).
                if response.status_code == 304:
                    return b"", response.headers, 304
                if response.status_code in TERMINAL_STATUSES:
                    response.raise_for_status()
                if response.status_code in RETRYABLE_STATUSES and attempt < max_retries:
                    await _backoff(
                        attempt, f"HTTP {response.status_code} from {url}.", max_retries
                    )
                    continue
                response.raise_for_status()

                try:
                    content_length = int(response.headers.get("Content-Length", "0") or 0)
                except ValueError:
                    content_length = 0
                if content_length > max_bytes:
                    raise FeedTooLargeError(
                        f"Feed at {url} declares {content_length} bytes "
                        f"(limit {max_bytes})"
                    )

                chunks: List[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        raise FeedTooLargeError(
                            f"Feed at {url} exceeded {max_bytes} bytes while streaming"
                        )
                    chunks.append(chunk)
                return b"".join(chunks), response.headers, response.status_code
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            if attempt < max_retries:
                await _backoff(attempt, f"Network error fetching {url} ({exc}).", max_retries)
                continue
            raise
    raise AssertionError("unreachable")  # pragma: no cover


def validate_feed_url(url: str) -> str:
    """SSRF gate for feed URLs (XIN-62).

    Allows only http/https, strips embedded credentials, resolves the host
    and rejects private/loopback/link-local/multicast/reserved IPs and the
    cloud metadata endpoint. Returns the sanitized URL. Raises ``ValueError``
    (fail closed) on any violation or unresolvable host.
    """
    raw = (url or "").strip()
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"Feed URL must be http(s), got scheme {parts.scheme!r}")
    host = parts.hostname
    if not host:
        raise ValueError(f"Feed URL has no host: {raw!r}")
    if parts.username or parts.password:
        logger.warning("Stripped credentials from feed URL host %s", host)

    try:
        addr_infos = socket.getaddrinfo(host, None)
    except socket.gaierror as exc:
        raise ValueError(f"Feed URL host does not resolve: {host} ({exc})")

    for info in addr_infos:
        ip = ipaddress.ip_address(info[4][0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise ValueError(f"Feed URL host {host} resolves to non-public IP {ip}")

    netloc = host
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path or "", parts.query, ""))
