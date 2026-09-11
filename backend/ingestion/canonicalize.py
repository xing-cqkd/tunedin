"""Feed URL canonicalization (XIN-44).

``feeds.rss_url`` carries a unique constraint, but the raw URL string was
stored verbatim: ``http`` vs ``https``, a trailing slash, default ports, or
tracking query params (``utm_*``, ``fbclid``...) all produced distinct rows
for the same podcast, splitting episode history across two ``feed_id``s and
doubling sync work.

This module owns *identity*: every lookup and insert goes through
:func:`canonicalize_feed_url` first (``DiscoveryService._get_or_create_feed``,
``FeedSyncService.sync_podcast_episodes_by_url``), and the canonical form is
what gets stored. It is deliberately separate from the SSRF gate
(``validate_feed_url`` in ``http_util.py``, XIN-62), which runs at fetch time
and answers a different question ("is this URL safe to fetch right now?").

The function is idempotent: ``canonicalize_feed_url(canonicalize_feed_url(u))``
always equals ``canonicalize_feed_url(u)``.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from backend.ingestion.errors import FeedValidationError

# Query params that identify the *link* rather than the *feed*. Dropped
# during canonicalization. Prefix rules (``utm_``) plus a fixed list.
_TRACKING_PARAM_PREFIXES = ("utm_",)
_TRACKING_PARAMS = frozenset(
    {
        "fbclid",
        "gclid",
        "gclsrc",
        "dclid",
        "msclkid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "_ga",
        "_gl",
        "ref",
        "source",
    }
)

_DEFAULT_PORTS = {"http": 80, "https": 443}


def _is_tracking_param(name: str) -> bool:
    lowered = name.lower()
    return lowered in _TRACKING_PARAMS or lowered.startswith(_TRACKING_PARAM_PREFIXES)


def canonicalize_feed_url(url: str) -> str:
    """Return the canonical identity form of a feed URL.

    Normalization rules:

    * scheme and host lowercased (``HTTP://Example.COM`` ->
      ``http://example.com``)
    * default ports stripped (``:80`` on http, ``:443`` on https)
    * trailing slashes collapsed (``https://example.com/feed/`` ->
      ``https://example.com/feed``; a bare ``/`` path collapses to empty)
    * tracking query params dropped (``utm_*``, ``fbclid``, ``gclid``, ...)
    * remaining query params sorted by (name, value) so param order does
      not create distinct identities
    * the fragment is dropped (never sent to a server)

    Raises :class:`FeedValidationError` (a ``ValueError``) when the input
    is not an absolute ``http(s)`` URL.
    """
    if not url or not isinstance(url, str):
        raise FeedValidationError(
            f"Invalid feed identifier {url!r}: expected an http(s) URL"
        )
    url = url.strip()
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise FeedValidationError(
            f"Invalid feed identifier {url!r}: expected an http(s) URL"
        )
    host = (parts.hostname or "").lower()
    if not host:
        raise FeedValidationError(
            f"Invalid feed identifier {url!r}: expected an http(s) URL"
        )

    # Strip the default port; keep explicit non-default ports.
    port = parts.port
    netloc = host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc = f"{host}:{port}"
    # Preserve userinfo if present (rare for feeds, but don't corrupt it).
    if parts.username:
        userinfo = parts.username
        if parts.password:
            userinfo += f":{parts.password}"
        netloc = f"{userinfo}@{netloc}"

    # Collapse trailing slashes: "/feed/", "/feed//", and "/feed" are the
    # same feed identity (XIN-44). The path is otherwise byte-identical
    # (paths are case-sensitive; "/Feed" and "/feed" may differ).
    path = parts.path.rstrip("/") or ""

    # Drop tracking params, sort the rest for order-independence. urlencode
    # re-applies percent-encoding so the output stays a valid URL (and the
    # function stays idempotent: parse_qsl(urlencode(pairs)) == pairs).
    kept = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not _is_tracking_param(k)
    )
    query = urlencode(kept)

    return urlunsplit((scheme, netloc, path, query, ""))
