"""Typed errors for the ingestion pipeline (XIN-53).

Error policy (single, pipeline-wide):

1. The service layer records a failure in the ``feeds`` row
   (``sync_status=error``, ``error_count`` bumped, ``last_fetched_at`` set)
   and commits, then RAISES a typed error. It never swallows, and it does not
   log at the raise site — the DB row is the durable record.
2. The catching caller logs exactly once, in its own format (crawler logs a
   warning per feed; the batch path logs per feed; batch_runner logs per
   feed). This replaces the old log-and-reraise + re-log double logging.
3. Transport failures (httpx) propagate as ``FeedFetchError`` with the
   original exception chained as ``__cause__``; parse failures propagate as
   ``FeedParseError``; caller-supplied bad input raises ``FeedValidationError``
   / ``FeedNotFoundError``.

The fetch/parse errors deliberately subclass ``ValueError`` so existing
``pytest.raises(ValueError)`` contracts keep passing; ``__cause__`` preserves
the original exception for callers that need it (e.g. a 429 throttle check
can read ``FeedFetchError.status_code`` instead of catching
``httpx.HTTPStatusError`` directly).
"""

from typing import Optional

import httpx


class IngestionError(Exception):
    """Base class for all ingestion-domain errors."""


class FeedValidationError(IngestionError, ValueError):
    """A caller-supplied feed identifier or discovered podcast failed validation
    (missing feed_url, malformed identifier). Fix the caller input."""


class FeedNotFoundError(IngestionError, ValueError):
    """A feed ID or URL did not resolve to a known Feed row."""


class FeedFetchError(IngestionError):
    """Fetching the feed bytes failed (network/HTTP). The original exception
    (e.g. ``httpx.ConnectError``) is chained as ``__cause__``."""

    @property
    def status_code(self) -> Optional[int]:
        """HTTP status code when the chained cause is an ``httpx.HTTPStatusError``."""
        cause = self.__cause__
        if isinstance(cause, httpx.HTTPStatusError):
            return cause.response.status_code
        return None


class FeedParseError(IngestionError, ValueError):
    """The feed bytes were fetched but could not be parsed into a feed."""


class FeedSyncError(IngestionError):
    """Sync failed after a successful parse (persistence step). The catching
    batch caller marks the feed row ERROR before continuing to the next feed."""
