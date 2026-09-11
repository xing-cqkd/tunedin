"""TuneIn HTTP API (FastAPI).

XIN-98 introduces the first public surface: published curated-playlist
feeds at ``/f/<slug>``. XIN-31's open question ("where does the HTTP layer
live?") is resolved for the feed surface only by this package — FastAPI was
already a declared dependency in ``backend/requirements.txt``.

FastAPI is imported lazily inside :func:`create_app` so that
:mod:`backend.api.rss` (pure XML rendering, no web framework) stays
importable in environments without the web dependencies installed.
"""

from __future__ import annotations

from typing import Any, Callable


def create_app(*, store_factory: Callable[[], Any] | None = None):
    """Build the FastAPI application.

    ``store_factory`` is a zero-arg callable returning a
    :class:`~backend.persistence.repositories.Store`. It is called once per
    request inside ``async with`` (commit on clean exit) and the store is
    closed afterwards, so factories may hand out a fresh store per request
    sharing an engine/client, or a caller-owned store. Defaults to
    ``settings.open_store`` — the configured database backend.
    """
    from fastapi import Depends, FastAPI

    from backend.api.developer import RateLimiter, rate_limited
    from backend.api.developer import router as developer_router
    from backend.api.feeds import router as feeds_router
    from backend.api.worker import router as worker_router

    app = FastAPI(title="TuneIn API")
    if store_factory is None:  # lazy: settings mirrors env at import time
        from settings import open_store

        store_factory = open_store
    app.state.store_factory = store_factory
    # Generous per-IP sliding window (XIN-104); tests may replace it with a
    # tighter limiter via ``app.state.rate_limiter``. The limiter keys on
    # request.client.host (the direct TCP peer) — deployments behind a
    # proxy/LB must resolve the real client IP (e.g. honor X-Forwarded-For
    # only from trusted proxies via middleware), or all clients behind the
    # proxy share one bucket.
    app.state.rate_limiter = RateLimiter()
    app.include_router(
        feeds_router,
        # XIN-118 (Chester's call): the public /f/<slug> endpoints render
        # full RSS from the database with no authentication, so they share
        # the developer API's generous per-IP limiter instead of staying
        # unlimited. Legitimate podcatchers poll at most every ~15 minutes
        # — far under 600/15min — and conditional requests keep repeat
        # polls cheap.
        dependencies=[Depends(rate_limited)],
    )
    app.include_router(developer_router)
    # XIN-31: worker webhook for PROCESS_EPISODE tasks. Validates the payload
    # and 501s until the transcription/insight worker is implemented.
    app.include_router(worker_router)
    return app
