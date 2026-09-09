"""DynamoDB implementation of the Store/repository protocol (Linear: XIN-90).

``DynamoDBStore`` wraps one async DynamoDB client (aioboto3 in production,
an injected moto-compatible adapter in tests) shared across all eight
repository objects, and implements every repository ABC defined in
:mod:`backend.persistence.repositories`.

Write semantics: write-through — ``save()`` persists immediately and
``commit()`` is a no-op safety net. ``rollback()`` is likewise a no-op
with the documented caveat: writes that already went through ``save()``
cannot be undone (there is no transaction to roll back on a write-through
backend).
"""

from __future__ import annotations

from typing import Any, Optional

from backend.persistence.dynamodb.repositories import (
    _EpisodeRepository,
    _FeedRepository,
    _InsightRepository,
    _PlaylistRepository,
    _ProgressRepository,
    _TagRepository,
    _TaskLogRepository,
    _UserRepository,
)
from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME
from backend.persistence.repositories import (
    EpisodeRepository,
    FeedRepository,
    InsightRepository,
    PlaylistRepository,
    ProgressRepository,
    Store,
    TagRepository,
    TaskLogRepository,
    UserRepository,
)


class DynamoDBStore(Store):
    """Unit of work backed by a DynamoDB single-table client.

    ``client`` is any object exposing the async DynamoDB client API
    (``get_item``, ``put_item``, ``query``, ``scan``,
    ``transact_write_items``, ``batch_write_item``, ``batch_get_item``,
    ``exceptions``, ``close``). When omitted, one aioboto3 client is
    created for the store (production path) and closed with it; an
    injected client is owned by the caller and left open by
    :meth:`close` so fixtures can share one client across stores.

    The production client is created through aioboto3's async client
    context, so the store MUST be used as ``async with`` (``async with
    DynamoDBStore(...) as store:`` or ``async with open_store() as
    store:``) — repository access before entering has no client yet.
    """

    def __init__(
        self,
        client: Any = None,
        *,
        table_name: str = DEFAULT_TABLE_NAME,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ) -> None:
        # aioboto3 clients are created via an async context manager
        # (``ClientCreatorContext``) that must be entered before the client
        # is usable. The context is therefore entered in ``__aenter__`` —
        # use ``async with DynamoDBStore(...)`` / ``async with
        # open_store()`` — and exited in :meth:`close`.
        if client is None:
            from backend.persistence.dynamodb.client import create_client

            self._client_ctx: Any = create_client(
                region_name=region_name, endpoint_url=endpoint_url
            )
            self._owns_client = True
            client = None
        else:
            self._client_ctx = None
            self._owns_client = False
        # Tracks whether the owned client context has been entered.
        # Guards both double-``__aenter__`` (entering twice would create a
        # second aioboto3 client and orphan the first) and ``close()``
        # before entering (exiting a never-entered context raises
        # ``AttributeError`` inside aioboto3).
        self._entered = False
        self._client = client
        self._table_name = table_name
        if client is not None:
            self._init_repositories(client)

    def _init_repositories(self, client: Any) -> None:
        """Build the eight repository objects over the given client."""
        self._feeds = _FeedRepository(client, self._table_name)
        self._episodes = _EpisodeRepository(client, self._table_name)
        self._insights = _InsightRepository(client, self._table_name)
        self._tags = _TagRepository(client, self._table_name)
        self._users = _UserRepository(client, self._table_name)
        self._playlists = _PlaylistRepository(client, self._table_name)
        self._progress = _ProgressRepository(client, self._table_name)
        self._task_logs = _TaskLogRepository(client, self._table_name)

    @property
    def table_name(self) -> str:
        """The DynamoDB table this store reads and writes."""
        return self._table_name

    # -- Store properties ---------------------------------------------------

    @property
    def feeds(self) -> FeedRepository:
        return self._feeds

    @property
    def episodes(self) -> EpisodeRepository:
        return self._episodes

    @property
    def insights(self) -> InsightRepository:
        return self._insights

    @property
    def tags(self) -> TagRepository:
        return self._tags

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def playlists(self) -> PlaylistRepository:
        return self._playlists

    @property
    def progress(self) -> ProgressRepository:
        return self._progress

    @property
    def task_logs(self) -> TaskLogRepository:
        return self._task_logs

    # -- Unit of work -------------------------------------------------------

    async def __aenter__(self) -> "DynamoDBStore":
        """Enter the store, entering the owned aioboto3 client context first.

        Stores built with an injected client (tests) are already usable and
        this is a no-op beyond returning ``self``. Always use ``async with``
        — repository access before entering raises ``AttributeError``
        because the client does not exist yet.

        Re-entering an already-entered store is a no-op returning ``self``
        (documented, not an error): entering the aioboto3 client context a
        second time would create a second client and orphan the first, so
        the ``_entered`` flag makes the second ``__aenter__`` return early
        with the client and repositories untouched.
        """
        if self._entered:
            return self
        if self._owns_client:
            assert self._client_ctx is not None
            self._client = await self._client_ctx.__aenter__()
            # Mark entered BEFORE _init_repositories so that if repository
            # construction fails midway, close() still exits the context
            # instead of leaking the client.
            self._entered = True
            self._init_repositories(self._client)
        else:
            self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the store: ABC commit/rollback semantics, then release."""
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            await self.close()

    async def commit(self) -> None:
        """No-op: writes are write-through, ``save()`` already persisted."""

    async def rollback(self) -> None:
        """No-op: write-through writes cannot be undone (documented in the
        ABC — there is no pending transaction to discard)."""

    async def close(self) -> None:
        """Release the store's resources.

        Exits the owned aioboto3 client context (idempotent — safe to call
        any number of times, and safe to call on a store that was never
        entered, in which case it is a no-op); an injected client belongs
        to the caller (e.g. a test fixture sharing one client across
        stores) and is left open.
        """
        if self._owns_client and self._entered:
            self._entered = False
            ctx, self._client_ctx = self._client_ctx, None
            assert ctx is not None
            await ctx.__aexit__(None, None, None)
