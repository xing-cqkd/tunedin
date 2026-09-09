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
    """

    def __init__(
        self,
        client: Any = None,
        *,
        table_name: str = DEFAULT_TABLE_NAME,
        region_name: str = "us-east-1",
    ) -> None:
        if client is None:
            from backend.persistence.dynamodb.client import create_client

            client = create_client(region_name=region_name)
            self._owns_client = True
        else:
            self._owns_client = False
        self._client = client
        self._table_name = table_name
        self._feeds = _FeedRepository(client, table_name)
        self._episodes = _EpisodeRepository(client, table_name)
        self._insights = _InsightRepository(client, table_name)
        self._tags = _TagRepository(client, table_name)
        self._users = _UserRepository(client, table_name)
        self._playlists = _PlaylistRepository(client, table_name)
        self._progress = _ProgressRepository(client, table_name)
        self._task_logs = _TaskLogRepository(client, table_name)

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

    async def commit(self) -> None:
        """No-op: writes are write-through, ``save()`` already persisted."""

    async def rollback(self) -> None:
        """No-op: write-through writes cannot be undone (documented in the
        ABC — there is no pending transaction to discard)."""

    async def close(self) -> None:
        """Release the store's resources.

        Closes the DynamoDB client only when this store created it; an
        injected client belongs to the caller (e.g. a test fixture sharing
        one client across stores) and is left open.
        """
        if self._owns_client:
            await self._client.close()
