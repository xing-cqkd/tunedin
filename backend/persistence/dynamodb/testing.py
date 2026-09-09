"""Test-only async adapter over a sync boto3 DynamoDB client (Linear: XIN-90).

moto 5.2.3 cannot intercept aioboto3's async HTTP layer — requests made
through aioboto3 would leak to real AWS. This adapter exposes the same
async method surface the production code expects (``query``, ``put_item``,
``transact_write_items``, ``get_waiter``, ``exceptions``, ...) by running
each sync boto3 call in a worker thread, so the full repository stack can
be exercised against moto with zero network access.

TEST-ONLY: production code must never import this module. ``DynamoDBStore``
accepts the adapter through its ``client=`` parameter; nothing in the
production path knows it exists.
"""

from __future__ import annotations

import asyncio
from typing import Any


class _AsyncWaiter:
    def __init__(self, waiter: Any) -> None:
        self._waiter = waiter

    async def wait(self, **kwargs: Any) -> None:
        await asyncio.to_thread(self._waiter.wait, **kwargs)


class AsyncBoto3Client:
    """Async facade over a sync boto3 DynamoDB client (moto-backed)."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self.exceptions = client.exceptions

    def get_waiter(self, name: str) -> _AsyncWaiter:
        return _AsyncWaiter(self._client.get_waiter(name))

    async def close(self) -> None:
        await asyncio.to_thread(self._client.close)

    def __getattr__(self, name: str) -> Any:
        meth = getattr(self._client, name)

        async def _call(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.to_thread(meth, *args, **kwargs)

        return _call
