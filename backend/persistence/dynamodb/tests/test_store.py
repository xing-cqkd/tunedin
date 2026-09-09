"""Store lifecycle tests: idempotent close/enter guards (Linear: XIN-93).

Regression tests for the review fixes on ``DynamoDBStore``:
- ``close()`` on a never-entered store is a no-op (previously raised
  ``AttributeError`` inside aioboto3's ``ClientCreatorContext``).
- Re-entering an entered store is a no-op returning ``self`` (previously
  created a second aioboto3 client and orphaned the first).
"""

import pytest

from backend.persistence.dynamodb.store import DynamoDBStore


class _FakeClientCtx:
    """Stand-in for aioboto3's ClientCreatorContext (no network)."""

    def __init__(self):
        self.enters = 0
        self.exits = 0
        self.client = object()

    async def __aenter__(self):
        self.enters += 1
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exits += 1


def _owned_store_with_fake_ctx():
    """DynamoDBStore on the owned-client path with a fake context."""
    store = DynamoDBStore()
    fake = _FakeClientCtx()
    store._client_ctx = fake
    return store, fake


async def test_close_without_enter_is_noop():
    # Exact scenario from the review: owned client, never entered.
    # Constructing the real aioboto3 context makes no network calls.
    store = DynamoDBStore()
    await store.close()
    await store.close()  # idempotent


async def test_close_without_enter_with_fake_ctx_is_noop():
    store, fake = _owned_store_with_fake_ctx()
    await store.close()
    await store.close()
    assert fake.enters == 0
    assert fake.exits == 0


async def test_double_enter_is_noop_without_leak():
    store, fake = _owned_store_with_fake_ctx()

    first = await store.__aenter__()
    client_before = store._client
    repos_before = (
        store.feeds, store.episodes, store.insights, store.tags,
        store.users, store.playlists, store.progress, store.task_logs,
    )

    second = await store.__aenter__()

    assert second is store
    assert first is store
    assert fake.enters == 1, "second enter must not create another client"
    assert store._client is client_before, "client identity must be stable"
    repos_after = (
        store.feeds, store.episodes, store.insights, store.tags,
        store.users, store.playlists, store.progress, store.task_logs,
    )
    assert repos_after == repos_before, "repositories must not be rebuilt"

    await store.close()


async def test_enter_close_lifecycle():
    store, fake = _owned_store_with_fake_ctx()

    await store.__aenter__()
    assert fake.enters == 1

    await store.close()
    assert fake.exits == 1

    await store.close()
    assert fake.exits == 1, "close must be idempotent"


async def test_context_manager_protocol():
    store, fake = _owned_store_with_fake_ctx()
    async with store as s:
        assert s is store
        assert fake.enters == 1
    assert fake.exits == 1


async def test_injected_client_close_is_noop():
    client = object()
    store = DynamoDBStore(client=client)
    await store.close()
    await store.close()
    assert store._client is client
