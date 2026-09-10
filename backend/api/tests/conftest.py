"""Shared backend-agnostic scaffolding for the backend/api/ test modules.

The API layer only talks to the ``Store``/repository protocol, so the same
tests run against the SQLAlchemy (in-memory SQLite) and DynamoDB (moto)
backends — this mirrors
``backend/persistence/tests/test_repository_conformance.py``. All tests
are synchronous: seeding runs via ``asyncio.run`` and HTTP assertions go
through FastAPI's ``TestClient``, so no event-loop nesting is involved.

Test modules define their own ``_seed(factory)`` coroutine (the per-file
data); the ``api_client`` fixture below parametrizes over the backends
and yields ``(client, seed)``.

NOTE: these tests are written but not run here — Chester runs the suite
himself. Only ``python -m py_compile`` sanity checks were done at author
time.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.api import create_app
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.models import Base
from backend.persistence.sqlalchemy_store import SQLAlchemyStore


class _SqliteBackend:
    async def setup(self) -> None:
        self._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._factory = async_sessionmaker(
            bind=self._engine, class_=AsyncSession, expire_on_commit=False
        )

    def store_factory(self):
        return lambda: SQLAlchemyStore(self._factory)

    async def teardown(self) -> None:
        await self._engine.dispose()


class _DynamoDBBackend:
    async def setup(self) -> None:
        self._mock = mock_aws()
        self._mock.start()
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        self._client = AsyncBoto3Client(sync)
        self._table_name = f"api-test-{uuid4().hex}"
        await ensure_table(self._client, table_name=self._table_name)

    def store_factory(self):
        return lambda: DynamoDBStore(
            client=self._client, table_name=self._table_name
        )

    async def teardown(self) -> None:
        await self._client.close()
        self._mock.stop()


_BACKENDS = {"sqlalchemy": _SqliteBackend, "dynamodb": _DynamoDBBackend}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(params=sorted(_BACKENDS))
def api_client(request):
    """A TestClient with a seeded, published playlist; yields (client, seed).

    The seed comes from the test module's own ``_seed(factory)``
    coroutine, so per-file data stays per-file while the backend
    parametrization lives here.
    """
    backend = _BACKENDS[request.param]()
    _run(backend.setup())
    try:
        factory = backend.store_factory()
        seed = _run(request.module._seed(factory))
        app = create_app(store_factory=factory)
        with TestClient(app) as client:
            yield client, seed
    finally:
        _run(backend.teardown())
