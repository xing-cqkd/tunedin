"""Tests for DynamoDB table provisioning (Linear: XIN-89).

``ensure_table()`` is exercised against moto.  moto cannot intercept
aioboto3's async HTTP layer, so these tests drive it through a minimal
async adapter over a sync boto3 client — the production code path stays
fully async (aioboto3).
"""

import asyncio

import boto3
import pytest
from moto import mock_aws

from backend.persistence.dynamodb import table
from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME, ensure_table


class _AsyncWaiter:
    def __init__(self, waiter):
        self._waiter = waiter

    async def wait(self, **kwargs):
        await asyncio.to_thread(self._waiter.wait, **kwargs)


class AsyncBoto3Client:
    """Test-only async adapter over a sync boto3 client."""

    def __init__(self, client):
        self._client = client
        self.exceptions = client.exceptions

    def get_waiter(self, name):
        return _AsyncWaiter(self._client.get_waiter(name))

    def __getattr__(self, name):
        meth = getattr(self._client, name)

        async def _call(*args, **kwargs):
            return await asyncio.to_thread(meth, *args, **kwargs)

        return _call


@pytest.fixture()
def ddb():
    with mock_aws():
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        yield AsyncBoto3Client(sync), sync


def _gsi_names(desc):
    return {g["IndexName"] for g in desc["Table"]["GlobalSecondaryIndexes"]}


def _gsi_statuses(desc):
    return {
        g["IndexName"]: g.get("IndexStatus")
        for g in desc["Table"]["GlobalSecondaryIndexes"]
    }


def _create_table_with_gsis(sync, *index_names):
    """Create the tunedin table with only the named GSIs (an older schema)."""
    defs = {g["IndexName"]: g for g in table.GSI_DEFS}
    attr_names = {"pk", "sk"}
    for name in index_names:
        for k in defs[name]["KeySchema"]:
            attr_names.add(k["AttributeName"])
    sync.create_table(
        TableName=DEFAULT_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": n, "AttributeType": "S"} for n in sorted(attr_names)
        ],
        GlobalSecondaryIndexes=[defs[name] for name in index_names],
        BillingMode="PAY_PER_REQUEST",
    )


class TestEnsureTable:
    async def test_creates_missing_table(self, ddb):
        client, sync = ddb
        result = await ensure_table(client)
        assert result == "created"

        desc = sync.describe_table(TableName=DEFAULT_TABLE_NAME)
        assert _gsi_names(desc) == {"gsi1", "gsi2", "gsi3"}
        assert desc["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        key_schema = {
            k["AttributeName"]: k["KeyType"]
            for k in desc["Table"]["KeySchema"]
        }
        assert key_schema == {"pk": "HASH", "sk": "RANGE"}
        for gsi in desc["Table"]["GlobalSecondaryIndexes"]:
            assert gsi["Projection"]["ProjectionType"] == "ALL"

    async def test_idempotent_rerun(self, ddb):
        client, _ = ddb
        assert await ensure_table(client) == "created"
        assert await ensure_table(client) == "exists"

    async def test_adds_missing_gsi_via_update_table(self, ddb):
        client, sync = ddb
        # Simulate an older table that only has gsi1.
        _create_table_with_gsis(sync, "gsi1")

        result = await ensure_table(client)
        assert result == "updated"

        desc = sync.describe_table(TableName=DEFAULT_TABLE_NAME)
        assert _gsi_names(desc) == {"gsi1", "gsi2", "gsi3"}
        # ensure_table must not return until the added GSIs are ACTIVE.
        assert _gsi_statuses(desc) == {
            "gsi1": "ACTIVE",
            "gsi2": "ACTIVE",
            "gsi3": "ACTIVE",
        }
        # A further run is a no-op once the GSI set matches.
        assert await ensure_table(client) == "exists"

    async def test_adds_two_missing_gsis_sequentially(self, ddb):
        client, sync = ddb
        # Simulate an older table that only has gsi1; gsi2 and gsi3 must be
        # added one UpdateTable call at a time (AWS rejects batching), each
        # reaching ACTIVE before the next is added.
        _create_table_with_gsis(sync, "gsi1")

        result = await ensure_table(client, gsi_wait_timeout=60)
        assert result == "updated"

        desc = sync.describe_table(TableName=DEFAULT_TABLE_NAME)
        assert _gsi_names(desc) == {"gsi1", "gsi2", "gsi3"}
        assert _gsi_statuses(desc) == {
            "gsi1": "ACTIVE",
            "gsi2": "ACTIVE",
            "gsi3": "ACTIVE",
        }
        assert await ensure_table(client) == "exists"

    async def test_enables_pitr(self, ddb):
        client, sync = ddb
        await ensure_table(client)
        status = sync.describe_continuous_backups(
            TableName=DEFAULT_TABLE_NAME
        )["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"][
            "PointInTimeRecoveryStatus"
        ]
        assert status == "ENABLED"
