"""DynamoDB client construction for the tunedin backend (Linear: XIN-90).

Production always uses ``aioboto3`` — the application is asyncio-based
and blocking ``boto3`` would stall the event loop. Tests inject a
moto-compatible async adapter (see :mod:`backend.persistence.dynamodb.testing`)
through ``DynamoDBStore(client=...)`` instead of calling anything here.
"""

from __future__ import annotations

from typing import Any


def create_client(*, region_name: str = "us-east-1") -> Any:
    """Create one shared async DynamoDB client (aioboto3) for a store.

    Uses botocore's standard retry mode so throttled requests back off
    instead of surfacing immediately. The caller owns the client and must
    ``await client.close()`` when done — ``DynamoDBStore`` does this in
    :meth:`close` when it created the client itself.
    """
    import aioboto3
    from botocore.config import Config

    session = aioboto3.Session()
    return session.client(
        "dynamodb",
        region_name=region_name,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
