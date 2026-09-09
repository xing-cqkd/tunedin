"""DynamoDB client construction for the tunedin backend (Linear: XIN-90).

Production always uses ``aioboto3`` — the application is asyncio-based
and blocking ``boto3`` would stall the event loop. Tests inject a
moto-compatible async adapter (see :mod:`backend.persistence.dynamodb.testing`)
through ``DynamoDBStore(client=...)`` instead of calling anything here.
"""

from __future__ import annotations

from typing import Any, Optional


def create_client(
    *,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
) -> Any:
    """Create one shared async DynamoDB client (aioboto3) for a store.

    ``endpoint_url`` points the client at DynamoDB Local (or another
    endpoint) for testing — ``None`` (the default) uses real AWS.
    Credentials always come from the standard AWS chain (env vars,
    ~/.aws, IAM role); they are never passed here.

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
        endpoint_url=endpoint_url,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
