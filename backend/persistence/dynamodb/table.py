"""DynamoDB table provisioning for the tunedin single-table design (Linear: XIN-89).

``ensure_table()`` is the table-evolution mechanism — there is no Alembic
equivalent for DynamoDB.  It creates the table when missing (on-demand
billing, 3 GSIs, ALL projections) and issues ``UpdateTable`` when the
actual GSI set differs from the desired set, so adding a fourth GSI later
is a spec change, not a migration script.  Point-in-time recovery (PITR)
is enabled on every run.

The ``client`` argument is any object exposing the async DynamoDB client
API (``describe_table``, ``create_table``, ``update_table``,
``update_continuous_backups``, ``get_waiter``) — in production an
``aioboto3`` client (one per store, shared across callers); in tests this
may be a thin async wrapper around sync boto3 under moto.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

DEFAULT_TABLE_NAME = "tunedin"

# Key attribute names (all String type).
_PK = "pk"
_SK = "sk"
_GSI1PK = "gsi1pk"
_GSI1SK = "gsi1sk"
_GSI2PK = "gsi2pk"
_GSI2SK = "gsi2sk"
_GSI3PK = "gsi3pk"
_GSI3SK = "gsi3sk"

GSI_DEFS: List[Dict[str, Any]] = [
    {
        # gsi1 — natural-key point lookups (feed by rss_url hash, episode by
        # id, tag by name+category hash, user by email hash, playlist by id).
        "IndexName": "gsi1",
        "KeySchema": [
            {"AttributeName": _GSI1PK, "KeyType": "HASH"},
            {"AttributeName": _GSI1SK, "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
    {
        # gsi2 — status scans (feeds by sync_status, task logs by
        # type/status), ordered by created timestamp.
        "IndexName": "gsi2",
        "KeySchema": [
            {"AttributeName": _GSI2PK, "KeyType": "HASH"},
            {"AttributeName": _GSI2SK, "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
    {
        # gsi3 — sparse unprocessed-episode index for the LLM pipeline.
        "IndexName": "gsi3",
        "KeySchema": [
            {"AttributeName": _GSI3PK, "KeyType": "HASH"},
            {"AttributeName": _GSI3SK, "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
]

_ATTRIBUTE_DEFINITIONS = [
    {"AttributeName": name, "AttributeType": "S"}
    for name in (_PK, _SK, _GSI1PK, _GSI1SK, _GSI2PK, _GSI2SK, _GSI3PK, _GSI3SK)
]

_MAIN_KEY_SCHEMA = [
    {"AttributeName": _PK, "KeyType": "HASH"},
    {"AttributeName": _SK, "KeyType": "RANGE"},
]


def _desired_gsi_names() -> List[str]:
    return [g["IndexName"] for g in GSI_DEFS]


async def _enable_pitr(client: Any, table_name: str) -> None:
    """Enable point-in-time recovery on the table (idempotent)."""
    await client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )


async def _table_description(client: Any, table_name: str) -> Optional[Dict[str, Any]]:
    """Return the table description, or ``None`` when the table is missing."""
    try:
        resp = await client.describe_table(TableName=table_name)
    except client.exceptions.ResourceNotFoundException:
        return None
    return resp["Table"]


async def _create_table(client: Any, table_name: str) -> None:
    await client.create_table(
        TableName=table_name,
        KeySchema=_MAIN_KEY_SCHEMA,
        AttributeDefinitions=_ATTRIBUTE_DEFINITIONS,
        GlobalSecondaryIndexes=GSI_DEFS,
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    await waiter.wait(TableName=table_name)


async def _add_missing_gsis(
    client: Any,
    table_name: str,
    missing: List[Dict[str, Any]],
    *,
    gsi_wait_timeout: float,
) -> None:
    """Add GSIs that exist in the spec but not on the table (UpdateTable).

    AWS allows only ONE GSI create per ``UpdateTable`` call, and forbids
    starting another index update while one is CREATING — so each missing
    GSI is added in its own ``UpdateTable`` call, and this does not return
    until that index's ``IndexStatus`` is ``ACTIVE`` before adding the next.
    """
    for gsi in missing:
        index_name = gsi["IndexName"]
        log.info("Creating GSI %s on DynamoDB table %s", index_name, table_name)
        await client.update_table(
            TableName=table_name,
            AttributeDefinitions=_attr_defs_for(gsi),
            GlobalSecondaryIndexUpdates=[{"Create": gsi}],
        )
        await _wait_for_gsi_active(
            client, table_name, index_name, timeout=gsi_wait_timeout
        )


def _attr_defs_for(gsi: Dict[str, Any]) -> List[Dict[str, str]]:
    """Attribute definitions for exactly the key attributes a GSI uses."""
    names = {k["AttributeName"] for k in gsi["KeySchema"]}
    return [d for d in _ATTRIBUTE_DEFINITIONS if d["AttributeName"] in names]


_GSI_POLL_INTERVAL = 2.0  # seconds between describe_table polls


async def _wait_for_gsi_active(
    client: Any, table_name: str, index_name: str, *, timeout: float
) -> None:
    """Poll ``describe_table`` until the GSI's ``IndexStatus`` is ACTIVE.

    There is no built-in waiter for GSI creation, so we poll.  Raises
    :class:`TimeoutError` if the index is not ACTIVE within ``timeout``
    seconds.
    """
    deadline = time.monotonic() + timeout
    while True:
        desc = await _table_description(client, table_name)
        gsis = {
            g["IndexName"]: g
            for g in (desc or {}).get("GlobalSecondaryIndexes", [])
        }
        if gsis.get(index_name, {}).get("IndexStatus") == "ACTIVE":
            log.info("GSI %s on DynamoDB table %s is ACTIVE", index_name, table_name)
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"GSI {index_name} on DynamoDB table {table_name} did not "
                f"become ACTIVE within {timeout} seconds"
            )
        await asyncio.sleep(_GSI_POLL_INTERVAL)


async def ensure_table(
    client: Any,
    *,
    table_name: str = DEFAULT_TABLE_NAME,
    gsi_wait_timeout: float = 300.0,
    enable_pitr: bool = True,
) -> str:
    """Ensure the tunedin table exists with the desired GSIs and PITR.

    * Table missing → create it (on-demand, 3 GSIs, wait for ACTIVE).
    * Table present but GSI set differs → add the missing GSIs one at a
      time via ``UpdateTable``, waiting for each to become ACTIVE before
      adding the next (the table-evolution mechanism).  ``ensure_table``
      does not return until every newly added GSI is ACTIVE.
    * PITR is enabled on every run (idempotent) unless ``enable_pitr``
      is False.  DynamoDB Local rejects ``update_continuous_backups``
      with ``UnsupportedOperationException``
      (awslabs/amazon-dynamodb-local-samples#17), so integration tests
      against the emulator pass ``enable_pitr=False``.

    Returns ``"created"``, ``"updated"`` (GSIs were added), or ``"exists"``.
    """
    desc = await _table_description(client, table_name)
    if desc is None:
        log.info("DynamoDB table %s missing; creating", table_name)
        await _create_table(client, table_name)
        if enable_pitr:
            await _enable_pitr(client, table_name)
        return "created"

    existing_gsis = {
        g["IndexName"] for g in desc.get("GlobalSecondaryIndexes", [])
    }
    missing = [
        gsi for gsi in GSI_DEFS if gsi["IndexName"] not in existing_gsis
    ]
    if missing:
        names = [g["IndexName"] for g in missing]
        log.info("DynamoDB table %s missing GSIs %s; updating", table_name, names)
        await _add_missing_gsis(
            client, table_name, missing, gsi_wait_timeout=gsi_wait_timeout
        )
        if enable_pitr:
            await _enable_pitr(client, table_name)
        return "updated"

    if enable_pitr:
        await _enable_pitr(client, table_name)
    return "exists"
