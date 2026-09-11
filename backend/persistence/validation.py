"""Backend-agnostic persistence validation (Linear: XIN-95).

DynamoDB rejects any single item larger than 400 KiB. That guard lives here
— in the shared persistence package, not in the DynamoDB package — so every
backend enforces the same limit and raises the same error type.

Rationale: data that fits on one backend must fit on all of them. If the
guard were DynamoDB-only, an oversized entity could be written to SQLite
without complaint and then fail the migration to DynamoDB — a parity
surprise at the worst possible moment (cutover). With the guard shared,
both backends reject the write identically, at write time.

The size check measures a canonical JSON serialization of the entity's
column values, not DynamoDB's exact on-the-wire accounting (DynamoDB-JSON
attribute-type wrappers add a few bytes per attribute, and the service also
counts index-key overhead). It is therefore an approximation: anything it
rejects would certainly be rejected by DynamoDB; a borderline item it
accepts could still trip DynamoDB's own limit, in which case the service
error surfaces as-is. In practice the guard catches the realistic cases
(multi-hundred-KB transcripts or descriptions stuffed into a Text column)
identically on both backends.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Mapping
from uuid import UUID

# DynamoDB's per-item size limit.
MAX_ITEM_BYTES = 400 * 1024


class ItemTooLargeError(ValueError):
    """A single entity's serialized size exceeds :data:`MAX_ITEM_BYTES`.

    Raised identically by every backend — this is the whole point of the
    shared guard. Callers can catch this one type regardless of backend.
    """


def _json_default(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=repr)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def item_size_bytes(fields: Mapping[str, Any]) -> int:
    """Return the canonical serialized size of a field mapping, in bytes."""
    return len(
        json.dumps(fields, default=_json_default, separators=(",", ":")).encode("utf-8")
    )


def check_item_size(fields: Mapping[str, Any], *, what: str) -> None:
    """Raise :class:`ItemTooLargeError` if ``fields`` serializes over the limit.

    ``what`` names the entity for the error message, e.g.
    ``"Episode(episode_id=...)"``.
    """
    size = item_size_bytes(fields)
    if size > MAX_ITEM_BYTES:
        raise ItemTooLargeError(
            f"{what}: serialized size {size:,} bytes exceeds the "
            f"{MAX_ITEM_BYTES:,}-byte per-item limit enforced on all backends"
        )


def entity_fields(entity: Any) -> dict:
    """Return ``{column_name: value}`` for a SQLAlchemy model instance.

    Shared by every backend so the guard always measures the same mapping
    for the same entity.
    """
    return {
        column.name: getattr(entity, column.name, None)
        for column in entity.__table__.columns
    }
