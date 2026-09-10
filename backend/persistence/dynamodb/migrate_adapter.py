"""DynamoDB migration endpoint for ``backend.migrate_data`` (Linear: XIN-94).

Implements the :class:`backend.migrate_data.Backend` ABC over the
single-table DynamoDB layout, so the migration CLI can copy data
``simple <-> dynamodb`` and ``app <-> dynamodb`` without touching the
orchestration in :func:`backend.migrate_data.migrate`.

Table-name contract: this backend reports the *SQLAlchemy* table names in
``sorted_tables`` order (identical to ``SqlAlchemyBackend``). Internally
each name maps to one DynamoDB item ``type``; association tables
(``episode_tags``, ``playlist_episodes``) map to their link-item types.

Items this backend deliberately does NOT migrate (internal DynamoDB
state, rebuilt on write or on demand):

* ``guid_marker`` items — the per-(feed, guid) dedup markers written by
  ``EpisodeRepository.save_many``. They have no SQL counterpart. On
  ``write_rows("episodes", ...)`` each episode's marker is rebuilt with
  the same ``if episode.guid:`` rule ``save()`` uses, so dedup keeps
  working after a migration without ever copying marker rows.
* ``tag_claim`` items — transient winners of the ``get_or_create`` tag
  race. They are short-lived coordination state, not data; future
  ``get_or_create`` calls recreate them as needed.

Migration writes are plain idempotent puts keyed by (pk, sk) — the same
item key the repositories use — so re-running a migration overwrites the
identical items and never duplicates rows. Unlike ``save_many``, the
episode path here is NOT transactional across episodes: source data
already passed dedup, so per-feed guids are unique and last-writer-wins
is a safe, documented choice for the bulk path.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID

from backend.migrate_data import Backend
from backend.persistence import models, validation
from backend.persistence.dynamodb import codec, keys
from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME, ensure_table
from backend.persistence.models.base import Base


def _s(value: str) -> dict:
    return {"S": value}


_TYPE_FILTER = "#t = :t"
_TYPE_NAMES = {"#t": "type"}


def _chunks(items: List[dict], size: int) -> List[List[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------------
# BatchWriteItem retry policy for the migration bulk path.
# ---------------------------------------------------------------------------

# UnprocessedItems usually means throttling; retry with exponential
# backoff + jitter, but fail loudly after a cap — spinning forever
# against a hot table is worse than an aborted migration.
_BATCH_WRITE_MAX_ATTEMPTS = 10
_BATCH_WRITE_BASE_DELAY = 0.1  # seconds; doubles each attempt, plus jitter.


def _retry_delay(attempt: int) -> float:
    """Backoff for a 1-based attempt number: base * 2^(n-1), plus jitter."""
    delay = _BATCH_WRITE_BASE_DELAY * (2 ** (attempt - 1))
    return delay + random.uniform(0, delay)


async def _batch_write_chunk(
    client: Any,
    dynamodb_table: str,
    table_name: str,
    chunk: List[dict],
    *,
    sleep: Callable[..., Any] = asyncio.sleep,
) -> None:
    """Write one <=25-item chunk, retrying UnprocessedItems with backoff.

    ``table_name`` is the migration (SQL) table name, used only for the
    error message. Raises RuntimeError naming the table if items remain
    unprocessed after ``_BATCH_WRITE_MAX_ATTEMPTS`` attempts. ``sleep`` is
    injectable so tests can assert the backoff without waiting.
    """
    request_items = {dynamodb_table: [{"PutRequest": {"Item": i}} for i in chunk]}
    for attempt in range(1, _BATCH_WRITE_MAX_ATTEMPTS + 1):
        resp = await client.batch_write_item(RequestItems=request_items)
        request_items = resp.get("UnprocessedItems", {})
        if not request_items:
            return
        await sleep(_retry_delay(attempt))
    leftover = sum(len(reqs) for reqs in request_items.values())
    raise RuntimeError(
        f"DynamoDB batch_write_item for migration table {table_name!r} still "
        f"had {leftover} unprocessed item(s) after {_BATCH_WRITE_MAX_ATTEMPTS} "
        "attempts; aborting instead of retrying forever"
    )


async def _scan_all(client: Any, table_name: str, **kwargs: Any) -> List[dict]:
    """Scan to exhaustion (follows LastEvaluatedKey)."""
    items: List[dict] = []
    start_key: Optional[dict] = None
    while True:
        kw = dict(kwargs)
        if start_key is not None:
            kw["ExclusiveStartKey"] = start_key
        resp = await client.scan(TableName=table_name, **kw)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return items


async def _scan_count(client: Any, table_name: str, **kwargs: Any) -> int:
    """Scan to exhaustion with ``Select="COUNT"``; items are never returned.

    For callers that only need a row count (target-only-table detection in
    parity checks) -- cheaper than a full scan on large tables.
    """
    total = 0
    start_key: Optional[dict] = None
    while True:
        kw = dict(kwargs, Select="COUNT")
        if start_key is not None:
            kw["ExclusiveStartKey"] = start_key
        resp = await client.scan(TableName=table_name, **kw)
        total += resp.get("Count", 0)
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return total


# ---------------------------------------------------------------------------
# Item builders (write path): row dict -> DynamoDB-JSON item(s).
#
# Values come from SqlAlchemyBackend.read_table / this backend's read_table
# as plain column dicts (UUID objects, tz-aware datetimes). No
# apply_defaults(): migration must preserve the source's values exactly,
# including NULLs, not fill in backend defaults.
# ---------------------------------------------------------------------------


def _model_item(model_cls: Any, row: Dict[str, Any], key_attrs: dict, type_name: str) -> dict:
    return codec.model_to_item(model_cls(**row), key_attrs, type_name)


def _feed_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.Feed,
        row,
        keys.feed_keys(
            row["feed_id"],
            rss_url=row["rss_url"],
            sync_status=row["sync_status"],
            created_at=row["created_at"],
        ),
        codec.TYPE_FEED,
    )


def _episode_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.Episode,
        row,
        keys.episode_keys(
            row["episode_id"],
            feed_id=row["feed_id"],
            published_at=row.get("published_at"),
            processed=row.get("processed"),
        ),
        codec.TYPE_EPISODE,
    )


def _guid_marker_item(feed_id: UUID, guid: str, episode_id: UUID) -> dict:
    item = {name: _s(value) for name, value in keys.guid_marker_keys(feed_id, guid).items()}
    item["type"] = _s(codec.TYPE_GUID_MARKER)
    item["episode_id"] = _s(str(episode_id))
    return item


def _insight_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.Insight,
        row,
        keys.insight_keys(
            row["insight_id"],
            episode_id=row["episode_id"],
            created_at=row.get("created_at"),
        ),
        codec.TYPE_INSIGHT,
    )


def _tag_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.Tag,
        row,
        keys.tag_keys(row["tag_id"], name=row["name"], category=row.get("category")),
        codec.TYPE_TAG,
    )


def _episode_tag_link_item(row: Dict[str, Any]) -> dict:
    item = {
        name: _s(value)
        for name, value in keys.episode_tag_link_keys(row["episode_id"], row["tag_id"]).items()
    }
    item["type"] = _s(codec.TYPE_EPISODE_TAG_LINK)
    return item


def _user_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.User,
        row,
        keys.user_keys(row["user_id"], email=row["email"]),
        codec.TYPE_USER,
    )


def _playlist_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.CuratedPlaylist,
        row,
        keys.playlist_keys(
            row["playlist_id"],
            user_id=row["user_id"],
            created_at=row.get("created_at"),
        ),
        codec.TYPE_PLAYLIST,
    )


def _playlist_episode_link_item(row: Dict[str, Any]) -> dict:
    item = {
        name: _s(value)
        for name, value in keys.playlist_episode_link_keys(row["playlist_id"], row["episode_id"]).items()
    }
    item["type"] = _s(codec.TYPE_PLAYLIST_EPISODE_LINK)
    item["position"] = {"N": str(row.get("position") or 0)}
    return item


def _progress_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.UserEpisodeProgress,
        row,
        keys.progress_keys(row["user_id"], row["episode_id"]),
        codec.TYPE_PROGRESS,
    )


def _task_log_item(row: Dict[str, Any]) -> dict:
    return _model_item(
        models.TaskLog,
        row,
        keys.task_log_keys(
            row["task_log_id"],
            task_type=row.get("task_type"),
            status=row.get("status"),
            created_at=row.get("created_at"),
        ),
        codec.TYPE_TASK_LOG,
    )


# table name -> item builder (one item per row, except episodes below).
_ITEM_BUILDERS: Dict[str, Callable[[Dict[str, Any]], dict]] = {
    "feeds": _feed_item,
    "episodes": _episode_item,
    "insights": _insight_item,
    "tags": _tag_item,
    "episode_tags": _episode_tag_link_item,
    "users": _user_item,
    "curated_playlists": _playlist_item,
    "playlist_episodes": _playlist_episode_link_item,
    "user_episode_progress": _progress_item,
    "task_logs": _task_log_item,
}


def _row_items(table_name: str, row: Dict[str, Any]) -> List[dict]:
    """All DynamoDB items one migrated row expands to.

    Episodes additionally rebuild their guid-dedup marker (same
    ``if guid:`` rule as ``EpisodeRepository.save``); markers are never
    migrated as rows themselves.
    """
    items = [_ITEM_BUILDERS[table_name](row)]
    if table_name == "episodes":
        guid = row.get("guid")
        if guid:
            items.append(_guid_marker_item(row["feed_id"], guid, row["episode_id"]))
    return items


# ---------------------------------------------------------------------------
# Row readers (read path): DynamoDB-JSON item -> plain column dict.
# ---------------------------------------------------------------------------


def _model_row(model_cls: Any, item: dict) -> Dict[str, Any]:
    model = codec.item_to_model(model_cls, item)
    return {c.name: getattr(model, c.name) for c in model_cls.__table__.columns}


def _episode_tag_link_row(item: dict) -> Dict[str, Any]:
    # Link items carry no column attributes — parse the UUIDs from the keys.
    return {
        "episode_id": keys.uuid_from_str(item["pk"]["S"].split("EP#", 1)[1]),
        "tag_id": keys.uuid_from_str(item["sk"]["S"].split("TAGLINK#", 1)[1]),
    }


def _playlist_episode_link_row(item: dict) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "playlist_id": keys.uuid_from_str(item["pk"]["S"].split("PL#", 1)[1]),
        "episode_id": keys.uuid_from_str(item["sk"]["S"].split("PLEP#", 1)[1]),
    }
    raw_position = item.get("position")
    row["position"] = int(codec.deserialize_plain(raw_position)) if raw_position else 0
    return row


# table name -> (model class, item type, row reader).
_READ_SPECS: Dict[str, tuple] = {
    "feeds": (models.Feed, codec.TYPE_FEED, _model_row),
    "episodes": (models.Episode, codec.TYPE_EPISODE, _model_row),
    "insights": (models.Insight, codec.TYPE_INSIGHT, _model_row),
    "tags": (models.Tag, codec.TYPE_TAG, _model_row),
    "episode_tags": (models.EpisodeTag, codec.TYPE_EPISODE_TAG_LINK, _episode_tag_link_row),
    "users": (models.User, codec.TYPE_USER, _model_row),
    "curated_playlists": (models.CuratedPlaylist, codec.TYPE_PLAYLIST, _model_row),
    "playlist_episodes": (
        models.PlaylistEpisode,
        codec.TYPE_PLAYLIST_EPISODE_LINK,
        _playlist_episode_link_row,
    ),
    "user_episode_progress": (models.UserEpisodeProgress, codec.TYPE_PROGRESS, _model_row),
    "task_logs": (models.TaskLog, codec.TYPE_TASK_LOG, _model_row),
}


# SQL table names in FK-safe parents-first order (identical to
# SqlAlchemyBackend.table_names). Internal item types (guid markers, tag
# claims) have no SQL table and are never listed, so they are never read
# as migration rows.
_MIGRATION_TABLES: List[str] = [t.name for t in Base.metadata.sorted_tables]


# Fail fast at import if a model is added without migration coverage: the
# write builders and read specs must cover exactly the SQL table set, or
# the next added table breaks migration with a cryptic KeyError/ValueError
# deep in a run instead of a clear import-time error.
_coverage = {
    "missing write builders": sorted(set(_MIGRATION_TABLES) - set(_ITEM_BUILDERS)),
    "missing read specs": sorted(set(_MIGRATION_TABLES) - set(_READ_SPECS)),
    "unknown write builders": sorted(set(_ITEM_BUILDERS) - set(_MIGRATION_TABLES)),
    "unknown read specs": sorted(set(_READ_SPECS) - set(_MIGRATION_TABLES)),
}
_mismatches = {k: v for k, v in _coverage.items() if v}
if _mismatches:
    details = "; ".join(f"{k}={v}" for k, v in _mismatches.items())
    raise AssertionError(
        f"DynamoDB migration coverage mismatch: {details}. "
        "Every table in Base.metadata needs both an _ITEM_BUILDERS entry "
        "and a _READ_SPECS entry."
    )
del _coverage, _mismatches


class DynamoDBBackend(Backend):
    """A :class:`backend.migrate_data.Backend` over the single-table layout."""

    name = "dynamodb"

    def __init__(
        self,
        client: Any = None,
        *,
        table_name: str = DEFAULT_TABLE_NAME,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ) -> None:
        # The owned client is created and entered lazily in _ensure_client():
        # aioboto3's session.client() returns an *un-entered*
        # ClientCreatorContext and every API call on it fails until
        # __aenter__ runs (the XIN-90 bug XIN-93 fixed in DynamoDBStore).
        # Injected clients (tests) are used as-is and never closed here.
        self._client = client
        self._owns_client = client is None
        self._entered = False
        self._table_name = table_name
        self._region_name = region_name
        self._endpoint_url = endpoint_url

    async def _ensure_client(self) -> Any:
        """Return an entered client, entering the owned one on first use."""
        if self._client is None and self._owns_client:
            from backend.persistence.dynamodb.client import create_client

            self._client = await create_client(
                region_name=self._region_name, endpoint_url=self._endpoint_url
            ).__aenter__()
            self._entered = True
        return self._client

    @property
    def identity(self) -> str:
        # endpoint_url is part of the identity: two backends with the same
        # region/table but different endpoints (moto-local vs real AWS)
        # are different stores, and the same-database guard must not
        # conflate them (XIN-132).
        return (
            f"dynamodb:{self._region_name}:{self._table_name}"
            f":{self._endpoint_url or ''}"
        )

    @property
    def table_names(self) -> List[str]:
        return list(_MIGRATION_TABLES)

    async def init(self) -> None:
        client = await self._ensure_client()
        await ensure_table(client, table_name=self._table_name)

    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        client = await self._ensure_client()
        model_cls, type_name, row_reader = _READ_SPECS[table_name]
        items = await _scan_all(
            client,
            self._table_name,
            FilterExpression=_TYPE_FILTER,
            ExpressionAttributeNames=_TYPE_NAMES,
            ExpressionAttributeValues={":t": _s(type_name)},
        )
        if row_reader is _model_row:
            return [row_reader(model_cls, item) for item in items]
        return [row_reader(item) for item in items]

    async def count_rows(self, table_name: str) -> int:
        # Count-only scan: no items are returned, so target-only-table
        # detection in parity checks never materializes a full table.
        client = await self._ensure_client()
        _model_cls, type_name, _row_reader = _READ_SPECS[table_name]
        return await _scan_count(
            client,
            self._table_name,
            FilterExpression=_TYPE_FILTER,
            ExpressionAttributeNames=_TYPE_NAMES,
            ExpressionAttributeValues={":t": _s(type_name)},
        )

    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        if table_name not in _ITEM_BUILDERS:
            raise ValueError(f"Unknown table for DynamoDB migration: {table_name!r}")
        client = await self._ensure_client()
        items: List[dict] = []
        for row in rows:
            # Same shared 400 KiB guard as the repository write paths: a row
            # the source accepted is re-checked here so an oversized row
            # fails the migration loudly instead of tripping DynamoDB's own
            # (less descriptive) service-side limit mid-backfill.
            validation.check_item_size(row, what=f"migration row {table_name}")
            items.extend(_row_items(table_name, row))
        # 25-item BatchWriteItem chunks; unprocessed items are retried with
        # exponential backoff + jitter, failing loudly after a cap
        # (see _batch_write_chunk).
        for chunk in _chunks(items, 25):
            await _batch_write_chunk(client, self._table_name, table_name, chunk)
        return len(rows)

    async def close(self) -> None:
        # Idempotent and safe even if the client was never entered (e.g. a
        # dry run with dynamodb as source never calls init()).
        if self._owns_client and self._entered and self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._entered = False
            self._client = None
