"""Model <-> DynamoDB item codec for the DynamoDB backend (Linear: XIN-90).

The repository ABCs traffic in the SQLAlchemy model classes as plain
attribute bags. This module translates those bags to/from DynamoDB-JSON
items:

* ``None`` attributes are omitted (DynamoDB has no NULL convention here;
  read-back maps a missing attribute to ``None``).
* ``""`` is omitted on write (DynamoDB rejects empty strings) and reads
  back as ``None`` — the same convention the conformance suite accepts.
* naive datetimes are normalized to UTC via :func:`keys.iso_timestamp`
  before storage, so every persisted timestamp is tz-aware ISO-8601.
* UUIDs are stored as canonical strings, ints as Numbers, bools as BOOL.

Column types are discovered from the SQLAlchemy table metadata, so the
codec stays correct if model columns are added later.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Type
from uuid import UUID, uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from sqlalchemy import Boolean, DateTime, Float, Integer, Uuid

from backend.persistence import models
from backend.persistence import validation
from backend.persistence.dynamodb import keys

_SER = TypeSerializer()
_DESER = TypeDeserializer()

# Entity ``type`` attribute values stored on every item. Used by scan
# filters (counts, list_all) to pick one entity kind out of the
# single table.
TYPE_FEED = "feed"
TYPE_EPISODE = "episode"
TYPE_GUID_MARKER = "guid_marker"
TYPE_INSIGHT = "insight"
TYPE_EPISODE_TAG_LINK = "episode_tag_link"
TYPE_TAG = "tag"
TYPE_TAG_CLAIM = "tag_claim"
TYPE_USER = "user"
TYPE_PLAYLIST = "playlist"
TYPE_PLAYLIST_EPISODE_LINK = "playlist_episode_link"
TYPE_SLUG_CLAIM = "slug_claim"
TYPE_RSS_URL_CLAIM = "rss_url_claim"
TYPE_EMAIL_CLAIM = "email_claim"
TYPE_PROGRESS = "progress"
TYPE_TASK_LOG = "task_log"
TYPE_FEED_TEMPLATE = "feed_template"
TYPE_DRIFT_DECISION = "drift_decision"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def apply_defaults(entity: Any) -> None:
    """Populate Python-side defaults on a transient model instance.

    Mirrors the SQLAlchemy column ``default=`` values that fire on flush
    in the SQL backend, so ``save()`` returns the entity with its primary
    key and server-side defaults populated on both backends.
    """
    if isinstance(entity, models.Feed):
        if entity.feed_id is None:
            entity.feed_id = uuid4()
        if entity.sync_status is None:
            entity.sync_status = "pending"
        if entity.error_count is None:
            entity.error_count = 0
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.Episode):
        if entity.episode_id is None:
            entity.episode_id = uuid4()
        if entity.processed is None:
            entity.processed = False
        if entity.episode_type is None:
            entity.episode_type = "full"
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.Insight):
        if entity.insight_id is None:
            entity.insight_id = uuid4()
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.Tag):
        if entity.tag_id is None:
            entity.tag_id = uuid4()
    elif isinstance(entity, models.User):
        if entity.user_id is None:
            entity.user_id = uuid4()
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.CuratedPlaylist):
        if entity.playlist_id is None:
            entity.playlist_id = uuid4()
        if entity.visibility is None:
            entity.visibility = "unlisted"
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.TaskLog):
        if entity.task_log_id is None:
            entity.task_log_id = uuid4()
        if entity.status is None:
            entity.status = "pending"
        if entity.created_at is None:
            entity.created_at = _now()
    elif isinstance(entity, models.UserEpisodeProgress):
        if entity.position_seconds is None:
            entity.position_seconds = 0
        if entity.completed is None:
            entity.completed = False
        if entity.last_played_at is None:
            entity.last_played_at = _now()
    elif isinstance(entity, models.PlaylistEpisode):
        if entity.position is None:
            entity.position = 0
        if entity.added_at is None:
            entity.added_at = _now()


def _serialize_value(value: Any) -> Any:
    """Serialize one Python value to DynamoDB-JSON (or ``None`` to omit)."""
    if value is None:
        return None
    if isinstance(value, str):
        value = keys.sanitize_str(value)
        if value is None:
            return None
    elif isinstance(value, datetime):
        value = keys.iso_timestamp(value)
    elif isinstance(value, UUID):
        value = str(value)
    elif isinstance(value, float):
        # DynamoDB has no float type; store as Decimal via str() to avoid
        # binary float artifacts (Decimal(0.1) != Decimal("0.1")).
        value = Decimal(str(value))
    return _SER.serialize(value)


def model_to_item(entity: Any, key_attrs: dict, type_name: str) -> dict:
    """Serialize a model instance to a DynamoDB-JSON item.

    ``key_attrs`` are the plain-string key attributes from
    :mod:`backend.persistence.dynamodb.keys` (already in DynamoDB-JSON
    ``{"S": ...}`` form is NOT required — this function wraps them).

    The shared 400 KiB item-size guard
    (:mod:`backend.persistence.validation`) runs on every item built here,
    so DynamoDB raises the same :class:`ItemTooLargeError` as the SQL
    backends instead of failing later at the service.
    """
    item = {name: {"S": value} for name, value in key_attrs.items()}
    item["type"] = {"S": type_name}
    for column in entity.__table__.columns:
        value = getattr(entity, column.name, None)
        serialized = _serialize_value(value)
        if serialized is not None:
            item[column.name] = serialized
    validation.check_item_size(item, what=f"{type_name} item")
    return item


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        return datetime.fromisoformat(text)
    return value


def item_to_model(model_cls: Type[Any], item: dict) -> Any:
    """Deserialize a DynamoDB-JSON item to a model instance (attribute bag)."""
    kwargs: dict[str, Any] = {}
    columns = {c.name: c for c in model_cls.__table__.columns}
    for name, column in columns.items():
        raw = item.get(name)
        if raw is None:
            continue
        value = _DESER.deserialize(raw)
        if isinstance(column.type, DateTime):
            value = _parse_datetime(value)
        elif isinstance(column.type, Uuid):
            value = UUID(str(value))
        elif isinstance(column.type, Integer) and value is not None:
            value = int(value)
        elif isinstance(column.type, Float) and value is not None:
            value = float(value)
        elif isinstance(column.type, Boolean) and value is not None:
            value = bool(value)
        kwargs[name] = value
    return model_cls(**kwargs)


def deserialize_plain(value: Any) -> Any:
    """Deserialize a DynamoDB-JSON value to a plain Python value."""
    return _DESER.deserialize(value)
