"""Single-table key builders for the DynamoDB backend (Linear: XIN-89).

This module is the single choke point for all DynamoDB key encoding in
tunedin.  Every key attribute (``pk``/``sk`` and the ``gsi{1,2,3}pk/sk``
attributes) for every entity type is built here, so encoding fixes apply
everywhere at once.

Table layout (see ``table.py`` for the physical spec):

========================  ========================  =====================================
Entity                    pk                        sk
========================  ========================  =====================================
Feed                      ``FEED#<feed_id>``        ``META``
Episode                   ``FEED#<feed_id>``        ``EP#<published_ts>#<episode_id>``
Guid-dedup marker         ``FEED#<feed_id>``        ``GUID#<guid>``
Insight                   ``EP#<episode_id>``       ``INSIGHT#<created_ts>#<insight_id>``
Episode-tag link          ``EP#<episode_id>``       ``TAGLINK#<tag_id>``
Tag                       ``TAG#<tag_id>``          ``META``
User                      ``USER#<user_id>``        ``META``
Playlist                  ``USER#<user_id>``        ``PL#<created_ts>#<playlist_id>``
Playlist-episode link     ``PL#<playlist_id>``      ``PLEP#<episode_id>``
Slug claim                ``SLUG#<slug>``           ``META``
User episode progress     ``USER#<user_id>``        ``PROG#<episode_id>``
Task log                  ``TASK#<task_log_id>``    ``META``
========================  ========================  =====================================

Every item also carries a ``type`` attribute (entity type string) used by
scans/filters.  The ``type`` attribute is added by
:func:`codec.model_to_item` at write time — the key builders here emit
only key attributes.

GSIs:

* ``gsi1`` (natural-key lookups, point queries): feed by rss_url
  (``URL#<sha256(rss_url)>``), episode by id, tag by name+category, user by
  hashed normalized email, playlist by id.  The sort key is the constant
  ``META`` — uniqueness lives in the partition key.
* ``gsi2`` (status scans): ``STATUS#<sync_status>`` / ``<created_ts>`` for
  feeds; ``TASKTYPE#<task_type>#<status>`` / ``<created_ts>`` for task logs.
* ``gsi3`` (sparse unprocessed-episode index): ``UNPROCESSED`` /
  ``EP#<published_ts>#<feed_id>#<episode_id>``, present only on unprocessed
  episodes.

Missing timestamps in sort keys use the ``0001-…`` sentinel (see
:func:`iso_timestamp`): every timestamp-bearing sort key in this schema is
read newest-first (``ScanIndexForward=False``), so the smallest sentinel
sorts *last*, i.e. nulls-last, matching the SQL ``… DESC NULLS LAST``
ordering contracts in the repository ABCs.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

# Sentinel for a missing timestamp in a sort key.  Sorts before every real
# ISO-8601 timestamp lexicographically, so under a descending scan (newest
# first) missing values come last — matching the SQL "DESC NULLS LAST"
# contracts.
MISSING_TS_MIN = "0001-01-01T00:00:00+00:00"

# Constant sort-key value for singleton items (mirrors the main table's META).
META = "META"


def iso_timestamp(dt: Optional[datetime], *, missing: str = MISSING_TS_MIN) -> str:
    """Encode a datetime as an ISO-8601 string for key use.

    Naive datetimes are normalized to UTC *first*: a naive
    ``2026-09-09T14:41:00`` becomes ``2026-09-09T14:41:00+00:00`` and can
    never sort after its aware equivalent.  Aware datetimes with a non-UTC
    offset are converted to UTC (e.g. ``16:41:00+02:00`` →
    ``14:41:00+00:00``) so the same instant always encodes to the same
    string — this is the single choke point for all timestamp keys, and
    lexicographic ordering of keys is only correct if every timestamp
    shares one offset.

    ``None`` encodes as ``missing`` (default :data:`MISSING_TS_MIN`).
    """
    if dt is None:
        return missing
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def sanitize_str(value: Optional[str]) -> Optional[str]:
    """Map ``""`` to ``None`` (attribute omitted on write).

    DynamoDB rejects empty strings; on read, an omitted attribute maps back
    to ``None``.  Callers must therefore treat ``""`` and ``None`` as
    equivalent for DynamoDB-persisted string fields.
    """
    if value == "":
        return None
    return value


def uuid_str(uid: UUID) -> str:
    """Canonical string form of a UUID for key use."""
    return str(uid)


def uuid_from_str(value: str) -> UUID:
    """Parse a key-form UUID string back to :class:`UUID`."""
    return UUID(value)


def sha256_hex(value: str) -> str:
    """Hex SHA-256 of ``value`` (UTF-8), for hashing long natural keys."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def feed_keys(
    feed_id: UUID, *, rss_url: str, sync_status: str, created_at: datetime
) -> dict:
    """Key attributes for a Feed item."""
    return {
        "pk": f"FEED#{uuid_str(feed_id)}",
        "sk": META,
        "gsi1pk": f"URL#{sha256_hex(rss_url)}",
        "gsi1sk": META,
        "gsi2pk": f"STATUS#{sync_status}",
        "gsi2sk": iso_timestamp(created_at),
    }


def episode_keys(
    episode_id: UUID,
    *,
    feed_id: UUID,
    published_at: Optional[datetime],
    processed: bool,
) -> dict:
    """Key attributes for an Episode item.

    The ``gsi3`` (sparse unprocessed-episode) attributes are present only
    when the episode is unprocessed — the index is sparse by construction.
    """
    keys = {
        "pk": f"FEED#{uuid_str(feed_id)}",
        "sk": f"EP#{iso_timestamp(published_at)}#{uuid_str(episode_id)}",
        "gsi1pk": f"EP#{uuid_str(episode_id)}",
        "gsi1sk": META,
    }
    if not processed:
        keys["gsi3pk"] = "UNPROCESSED"
        keys["gsi3sk"] = (
            f"EP#{iso_timestamp(published_at)}#{uuid_str(feed_id)}"
            f"#{uuid_str(episode_id)}"
        )
    return keys


def guid_marker_keys(feed_id: UUID, guid: str) -> dict:
    """Key attributes for an episode guid-dedup marker item.

    Written with an ``attribute_not_exists`` condition inside
    ``TransactWriteItems`` (see the plan §2.4); a duplicate guid for the
    same feed therefore fails the transaction instead of double-inserting.
    """
    return {
        "pk": f"FEED#{uuid_str(feed_id)}",
        "sk": f"GUID#{guid}",
    }


def insight_keys(
    insight_id: UUID, *, episode_id: UUID, created_at: datetime
) -> dict:
    """Key attributes for an Insight item."""
    return {
        "pk": f"EP#{uuid_str(episode_id)}",
        "sk": f"INSIGHT#{iso_timestamp(created_at)}#{uuid_str(insight_id)}",
    }


def episode_tag_link_keys(episode_id: UUID, tag_id: UUID) -> dict:
    """Key attributes for an episode↔tag link item."""
    return {
        "pk": f"EP#{uuid_str(episode_id)}",
        "sk": f"TAGLINK#{uuid_str(tag_id)}",
    }


def tag_keys(tag_id: UUID, *, name: str, category: Optional[str]) -> dict:
    """Key attributes for a Tag item.

    The natural key is (name, category), hashed because names are
    unbounded in length; lookup normalizes case the same way.
    """
    return {
        "pk": f"TAG#{uuid_str(tag_id)}",
        "sk": META,
        "gsi1pk": (
            f"TAGNAME#{sha256_hex(name.lower())}"
            f"#{sha256_hex((category or '').lower())}"
        ),
        "gsi1sk": META,
    }


def tag_natural_key_hash(name: str, category: Optional[str]) -> str:
    """Hash of the exact-case (name, category) natural key (Linear: XIN-124).

    Used for tag-claim item keys (write-time uniqueness in
    ``get_or_create``), which must be case-SENSITIVE to match the SQL
    unique constraint on (name, category). Deliberately NOT the same
    normalization as :func:`tag_keys`' lowercased gsi1 lookup key: the
    lookup key is case-insensitive by design (candidates are re-checked
    for the exact pair in Python), while the claim key must distinguish
    ``"Foo"`` from ``"FOO"``. Keep both derivations here so the two
    normalizations cannot drift apart.
    """
    return (
        f"TAGNAME#{sha256_hex(name)}"
        f"#{sha256_hex(category or '')}"
    )


def normalize_email(email: str) -> str:
    """Normalize an email for key use (lowercase, stripped)."""
    return email.strip().lower()


def user_keys(user_id: UUID, *, email: str) -> dict:
    """Key attributes for a User item (email lookup via hashed gsi1pk)."""
    return {
        "pk": f"USER#{uuid_str(user_id)}",
        "sk": META,
        "gsi1pk": f"EMAIL#{sha256_hex(normalize_email(email))}",
        "gsi1sk": META,
    }


def playlist_keys(
    playlist_id: UUID, *, user_id: UUID, created_at: datetime
) -> dict:
    """Key attributes for a playlist item."""
    return {
        "pk": f"USER#{uuid_str(user_id)}",
        "sk": f"PL#{iso_timestamp(created_at)}#{uuid_str(playlist_id)}",
        "gsi1pk": f"PL#{uuid_str(playlist_id)}",
        "gsi1sk": META,
    }


def playlist_episode_link_keys(playlist_id: UUID, episode_id: UUID) -> dict:
    """Key attributes for a playlist↔episode link item."""
    return {
        "pk": f"PL#{uuid_str(playlist_id)}",
        "sk": f"PLEP#{uuid_str(episode_id)}",
    }


def slug_claim_keys(slug: str) -> dict:
    """Key attributes for a playlist slug-claim item (Linear: XIN-97).

    One claim item per assigned slug; written with a conditional write
    (``attribute_not_exists(pk)`` or owned by the same playlist), so slug
    uniqueness is enforced at write time without a GSI. The claim carries
    the owning ``playlist_id`` so ``get_by_slug`` can resolve it.
    """
    return {
        "pk": f"SLUG#{slug}",
        "sk": META,
    }


def rss_url_claim_keys(rss_url: str) -> dict:
    """Key attributes for a feed rss_url-claim item (Linear: XIN-124).

    The write-time uniqueness lock for ``Feed.rss_url``, mirroring the
    SQL ``unique=True`` constraint. The URL is hashed (it can be up to
    1024 chars) and the claim carries the owning ``feed_id`` so
    re-saves of the same feed pass the claim's condition expression.
    """
    return {
        "pk": f"RSSURLCLAIM#{sha256_hex(rss_url)}",
        "sk": META,
    }


def email_claim_keys(email: str) -> dict:
    """Key attributes for a user email-claim item (Linear: XIN-124).

    The write-time uniqueness lock for ``User.email``, mirroring the
    SQL ``unique=True`` constraint. The hash is over the exact-case
    email (matching SQL's case-sensitive uniqueness); lookup still goes
    through the normalized gsi1 key with an exact re-check in Python.
    The claim carries the owning ``user_id`` so re-saves of the same
    user pass the claim's condition expression.
    """
    return {
        "pk": f"EMAILCLAIM#{sha256_hex(email)}",
        "sk": META,
    }


def progress_keys(user_id: UUID, episode_id: UUID) -> dict:
    """Key attributes for a user-episode-progress item."""
    return {
        "pk": f"USER#{uuid_str(user_id)}",
        "sk": f"PROG#{uuid_str(episode_id)}",
    }


def task_log_keys(
    task_log_id: UUID, *, task_type: str, status: str, created_at: datetime
) -> dict:
    """Key attributes for a TaskLog item (gsi2 by type/status/ts)."""
    return {
        "pk": f"TASK#{uuid_str(task_log_id)}",
        "sk": META,
        "gsi2pk": f"TASKTYPE#{task_type}#{status}",
        "gsi2sk": iso_timestamp(created_at),
    }
