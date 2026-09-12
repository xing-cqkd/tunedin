"""DynamoDB implementations of the Store/repository protocols (Linear: XIN-90).

Single-table design; key builders live in :mod:`keys`, table provisioning
in :mod:`table`, model<->item translation in :mod:`codec`. Every write here
is a full-item ``PutItem`` (never a partial ``UpdateItem``), so GSI key
attributes can never go stale when an entity's indexed fields change.

Write-through semantics: ``save()`` persists immediately; ``commit()`` /
``rollback()`` on the store are no-ops (see :class:`DynamoDBStore`).

Atomicity contracts: SQL commits multi-entity mutations atomically;
DynamoDB write-through does not. Each multi-entity operation below states
in its docstring whether it uses ``TransactWriteItems`` or
ordered-writes-plus-idempotent-retry, and why.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Iterable, Optional, TypeVar
from uuid import UUID

from backend.persistence import models
from backend.persistence.dynamodb import codec, keys
from backend.persistence.repositories import (
    VISIBILITY_UNLISTED,
    DuplicateFeedError,
    EpisodeRepository,
    FeedRepository,
    InsightRepository,
    MissingParentError,
    PlaylistEpisodeEntry,
    PlaylistRepository,
    ProgressRepository,
    SlugConflictError,
    Store,
    TagRepository,
    TaskLogRepository,
    UserRepository,
    _now_utc,
    generate_slug,
    generate_token,
    validate_visibility,
)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Low-level helpers (DynamoDB-JSON plumbing, paginated queries)
# ---------------------------------------------------------------------------


def _s(value: str) -> dict:
    return {"S": value}


def _key(pk: str, sk: str) -> dict:
    return {"pk": _s(pk), "sk": _s(sk)}


def _chunks(items: list[T], size: int) -> Iterable[list[T]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


# DynamoDB allows at most 100 items in one TransactWriteItems call.
_TRANSACT_ITEM_LIMIT = 100


def _save_many_op_chunks(
    candidates: list[tuple[int, models.Episode]],
    old_guids: dict[tuple[str, str], str],
    old_sks: dict[tuple[str, str], str],
) -> Iterable[list[tuple[int, models.Episode]]]:
    """Split candidates into chunks bounded by the transact-item limit.

    A candidate costs one transact item for the episode put, plus one
    for the guid-marker put (guid episodes only), plus one more for the
    stale-marker delete when the episode's guid changed, plus one more
    for the stale-item delete when the episode's sk moved
    (``published_at`` change). Fixed
    episode-count chunking is unsafe here: 50 guid-changing episodes
    would need 150 items. Greedy packing keeps every chunk at or under
    the 100-item limit; the retry loop inside ``save_many`` only ever
    shrinks ``remaining``, so a retried chunk stays within budget too.
    """
    chunk: list[tuple[int, models.Episode]] = []
    cost = 0
    for candidate in candidates:
        _, episode = candidate
        item_cost = 1
        if episode.guid:
            item_cost += 1
            prev = old_guids.get(
                (str(episode.feed_id), str(episode.episode_id))
            )
            if prev is not None and prev != episode.guid:
                item_cost += 1
        prev_sk = old_sks.get((str(episode.feed_id), str(episode.episode_id)))
        if prev_sk is not None and prev_sk != _episode_key(episode)["sk"]["S"]:
            item_cost += 1
        if chunk and cost + item_cost > _TRANSACT_ITEM_LIMIT:
            yield chunk
            chunk = []
            cost = 0
        chunk.append(candidate)
        cost += item_cost
    if chunk:
        yield chunk


async def _query_all(client: Any, table_name: str, **kwargs: Any) -> list[dict]:
    """Run a Query to exhaustion, returning every matching item."""
    items: list[dict] = []
    start_key: Optional[dict] = None
    while True:
        kw = dict(kwargs)
        if start_key is not None:
            kw["ExclusiveStartKey"] = start_key
        resp = await client.query(TableName=table_name, **kw)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return items


async def _query_until(
    client: Any, table_name: str, n: int, **kwargs: Any
) -> list[dict]:
    """Paginate a Query until ``n`` items have accumulated or it is exhausted.

    DynamoDB applies ``Limit`` BEFORE ``FilterExpression``, so a filtered
    query must loop on ``LastEvaluatedKey`` — a single page can legally
    return zero matches even when more exist. This helper implements the
    pagination contract the repository ABCs require (notably
    ``list_unprocessed``).
    """
    items: list[dict] = []
    start_key: Optional[dict] = None
    while len(items) < n:
        kw = dict(kwargs)
        if start_key is not None:
            kw["ExclusiveStartKey"] = start_key
        resp = await client.query(TableName=table_name, **kw)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items[:n]


async def _scan_all(client: Any, table_name: str, **kwargs: Any) -> list[dict]:
    """Run a Scan to exhaustion, returning every matching item."""
    items: list[dict] = []
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


async def _count(
    client: Any, table_name: str, *, index: Optional[str] = None, **kwargs: Any
) -> int:
    """Count items matching a key condition (and optional filter)."""
    total = 0
    start_key: Optional[dict] = None
    while True:
        kw = dict(kwargs)
        if index is not None:
            kw["IndexName"] = index
        if start_key is not None:
            kw["ExclusiveStartKey"] = start_key
        if index is not None:
            resp = await client.query(TableName=table_name, Select="COUNT", **kw)
        else:
            resp = await client.scan(TableName=table_name, Select="COUNT", **kw)
        total += resp.get("Count", 0)
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return total


_TYPE_FILTER = "#t = :t"
_TYPE_NAMES = {"#t": "type"}


def _type_values(type_name: str) -> dict:
    return {":t": _s(type_name)}


async def _batch_get(
    client: Any, table_name: str, key_dicts: list[dict]
) -> list[dict]:
    """Fetch items by exact main-table keys (100 per BatchGetItem call)."""
    out: list[dict] = []
    for chunk in _chunks(key_dicts, 100):
        pending: list[dict] = list(chunk)
        while pending:
            resp = await client.batch_get_item(
                RequestItems={table_name: {"Keys": pending}}
            )
            out.extend(resp.get("Responses", {}).get(table_name, []))
            pending = (
                resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            )
    return out


def _sort_by_created(models_list: list[T], *, reverse: bool = False) -> list[T]:
    """Sort entities by ``created_at`` (all aware UTC after the codec)."""
    return sorted(models_list, key=lambda m: m.created_at, reverse=reverse)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Feed repository
# ---------------------------------------------------------------------------


def _feed_item(feed: models.Feed) -> dict:
    codec.apply_defaults(feed)
    key_attrs = keys.feed_keys(
        feed.feed_id,
        rss_url=feed.rss_url,
        sync_status=feed.sync_status,
        created_at=feed.created_at,
    )
    return codec.model_to_item(feed, key_attrs, codec.TYPE_FEED)


def _rss_url_claim_item(feed: models.Feed) -> dict:
    """Build the rss_url-claim item for a feed (Linear: XIN-124).

    The claim's key (``RSSURLCLAIM#<sha256(rss_url)>`` / ``META``) is the
    write-time uniqueness lock for ``Feed.rss_url`` — the DynamoDB
    analogue of the SQL ``unique=True`` constraint. The ``feed_id``
    attribute lets re-saves of the same feed pass the claim's condition
    expression. Claim items accumulate (never deleted except when a
    feed's own rss_url changes); see the tag-claim tradeoff comment in
    :meth:`_TagRepository._get_or_create`.
    """
    key_attrs = keys.rss_url_claim_keys(feed.rss_url)
    item = {name: _s(value) for name, value in key_attrs.items()}
    item["type"] = _s(codec.TYPE_RSS_URL_CLAIM)
    item["feed_id"] = _s(str(feed.feed_id))
    return item


class _FeedRepository(FeedRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def get_by_id(self, feed_id: UUID) -> Optional[models.Feed]:
        resp = await self._c.get_item(
            TableName=self._t, Key=_key(f"FEED#{feed_id}", keys.META)
        )
        item = resp.get("Item")
        return codec.item_to_model(models.Feed, item) if item else None

    async def get_by_rss_url(self, rss_url: str) -> Optional[models.Feed]:
        items = await _query_all(
            self._c,
            self._t,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
            ExpressionAttributeValues={
                ":p": _s(f"URL#{keys.sha256_hex(rss_url)}"),
                ":s": _s(keys.META),
            },
        )
        return codec.item_to_model(models.Feed, items[0]) if items else None

    async def list_by_statuses(
        self, statuses: list[str], *, limit: Optional[int] = None
    ) -> list[models.Feed]:
        # One GSI partition per status; DynamoDB cannot merge-sort across
        # partitions, so the merge + created_at ordering happens in code.
        if not statuses:
            return []
        items: list[dict] = []
        for status in statuses:
            items.extend(
                await _query_all(
                    self._c,
                    self._t,
                    IndexName="gsi2",
                    KeyConditionExpression="gsi2pk = :p",
                    ExpressionAttributeValues={":p": _s(f"STATUS#{status}")},
                    ScanIndexForward=True,
                )
            )
        feeds = [codec.item_to_model(models.Feed, i) for i in items]
        feeds = _sort_by_created(feeds)
        if limit is not None:
            feeds = feeds[:limit]
        return feeds

    async def list_error_due_retry(
        self, cutoff: datetime, max_attempts: int
    ) -> list[models.Feed]:
        # Mirrors the SQL coarse pre-filter: error status, error_count below
        # the cap, and last_fetched_at at or before the cutoff — or NULL
        # (XIN-128: null-last_fetched_at error rows must not be stranded).
        # The GSI query is already created_at-ascending and the filter
        # preserves relative order, so no re-sort is needed.
        items = await _query_all(
            self._c,
            self._t,
            IndexName="gsi2",
            KeyConditionExpression="gsi2pk = :p",
            FilterExpression=(
                "error_count < :m AND (attribute_not_exists(last_fetched_at) "
                "OR last_fetched_at <= :c)"
            ),
            ExpressionAttributeValues={
                ":p": _s("STATUS#error"),
                ":m": {"N": str(max_attempts)},
                ":c": _s(keys.iso_timestamp(cutoff)),
            },
            ScanIndexForward=True,
        )
        return [codec.item_to_model(models.Feed, i) for i in items]

    async def save(self, feed: models.Feed) -> models.Feed:
        """Upsert by primary key, enforcing rss_url uniqueness at write time.

        Atomicity: the feed item and its rss_url-claim item are written in
        one ``TransactWriteItems`` call, with
        ``attribute_not_exists(pk) OR feed_id = :fid`` on the claim. An
        rss_url taken by a *different* feed cancels the whole write and
        raises :class:`DuplicateFeedError` (SQL raises ``IntegrityError``
        on the unique constraint instead — the error types differ but both
        reject the duplicate). Re-saving the same feed (idempotent
        re-write) passes the condition. When the rss_url itself changed,
        the stale claim is deleted in the SAME transaction, so a crash
        can never leave the old URL claimed forever (Linear: XIN-123).

        Consistency: the old-row lookup goes through :meth:`get_by_id`,
        a main-table ``GetItem`` (strongly consistent) — no staleness
        window on the stale-claim cleanup, unlike the slug-claim path
        which reads through gsi1.
        """
        is_new = feed.feed_id is None
        codec.apply_defaults(feed)
        old = None if is_new else await self.get_by_id(feed.feed_id)
        transact_items = [
            {"Put": {"TableName": self._t, "Item": _feed_item(feed)}},
            {
                "Put": {
                    "TableName": self._t,
                    "Item": _rss_url_claim_item(feed),
                    "ConditionExpression": (
                        "attribute_not_exists(pk) OR feed_id = :fid"
                    ),
                    "ExpressionAttributeValues": {
                        ":fid": _s(str(feed.feed_id))
                    },
                }
            },
        ]
        if old is not None and old.rss_url and old.rss_url != feed.rss_url:
            transact_items.append(
                {
                    "Delete": {
                        "TableName": self._t,
                        "Key": _key(
                            keys.rss_url_claim_keys(old.rss_url)["pk"],
                            keys.META,
                        ),
                    }
                }
            )
        try:
            await self._c.transact_write_items(TransactItems=transact_items)
        except self._c.exceptions.TransactionCanceledException as exc:
            if _conditional_check_failed_indices(exc):
                raise DuplicateFeedError(
                    f"feed rss_url {feed.rss_url!r} is already taken"
                ) from exc
            raise
        return feed

    async def count_by_status(self, status: str) -> int:
        return await _count(
            self._c,
            self._t,
            index="gsi2",
            KeyConditionExpression="gsi2pk = :p",
            ExpressionAttributeValues={":p": _s(f"STATUS#{status}")},
        )

    async def count_by_statuses(self, statuses: list[str]) -> int:
        if not statuses:
            return 0
        total = 0
        for status in statuses:
            total += await self.count_by_status(status)
        return total

    async def count_all(self) -> int:
        return await _count(
            self._c,
            self._t,
            FilterExpression=_TYPE_FILTER,
            ExpressionAttributeNames=_TYPE_NAMES,
            ExpressionAttributeValues=_type_values(codec.TYPE_FEED),
        )

    async def list_all(self, *, limit: Optional[int] = None) -> list[models.Feed]:
        # No "all feeds by created_at" index exists; scan + in-code sort.
        items = await _scan_all(
            self._c,
            self._t,
            FilterExpression=_TYPE_FILTER,
            ExpressionAttributeNames=_TYPE_NAMES,
            ExpressionAttributeValues=_type_values(codec.TYPE_FEED),
        )
        feeds = _sort_by_created([codec.item_to_model(models.Feed, i) for i in items])
        if limit is not None:
            feeds = feeds[:limit]
        return feeds


# ---------------------------------------------------------------------------
# Episode repository
# ---------------------------------------------------------------------------


def _episode_key(episode: models.Episode) -> dict:
    """Main-table ``Key`` (pk/sk) for an episode item.

    The sk embeds ``published_at``, so a re-save with a changed
    ``published_at`` lands on a different key — callers use this to
    delete the stale item under the old key in the same transaction.
    """
    key_attrs = keys.episode_keys(
        episode.episode_id,
        feed_id=episode.feed_id,
        published_at=episode.published_at,
        processed=episode.processed,
    )
    return _key(key_attrs["pk"], key_attrs["sk"])


def _episode_item(episode: models.Episode) -> dict:
    codec.apply_defaults(episode)
    key_attrs = keys.episode_keys(
        episode.episode_id,
        feed_id=episode.feed_id,
        published_at=episode.published_at,
        processed=episode.processed,
    )
    return codec.model_to_item(episode, key_attrs, codec.TYPE_EPISODE)


async def _get_episode_by_id(
    client: Any, table_name: str, episode_id: UUID
) -> Optional[models.Episode]:
    """Fetch one episode by id through the gsi1 point lookup.

    Module-level so both :meth:`_EpisodeRepository.get_by_id` and the
    playlist link resolvers share the single implementation (Linear:
    XIN-125) — batch this one place later (backlog XIN-107) and the N+1
    in ``list_episodes``/``list_entries`` is fixed everywhere at once.
    """
    items = await _query_all(
        client,
        table_name,
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
        ExpressionAttributeValues={
            ":p": _s(f"EP#{episode_id}"),
            ":s": _s(keys.META),
        },
    )
    return codec.item_to_model(models.Episode, items[0]) if items else None


async def _playlist_exists(
    client: Any, table_name: str, playlist_id: UUID
) -> bool:
    """Point lookup for a playlist through gsi1 (``PL#<id>`` / META)."""
    items = await _query_all(
        client,
        table_name,
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
        ExpressionAttributeValues={
            ":p": _s(f"PL#{playlist_id}"),
            ":s": _s(keys.META),
        },
    )
    return bool(items)


def _guid_marker_item(episode: models.Episode) -> dict:
    key_attrs = keys.guid_marker_keys(episode.feed_id, episode.guid)
    item = {name: _s(value) for name, value in key_attrs.items()}
    item["type"] = _s(codec.TYPE_GUID_MARKER)
    item["episode_id"] = _s(str(episode.episode_id))
    return item


class _EpisodeRepository(EpisodeRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def get_by_id(self, episode_id: UUID) -> Optional[models.Episode]:
        return await _get_episode_by_id(self._c, self._t, episode_id)

    async def list_guids_by_feed(self, feed_id: UUID) -> set[str]:
        # Union of guid attributes on episode items (written by save()) and
        # guid-dedup marker items (written by save_many()). Episodes with a
        # null/empty guid are excluded — they cannot be deduped.
        pk_value = _s(f"FEED#{feed_id}")
        ep_items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={":p": pk_value, ":e": _s("EP#")},
            ProjectionExpression="guid",
        )
        guids = {
            codec.deserialize_plain(item["guid"])
            for item in ep_items
            if "guid" in item
        }
        marker_items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :g)",
            ExpressionAttributeValues={":p": pk_value, ":g": _s("GUID#")},
            ProjectionExpression="sk",
        )
        for item in marker_items:
            guid = item["sk"]["S"].split("GUID#", 1)[1]
            if guid:
                guids.add(guid)
        return {g for g in guids if g}

    async def list_episodes_by_feed(self, feed_id: UUID) -> list[models.Episode]:
        # sk = EP#<published_ts>#<episode_id>, descending; the 0001-…
        # sentinel sorts last, giving published_at DESC NULLS LAST.
        items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={
                ":p": _s(f"FEED#{feed_id}"),
                ":e": _s("EP#"),
            },
            ScanIndexForward=False,
        )
        return [codec.item_to_model(models.Episode, i) for i in items]

    async def list_unprocessed(
        self,
        *,
        feed_id: Optional[UUID] = None,
        limit: int = 50,
    ) -> list[models.Episode]:
        # Global: the sparse gsi3 holds ONLY unprocessed episodes, so every
        # returned item matches — no filter needed. Per-feed: query the
        # feed's episode partition and filter on processed; the
        # LastEvaluatedKey loop in _query_until is REQUIRED because DynamoDB
        # applies Limit before FilterExpression (a single page may return
        # zero matches while more exist).
        if feed_id is not None:
            items = await _query_until(
                self._c,
                self._t,
                limit,
                KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
                # "processed" is a DynamoDB reserved keyword -> #p.
                FilterExpression="#p = :f",
                ExpressionAttributeNames={"#p": "processed"},
                ExpressionAttributeValues={
                    ":p": _s(f"FEED#{feed_id}"),
                    ":e": _s("EP#"),
                    ":f": {"BOOL": False},
                },
                ScanIndexForward=False,
            )
        else:
            items = await _query_until(
                self._c,
                self._t,
                limit,
                IndexName="gsi3",
                KeyConditionExpression="gsi3pk = :p",
                ExpressionAttributeValues={":p": _s("UNPROCESSED")},
                ScanIndexForward=False,
            )
        return [codec.item_to_model(models.Episode, i) for i in items]

    async def save(self, episode: models.Episode) -> models.Episode:
        """Upsert by primary key; maintains the guid-dedup marker.

        Atomicity: one ``TransactWriteItems`` call holding the episode
        put, the guid-marker put (when the episode has a guid), the
        stale-marker delete (when the guid changed), and the stale-item
        delete (when the episode key moved — e.g. a ``published_at``
        change moves the sk) — a crash can never leave a stale marker
        claimed forever (Linear: XIN-123) or a duplicate episode item
        under the old sk. The
        transactional cross-episode dedup guarantee lives in
        :meth:`save_many`, which is what the sync pipeline uses;
        concurrent ``save()`` calls racing on the same guid are
        last-writer-wins.

        Consistency: the old-guid lookup goes through :meth:`get_by_id`,
        which reads gsi1 — eventually consistent on real AWS, so a read
        immediately after a prior write can see stale data (e.g. miss an
        episode that was just saved and skip the stale-marker cleanup).
        Residual staleness window: a gsi1-stale ``old`` can only ever
        skip a cleanup or delete an already-dangling claim — the delete
        is idempotent, and the marker for the *current* guid is always
        written, so dedup for the live guid is never poisoned.
        """
        is_new = episode.episode_id is None
        codec.apply_defaults(episode)
        old = None if is_new else await self.get_by_id(episode.episode_id)
        transact_items: list[dict] = [
            {"Put": {"TableName": self._t, "Item": _episode_item(episode)}}
        ]
        if episode.guid:
            transact_items.append(
                {
                    "Put": {
                        "TableName": self._t,
                        "Item": _guid_marker_item(episode),
                    }
                }
            )
        if old is not None and old.guid and old.guid != episode.guid:
            transact_items.append(
                {
                    "Delete": {
                        "TableName": self._t,
                        "Key": _key(
                            f"FEED#{old.feed_id}", f"GUID#{old.guid}"
                        ),
                    }
                }
            )
        if old is not None and _episode_key(old) != _episode_key(episode):
            # published_at (or feed/processed) changed: the new item lands
            # under a new sk, so delete the stale item under the old key
            # in the same transaction — otherwise list_episodes_by_feed
            # returns both copies of the episode.
            transact_items.append(
                {"Delete": {"TableName": self._t, "Key": _episode_key(old)}}
            )
        await self._c.transact_write_items(TransactItems=transact_items)
        return episode

    async def _old_guids_for_chunk(
        self, candidates: list[tuple[int, models.Episode]]
    ) -> dict[tuple[str, str], str]:
        """Map (feed_id, episode_id) -> currently-claimed guid.

        Read from the guid-marker items (one ``begins_with(GUID#)`` query
        per feed), so :meth:`save_many` can delete the stale marker when
        a re-saved episode's guid changed — in the same transaction that
        writes the new marker (Linear: XIN-123 §3). A guid change that
        this pre-read misses (marker written concurrently after the
        read) leaves a dangling marker, which the next ``save()`` of
        that episode removes; markers are never read for anything but
        dedup, so a dangling one only ever fails dedup CLOSED.
        """
        feed_ids = {str(episode.feed_id) for _, episode in candidates}
        old_guids: dict[tuple[str, str], str] = {}
        for feed_id in feed_ids:
            marker_items = await _query_all(
                self._c,
                self._t,
                KeyConditionExpression="pk = :p AND begins_with(sk, :g)",
                ExpressionAttributeValues={
                    ":p": _s(f"FEED#{feed_id}"),
                    ":g": _s("GUID#"),
                },
                ProjectionExpression="sk, episode_id",
            )
            for marker in marker_items:
                guid = marker["sk"]["S"].split("GUID#", 1)[1]
                raw_ep_id = marker.get("episode_id")
                if guid and raw_ep_id is not None:
                    old_guids[(feed_id, codec.deserialize_plain(raw_ep_id))] = (
                        guid
                    )
        return old_guids

    async def _old_sks_for_chunk(
        self, candidates: list[tuple[int, models.Episode]]
    ) -> dict[tuple[str, str], str]:
        """Map (feed_id, episode_id) -> current episode sk.

        One ``begins_with(EP#)`` keys-only query per feed, so
        :meth:`save_many` can delete the stale episode item when a
        re-saved episode's key moved (a ``published_at`` change moves
        the sk) — in the same transaction that writes the new item.
        Without this the old item survives under the old sk and
        ``list_episodes_by_feed`` returns duplicate copies of the
        episode. Keys-only projection keeps the pre-read cheap.
        """
        feed_ids = {str(episode.feed_id) for _, episode in candidates}
        old_sks: dict[tuple[str, str], str] = {}
        for feed_id in feed_ids:
            items = await _query_all(
                self._c,
                self._t,
                KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
                ExpressionAttributeValues={
                    ":p": _s(f"FEED#{feed_id}"),
                    ":e": _s("EP#"),
                },
                ProjectionExpression="sk",
            )
            for item in items:
                sk = item["sk"]["S"]
                parts = sk.split("#", 2)
                if len(parts) == 3:
                    old_sks[(feed_id, parts[2])] = sk
        return old_sks

    async def save_many(
        self, episodes: list[models.Episode]
    ) -> list[models.Episode]:
        """Persist a batch of episodes with guid-dedup.

        Atomicity: ``TransactWriteItems`` per chunk, where chunks are
        packed against the DynamoDB 100-item transaction limit (a guid
        episode costs a marker put + an episode put, plus an optional
        stale-marker delete on guid change). Each marker carries
        ``attribute_not_exists(pk)``, so a duplicate guid for the same feed
        cancels only its own episode — conflicting duplicates are dropped
        and reported by omission from the return value (input order is
        preserved for the survivors). Chunks commit INDEPENDENTLY: unlike
        the SQL single-flush batch, a later chunk failing does not roll
        back earlier chunks; the return value is the record of what
        persisted. Null-guid episodes skip markers (they cannot be
        deduped) and are plain transactional puts.

        Guid changes: when a candidate re-saves an existing episode under
        a new guid, the old guid's marker is deleted in the SAME
        transaction that writes the new one (Linear: XIN-123 §3) — the
        old guid would otherwise stay claimed forever and poison future
        dedup. Id-less episodes get their ids assigned up front
        (:func:`codec.apply_defaults` runs before any marker is built),
        so the persisted marker's ``episode_id`` is the real id and an
        idempotent re-write of the batch resolves its own markers
        (Linear: XIN-124).
        """
        if not episodes:
            return []
        # Assign ids/defaults BEFORE any item is built: the guid-dedup
        # marker embeds episode_id, and without this an id-less episode's
        # marker would carry the literal string "None" (XIN-124).
        # Within-batch dedup second: one TransactWriteItems call cannot
        # contain two operations on the same marker item, so keep only the
        # first episode per (feed_id, guid).
        seen: set[tuple[str, str]] = set()
        candidates: list[tuple[int, models.Episode]] = []
        for index, episode in enumerate(episodes):
            codec.apply_defaults(episode)
            dedup_key = (
                (str(episode.feed_id), episode.guid) if episode.guid else None
            )
            if dedup_key is not None:
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
            candidates.append((index, episode))

        persisted: set[int] = set()
        # The stale-marker pre-read is one begins_with(GUID#) query per
        # feed over all candidates (not per chunk) — the chunker needs
        # the guid-change costs up front. Skipped entirely when no
        # candidate has a guid (nothing can need a stale-marker delete).
        if any(episode.guid for _, episode in candidates):
            old_guids = await self._old_guids_for_chunk(candidates)
        else:
            old_guids = {}
        # Stale-sk pre-read: one keys-only query per feed, so a re-saved
        # episode whose published_at changed gets its old item deleted in
        # the same transaction that writes the new one.
        old_sks = await self._old_sks_for_chunk(candidates)
        for chunk in _save_many_op_chunks(candidates, old_guids, old_sks):
            remaining = list(chunk)
            while remaining:
                transact_items: list[dict] = []
                owners: list[int] = []  # transact-item idx -> input idx
                for index, episode in remaining:
                    if episode.guid:
                        transact_items.append(
                            {
                                "Put": {
                                    "TableName": self._t,
                                    "Item": _guid_marker_item(episode),
                                    "ConditionExpression": "attribute_not_exists(pk)",
                                }
                            }
                        )
                        owners.append(index)
                        prev_guid = old_guids.get(
                            (str(episode.feed_id), str(episode.episode_id))
                        )
                        if prev_guid is not None and prev_guid != episode.guid:
                            # Guid change on a re-saved episode: release
                            # the old guid in the same transaction.
                            transact_items.append(
                                {
                                    "Delete": {
                                        "TableName": self._t,
                                        "Key": _key(
                                            f"FEED#{episode.feed_id}",
                                            f"GUID#{prev_guid}",
                                        ),
                                    }
                                }
                            )
                            owners.append(index)
                    transact_items.append(
                        {"Put": {"TableName": self._t, "Item": _episode_item(episode)}}
                    )
                    owners.append(index)
                    new_sk = _episode_key(episode)["sk"]["S"]
                    old_sk = old_sks.get(
                        (str(episode.feed_id), str(episode.episode_id))
                    )
                    if old_sk is not None and old_sk != new_sk:
                        # published_at changed: the put above lands under a
                        # new sk — delete the stale item under the old sk in
                        # the same transaction.
                        transact_items.append(
                            {
                                "Delete": {
                                    "TableName": self._t,
                                    "Key": _key(
                                        f"FEED#{episode.feed_id}", old_sk
                                    ),
                                }
                            }
                        )
                        owners.append(index)
                try:
                    await self._c.transact_write_items(TransactItems=transact_items)
                except self._c.exceptions.TransactionCanceledException as exc:
                    conflicts = _conditional_check_failed_indices(exc)
                    if not conflicts:
                        raise
                    # A ConditionalCheckFailed means the marker already
                    # exists. That is a genuine duplicate — unless the
                    # marker points at THIS episode (idempotent re-write,
                    # e.g. the episode was persisted via save()), in which
                    # case the episode item is rewritten unconditionally.
                    failed = {owners[i] for i in conflicts}
                    for input_idx in failed:
                        candidate = next(
                            ep for (j, ep) in remaining if j == input_idx
                        )
                        if await self._resolve_marker_conflict(candidate):
                            persisted.add(input_idx)
                    remaining = [
                        (i, ep) for (i, ep) in remaining if i not in failed
                    ]
                    continue
                persisted.update(i for i, _ in remaining)
                remaining = []
        return [ep for i, ep in enumerate(episodes) if i in persisted]

    async def _resolve_marker_conflict(
        self, episode: models.Episode
    ) -> bool:
        """Resolve a marker conflict for one candidate.

        Returns True when the candidate was (re-)persisted: the winning
        marker belongs to the same episode, so this is an idempotent
        re-write rather than a duplicate. Also cleans up a stale marker
        when the episode's guid itself changed — the episode rewrite and
        the stale-marker delete go in one ``TransactWriteItems`` call so
        a crash cannot strand the old guid claimed (Linear: XIN-123).

        Consistency: the marker read is a main-table ``GetItem``
        (strongly consistent), but the old-episode read goes through
        :meth:`get_by_id`, which queries gsi1 — eventually consistent on
        real AWS, so a read immediately after a prior write can see stale
        data. A stale ``old`` only skips a stale-marker cleanup; the
        cleanup is idempotent and the next save of the episode retries
        it.
        """
        resp = await self._c.get_item(
            TableName=self._t,
            Key=_key(f"FEED#{episode.feed_id}", f"GUID#{episode.guid}"),
        )
        marker = resp.get("Item")
        if marker is None or "episode_id" not in marker:
            return False
        if codec.deserialize_plain(marker["episode_id"]) != str(
            episode.episode_id
        ):
            return False
        old = await self.get_by_id(episode.episode_id)
        transact_items: list[dict] = [
            {"Put": {"TableName": self._t, "Item": _episode_item(episode)}}
        ]
        if old is not None and old.guid and old.guid != episode.guid:
            transact_items.append(
                {
                    "Delete": {
                        "TableName": self._t,
                        "Key": _key(f"FEED#{old.feed_id}", f"GUID#{old.guid}"),
                    }
                }
            )
        if old is not None:
            old_sk = _episode_key(old)["sk"]["S"]
            new_sk = _episode_key(episode)["sk"]["S"]
            if old_sk != new_sk:
                # published_at changed: the put above lands under a new sk
                # — delete the stale item under the old sk in the same
                # transaction (the normal save_many path does this via
                # _old_sks_for_chunk; the conflict-rewrite path must too).
                transact_items.append(
                    {
                        "Delete": {
                            "TableName": self._t,
                            "Key": _key(f"FEED#{old.feed_id}", old_sk),
                        }
                    }
                )
        await self._c.transact_write_items(TransactItems=transact_items)
        return True

    async def mark_processed(
        self, episode_id: UUID, processed: bool = True
    ) -> Optional[models.Episode]:
        """Set the ``processed`` flag.

        Atomicity: single-entity read-modify-write via full-item
        ``PutItem`` — last-writer-wins, same as the SQL read-modify-flush.
        Marking processed removes the item from the sparse gsi3 index in
        the same write; un-marking re-adds it.

        Consistency: the episode is read through :meth:`get_by_id`,
        which queries gsi1 — eventually consistent on real AWS, so a read
        immediately after a prior write can see stale data. Rewriting a
        slightly stale row is harmless (last-writer-wins, same as SQL);
        the next read observes the latest write.
        """
        episode = await self.get_by_id(episode_id)
        if episode is None:
            return None
        episode.processed = processed
        await self._c.put_item(TableName=self._t, Item=_episode_item(episode))
        return episode

    async def count_all(self) -> int:
        return await _count(
            self._c,
            self._t,
            FilterExpression=_TYPE_FILTER,
            ExpressionAttributeNames=_TYPE_NAMES,
            ExpressionAttributeValues=_type_values(codec.TYPE_EPISODE),
        )

    async def count_unprocessed(self) -> int:
        return await _count(
            self._c,
            self._t,
            index="gsi3",
            KeyConditionExpression="gsi3pk = :p",
            ExpressionAttributeValues={":p": _s("UNPROCESSED")},
        )


def _conditional_check_failed_indices(exc: Exception) -> list[int]:
    """Transact-item indices whose cancellation reason is ConditionalCheckFailed."""
    reasons = (getattr(exc, "response", None) or {}).get("CancellationReasons", [])
    return [
        i
        for i, reason in enumerate(reasons)
        if isinstance(reason, dict) and reason.get("Code") == "ConditionalCheckFailed"
    ]


# ---------------------------------------------------------------------------
# Insight repository
# ---------------------------------------------------------------------------


def _insight_item(insight: models.Insight) -> dict:
    codec.apply_defaults(insight)
    key_attrs = keys.insight_keys(
        insight.insight_id,
        episode_id=insight.episode_id,
        created_at=insight.created_at,
    )
    return codec.model_to_item(insight, key_attrs, codec.TYPE_INSIGHT)


class _InsightRepository(InsightRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def list_by_episode(
        self, episode_id: UUID
    ) -> list[models.Insight]:
        # sk = INSIGHT#<created_ts>#<insight_id>, ascending.
        items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={
                ":p": _s(f"EP#{episode_id}"),
                ":e": _s("INSIGHT#"),
            },
            ScanIndexForward=True,
        )
        return [codec.item_to_model(models.Insight, i) for i in items]

    async def save(self, insight: models.Insight) -> models.Insight:
        """Upsert by primary key.

        Atomicity: single-item ``PutItem`` — atomic by DynamoDB
        single-item write semantics.
        """
        await self._c.put_item(TableName=self._t, Item=_insight_item(insight))
        return insight

    async def save_many(
        self, insights: list[models.Insight]
    ) -> list[models.Insight]:
        """Persist a batch of insights.

        Atomicity: ordered-writes via ``BatchWriteItem`` (25 per batch)
        with ``UnprocessedItems`` retry — NOT atomic across batches, unlike
        the SQL single-flush batch. Retries are idempotent because
        insight_ids are client-generated UUIDs (a retried put overwrites
        the identical item).
        """
        for insight in insights:
            codec.apply_defaults(insight)
        for chunk in _chunks(list(insights), 25):
            pending = [{"PutRequest": {"Item": _insight_item(i)}} for i in chunk]
            while pending:
                resp = await self._c.batch_write_item(
                    RequestItems={self._t: pending}
                )
                pending = resp.get("UnprocessedItems", {}).get(self._t, [])
        return list(insights)


# ---------------------------------------------------------------------------
# Tag repository
# ---------------------------------------------------------------------------


def _tag_natural_key(name: str, category: Optional[str]) -> str:
    """The gsi1 partition key for a (name, category) pair.

    Derived from :func:`keys.tag_keys` (not reimplemented) so the hash
    normalization can never drift from the key schema. This is the
    case-INSENSITIVE lookup key — it deliberately differs from the
    exact-case claim key built by :func:`keys.tag_natural_key_hash`
    (Linear: XIN-124): lookup finds candidates, the exact (name,
    category) re-check in :meth:`_TagRepository.get_by_name_category`
    picks the SQL-semantics match.
    """
    probe = keys.tag_keys(UUID(int=0), name=name, category=category)
    return probe["gsi1pk"]


def _tag_claim_pk(name: str, category: Optional[str]) -> str:
    """The claim-item partition key for a (name, category) pair.

    Hashes the exact-case pair (via :func:`keys.tag_natural_key_hash`),
    so ``"Foo"`` and ``"FOO"`` are distinct claims — matching the SQL
    case-sensitive unique constraint (Linear: XIN-124).
    """
    return keys.tag_natural_key_hash(name, category).replace(
        "TAGNAME#", "TAGCLAIM#", 1
    )


def _tag_item(tag: models.Tag) -> dict:
    codec.apply_defaults(tag)
    key_attrs = keys.tag_keys(tag.tag_id, name=tag.name, category=tag.category)
    return codec.model_to_item(tag, key_attrs, codec.TYPE_TAG)


class _TagRepository(TagRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def get_by_name_category(
        self, name: str, category: Optional[str]
    ) -> Optional[models.Tag]:
        # The gsi1 key hashes the lowercased pair (unbounded name lengths),
        # so the candidate set is re-checked in Python for the exact
        # case-sensitive (name, category) pair — matching SQL semantics.
        items = await _query_all(
            self._c,
            self._t,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
            ExpressionAttributeValues={
                ":p": _s(_tag_natural_key(name, category)),
                ":s": _s(keys.META),
            },
        )
        for item in items:
            tag = codec.item_to_model(models.Tag, item)
            if tag.name == name and tag.category == category:
                return tag
        return None

    async def get_or_create(
        self, name: str, category: Optional[str]
    ) -> models.Tag:
        """Return the tag for (name, category), creating it if needed.

        Atomicity: ordered-writes + idempotent retry. A conditional claim
        item (``attribute_not_exists``) on the hashed natural key elects
        exactly one winner among concurrent creators; the loser re-reads
        the winner's tag with bounded retry. At most one tag per
        (name, category) is ever visible — the DynamoDB analogue of the
        SQL unique constraint that backs this method.

        Crash recovery: a claim item is never deleted on the happy path,
        so a winner crash between the claim put and the tag put would
        otherwise poison the natural key forever (every future call would
        wait out the retry budget and raise). When the retry budget is
        exhausted, the stale claim is deleted and the whole operation is
        retried once — bounded, no infinite loop.
        """
        return await self._get_or_create(name, category, recovered=False)

    async def _get_or_create(
        self, name: str, category: Optional[str], *, recovered: bool
    ) -> models.Tag:
        existing = await self.get_by_name_category(name, category)
        if existing is not None:
            return existing
        tag = models.Tag(name=name, category=category)
        codec.apply_defaults(tag)
        # Deliberate tradeoff: claim items accumulate forever and are
        # never garbage-collected. The claim is the write-time uniqueness
        # lock that the whole get_or_create race protocol relies on (a
        # later creator must observe the earlier winner's claim even
        # across crashes, and the stale-claim recovery path must be able
        # to distinguish "winner crashed" from "never claimed"), so a
        # naive GC that deleted claims would re-open the duplicate-tag
        # race. Claims are tiny (one item per natural key) — do NOT add
        # naive GC here.
        claim_pk = _tag_claim_pk(name, category)
        claim_item = {
            "pk": _s(claim_pk),
            "sk": _s(keys.META),
            "type": _s(codec.TYPE_TAG_CLAIM),
            "tag_id": _s(str(tag.tag_id)),
        }
        try:
            await self._c.put_item(
                TableName=self._t,
                Item=claim_item,
                ConditionExpression="attribute_not_exists(pk)",
            )
        except self._c.exceptions.ConditionalCheckFailedException:
            try:
                return await self._read_claim_winner(claim_pk)
            except RuntimeError:
                if recovered:
                    raise
                # Stale claim: the winner crashed between its claim put
                # and its tag put, so the claim can never resolve. Delete
                # the poisoned claim and retry once (bounded).
                await self._c.delete_item(
                    TableName=self._t, Key=_key(claim_pk, keys.META)
                )
                return await self._get_or_create(
                    name, category, recovered=True
                )
        await self._c.put_item(TableName=self._t, Item=_tag_item(tag))
        return tag

    async def _read_claim_winner(self, claim_pk: str) -> models.Tag:
        """Re-read the tag created by the winner of a claim race."""
        for _ in range(20):
            resp = await self._c.get_item(
                TableName=self._t, Key=_key(claim_pk, keys.META)
            )
            claim = resp.get("Item")
            if claim is not None and "tag_id" in claim:
                tag_id = codec.deserialize_plain(claim["tag_id"])
                tag_resp = await self._c.get_item(
                    TableName=self._t, Key=_key(f"TAG#{tag_id}", keys.META)
                )
                tag_item = tag_resp.get("Item")
                if tag_item is not None:
                    return codec.item_to_model(models.Tag, tag_item)
            await asyncio.sleep(0.05)
        raise RuntimeError(
            f"Tag claim {claim_pk} exists but its tag never materialized"
        )

    async def add_episode_tag(self, episode_id: UUID, tag_id: UUID) -> None:
        """Link a tag to an episode (idempotent).

        Atomicity: single-item ``PutItem`` keyed by (episode_id, tag_id) —
        re-adding overwrites the identical item, so the operation is
        naturally idempotent with no read-before-write.

        FK parity with the SQL backend (Linear: XIN-124 — Chester's call):
        the episode and the tag must both exist, otherwise
        :class:`MissingParentError` is raised instead of silently creating
        an orphaned link. Two point reads gate the write; the link write
        itself stays a single atomic ``PutItem``.

        Consistency: the episode read goes through gsi1 (eventually
        consistent on real AWS), so an episode saved a moment ago can be
        missed and the link spuriously rejected — the caller should retry.
        The SQL backend has no such window (same-transaction FK check).
        """
        if await _get_episode_by_id(self._c, self._t, episode_id) is None:
            raise MissingParentError(
                f"episode {episode_id} does not exist"
            )
        tag_resp = await self._c.get_item(
            TableName=self._t, Key=_key(f"TAG#{tag_id}", keys.META)
        )
        if tag_resp.get("Item") is None:
            raise MissingParentError(f"tag {tag_id} does not exist")
        key_attrs = keys.episode_tag_link_keys(episode_id, tag_id)
        item = {name: _s(value) for name, value in key_attrs.items()}
        item["type"] = _s(codec.TYPE_EPISODE_TAG_LINK)
        await self._c.put_item(TableName=self._t, Item=item)

    async def list_tags_for_episode(
        self, episode_id: UUID
    ) -> list[models.Tag]:
        link_items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={
                ":p": _s(f"EP#{episode_id}"),
                ":e": _s("TAGLINK#"),
            },
            ProjectionExpression="sk",
        )
        tag_ids = [
            item["sk"]["S"].split("TAGLINK#", 1)[1] for item in link_items
        ]
        if not tag_ids:
            return []
        tag_items = await _batch_get(
            self._c,
            self._t,
            [_key(f"TAG#{tag_id}", keys.META) for tag_id in tag_ids],
        )
        tags = [codec.item_to_model(models.Tag, i) for i in tag_items]
        tags.sort(key=lambda t: t.name)
        return tags


# ---------------------------------------------------------------------------
# User repository
# ---------------------------------------------------------------------------


def _user_item(user: models.User) -> dict:
    codec.apply_defaults(user)
    key_attrs = keys.user_keys(user.user_id, email=user.email)
    return codec.model_to_item(user, key_attrs, codec.TYPE_USER)


def _email_claim_item(user: models.User) -> dict:
    """Build the email-claim item for a user (Linear: XIN-124).

    The claim's key (``EMAILCLAIM#<sha256(email)>`` / ``META``) is the
    write-time uniqueness lock for ``User.email`` — the DynamoDB analogue
    of the SQL ``unique=True`` constraint. The ``user_id`` attribute lets
    re-saves of the same user pass the claim's condition expression.
    Claim items accumulate (never deleted except when a user's own email
    changes); see the tag-claim tradeoff comment in
    :meth:`_TagRepository._get_or_create`.
    """
    key_attrs = keys.email_claim_keys(user.email)
    item = {name: _s(value) for name, value in key_attrs.items()}
    item["type"] = _s(codec.TYPE_EMAIL_CLAIM)
    item["user_id"] = _s(str(user.user_id))
    return item


class _UserRepository(UserRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def get_by_id(self, user_id: UUID) -> Optional[models.User]:
        resp = await self._c.get_item(
            TableName=self._t, Key=_key(f"USER#{user_id}", keys.META)
        )
        item = resp.get("Item")
        return codec.item_to_model(models.User, item) if item else None

    async def get_by_email(self, email: str) -> Optional[models.User]:
        # The gsi1 key hashes the normalized email; re-check the exact
        # address in Python to preserve SQL's case-sensitive semantics.
        probe = keys.user_keys(UUID(int=0), email=email)
        items = await _query_all(
            self._c,
            self._t,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
            ExpressionAttributeValues={
                ":p": _s(probe["gsi1pk"]),
                ":s": _s(keys.META),
            },
        )
        for item in items:
            user = codec.item_to_model(models.User, item)
            if user.email == email:
                return user
        return None

    async def save(self, user: models.User) -> models.User:
        """Upsert by primary key, enforcing email uniqueness at write time.

        Atomicity: the user item and its email-claim item are written in
        one ``TransactWriteItems`` call, with
        ``attribute_not_exists(pk) OR user_id = :uid`` on the claim. An
        email taken by a *different* user cancels the whole write and
        raises :class:`ValueError` (SQL raises ``IntegrityError`` on the
        unique constraint instead — the error types differ but both
        reject the duplicate). Re-saving the same user (idempotent
        re-write) passes the condition. When the email itself changed,
        the stale claim is deleted in the SAME transaction, so a crash
        can never leave the old address claimed forever (Linear: XIN-123).

        Consistency: the old-row lookup goes through :meth:`get_by_id`,
        a main-table ``GetItem`` (strongly consistent) — no staleness
        window on the stale-claim cleanup, unlike the slug-claim path
        which reads through gsi1.
        """
        is_new = user.user_id is None
        codec.apply_defaults(user)
        old = None if is_new else await self.get_by_id(user.user_id)
        transact_items = [
            {"Put": {"TableName": self._t, "Item": _user_item(user)}},
            {
                "Put": {
                    "TableName": self._t,
                    "Item": _email_claim_item(user),
                    "ConditionExpression": (
                        "attribute_not_exists(pk) OR user_id = :uid"
                    ),
                    "ExpressionAttributeValues": {
                        ":uid": _s(str(user.user_id))
                    },
                }
            },
        ]
        if old is not None and old.email and old.email != user.email:
            transact_items.append(
                {
                    "Delete": {
                        "TableName": self._t,
                        "Key": _key(
                            keys.email_claim_keys(old.email)["pk"],
                            keys.META,
                        ),
                    }
                }
            )
        try:
            await self._c.transact_write_items(TransactItems=transact_items)
        except self._c.exceptions.TransactionCanceledException as exc:
            if _conditional_check_failed_indices(exc):
                raise ValueError(
                    f"user email {user.email!r} is already taken"
                ) from exc
            raise
        return user


# ---------------------------------------------------------------------------
# Playlist repository
# ---------------------------------------------------------------------------


def _playlist_item(playlist: models.CuratedPlaylist) -> dict:
    codec.apply_defaults(playlist)
    key_attrs = keys.playlist_keys(
        playlist.playlist_id,
        user_id=playlist.user_id,
        created_at=playlist.created_at,
    )
    return codec.model_to_item(playlist, key_attrs, codec.TYPE_PLAYLIST)


def _slug_claim_item(playlist: models.CuratedPlaylist) -> dict:
    """Build the slug-claim item for a playlist's ``slug``.

    The claim's key (``SLUG#<slug>`` / ``META``) is the write-time
    uniqueness lock; the ``playlist_id`` attribute lets ``get_by_slug``
    resolve the owning playlist and lets re-saves of the same playlist
    pass the claim's condition expression.
    """
    key_attrs = keys.slug_claim_keys(playlist.slug)
    item = {name: _s(value) for name, value in key_attrs.items()}
    item["type"] = _s(codec.TYPE_SLUG_CLAIM)
    item["playlist_id"] = _s(str(playlist.playlist_id))
    return item


class _PlaylistRepository(PlaylistRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def list_by_user(
        self, user_id: UUID
    ) -> list[models.CuratedPlaylist]:
        # sk = PL#<created_ts>#<playlist_id>, ascending. The "PL#" prefix
        # does not collide with "PROG#" items sharing the USER# partition.
        items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={
                ":p": _s(f"USER#{user_id}"),
                ":e": _s("PL#"),
            },
            ScanIndexForward=True,
        )
        return [codec.item_to_model(models.CuratedPlaylist, i) for i in items]

    async def get_by_id(
        self, playlist_id: UUID
    ) -> Optional[models.CuratedPlaylist]:
        items = await _query_all(
            self._c,
            self._t,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :p AND gsi1sk = :s",
            ExpressionAttributeValues={
                ":p": _s(f"PL#{playlist_id}"),
                ":s": _s(keys.META),
            },
        )
        return codec.item_to_model(models.CuratedPlaylist, items[0]) if items else None

    async def save(self, playlist: models.CuratedPlaylist) -> models.CuratedPlaylist:
        """Upsert by primary key, enforcing slug uniqueness at write time.

        Atomicity: when the playlist has a slug, the playlist item, its
        slug-claim item, and (on a slug change) the stale-claim delete go
        in one ``TransactWriteItems`` call — different items are allowed
        in one transaction, so a crash can never leave the old slug
        claimed forever (Linear: XIN-123). The claim carries
        ``attribute_not_exists(pk) OR playlist_id = :pid``: a slug taken
        by a *different* playlist cancels the whole write and raises
        :class:`SlugConflictError`; re-saving the same playlist
        (idempotent re-write) passes the condition. Slug-less playlists
        are a single-item ``PutItem``, as before.

        Consistency: the old-row lookup goes through :meth:`get_by_id`,
        which reads gsi1 — eventually consistent on real AWS. Residual
        staleness window: if gsi1 returns a stale ``old`` that misses a
        very recent slug change on this same playlist, the true stale
        claim is not included in this transaction and stays claimed.
        The stale-claim delete is idempotent, so a stale ``old`` can
        only ever delete an already-dangling claim — never a live one.
        """
        is_new = playlist.playlist_id is None
        codec.apply_defaults(playlist)
        old = None if is_new else await self.get_by_id(playlist.playlist_id)
        item = _playlist_item(playlist)
        if playlist.slug:
            transact_items: list[dict] = [
                {"Put": {"TableName": self._t, "Item": item}},
                {
                    "Put": {
                        "TableName": self._t,
                        "Item": _slug_claim_item(playlist),
                        "ConditionExpression": (
                            "attribute_not_exists(pk)"
                            " OR playlist_id = :pid"
                        ),
                        "ExpressionAttributeValues": {
                            ":pid": _s(str(playlist.playlist_id))
                        },
                    }
                },
            ]
            if old is not None and old.slug and old.slug != playlist.slug:
                transact_items.append(
                    {
                        "Delete": {
                            "TableName": self._t,
                            "Key": _key(
                                keys.slug_claim_keys(old.slug)["pk"],
                                keys.META,
                            ),
                        }
                    }
                )
            try:
                await self._c.transact_write_items(
                    TransactItems=transact_items
                )
            except self._c.exceptions.TransactionCanceledException as exc:
                if _conditional_check_failed_indices(exc):
                    raise SlugConflictError(
                        f"playlist slug {playlist.slug!r} is already taken"
                    ) from exc
                raise
        else:
            await self._c.put_item(TableName=self._t, Item=item)
        return playlist

    async def add_episode(
        self, playlist_id: UUID, episode_id: UUID, position: int
    ) -> None:
        """Link an episode into a playlist (upsert on the link key).

        FK parity with the SQL backend (Linear: XIN-124 — Chester's call):
        the playlist and the episode must both exist, otherwise
        :class:`MissingParentError` is raised instead of silently creating
        an orphaned link. Two point reads (gsi1) gate the write; the link
        write itself stays a single atomic ``UpdateItem``.

        Atomicity: single-item ``UpdateItem`` — re-adding overwrites the
        link's ``position`` in one atomic write, while ``added_at`` is set
        only on first insert (``if_not_exists``), so re-adding an existing
        link preserves the original added date (Linear: XIN-98). No
        read-before-write on the link itself, so concurrent adds cannot
        duplicate it.
        """
        if not await _playlist_exists(self._c, self._t, playlist_id):
            raise MissingParentError(
                f"playlist {playlist_id} does not exist"
            )
        if await _get_episode_by_id(self._c, self._t, episode_id) is None:
            raise MissingParentError(
                f"episode {episode_id} does not exist"
            )
        key_attrs = keys.playlist_episode_link_keys(playlist_id, episode_id)
        await self._c.update_item(
            TableName=self._t,
            Key={name: _s(value) for name, value in key_attrs.items()},
            UpdateExpression=(
                "SET #typ = :typ, #pos = :p, "
                "#added = if_not_exists(#added, :now)"
            ),
            ExpressionAttributeNames={
                "#typ": "type",
                "#pos": "position",
                "#added": "added_at",
            },
            ExpressionAttributeValues={
                ":typ": _s(codec.TYPE_PLAYLIST_EPISODE_LINK),
                ":p": {"N": str(position)},
                ":now": _s(keys.iso_timestamp(_now_utc())),
            },
        )

    async def _ordered_link_entries(
        self, playlist_id: UUID
    ) -> tuple[list[tuple[int, UUID, datetime]], dict[str, models.Episode]]:
        """Shared link query for ``list_episodes``/``list_entries``.

        Returns ``(ordered, episodes)``: ``ordered`` is the
        ``(position, episode_id, added_at)`` rows sorted by
        ``(position, episode_id)`` ascending (ties on position break by
        episode_id, fully deterministic on all backends), and
        ``episodes`` maps ``str(episode_id)`` to the episode for links
        whose episode still exists. Links to deleted episodes are
        dropped here, so both public methods project from the same
        resolved rows (Linear: XIN-125).

        ``added_at`` fallback: a link item predating the XIN-98
        ``added_at`` attribute has none; fall back to the episode's
        ``published_at`` (deterministic), then to now (Linear: XIN-124).
        """
        link_items = await _query_all(
            self._c,
            self._t,
            KeyConditionExpression="pk = :p AND begins_with(sk, :e)",
            ExpressionAttributeValues={
                ":p": _s(f"PL#{playlist_id}"),
                ":e": _s("PLEP#"),
            },
        )
        parsed: list[tuple[int, UUID, Optional[datetime]]] = []
        for link in link_items:
            ep_id = UUID(link["sk"]["S"].split("PLEP#", 1)[1])
            position = int(
                codec.deserialize_plain(link.get("position", {"N": "0"}))
            )
            raw_added = link.get("added_at", {}).get("S")
            added_at = (
                codec._parse_datetime(raw_added) if raw_added else None
            )
            parsed.append((position, ep_id, added_at))
        episodes: dict[str, models.Episode] = {}
        for _, ep_id, _ in parsed:
            episode = await _get_episode_by_id(self._c, self._t, ep_id)
            if episode is not None:
                episodes[str(ep_id)] = episode
        ordered: list[tuple[int, UUID, datetime]] = []
        for position, ep_id, added_at in sorted(
            parsed, key=lambda e: (e[0], e[1])
        ):
            if str(ep_id) not in episodes:
                continue
            if added_at is None:
                added_at = episodes[str(ep_id)].published_at or _now_utc()
            ordered.append((position, ep_id, added_at))
        return ordered, episodes

    async def list_episodes(
        self, playlist_id: UUID
    ) -> list[models.Episode]:
        # Order by (position, episode_id): ties on position are broken by
        # episode_id ascending, fully deterministic on all backends.
        ordered, episodes = await self._ordered_link_entries(playlist_id)
        return [episodes[str(ep_id)] for _, ep_id, _ in ordered]

    async def list_entries(
        self, playlist_id: UUID
    ) -> list[PlaylistEpisodeEntry]:
        # Same resolved rows as list_episodes, but keep position + added_at
        # per link so the RSS endpoint can emit the curator order and the
        # added-date pubDate.
        ordered, episodes = await self._ordered_link_entries(playlist_id)
        return [
            PlaylistEpisodeEntry(
                episode=episodes[str(ep_id)],
                position=position,
                added_at=added_at,
            )
            for position, ep_id, added_at in ordered
        ]

    async def publish(
        self, playlist_id: UUID, visibility: str
    ) -> Optional[models.CuratedPlaylist]:
        validate_visibility(visibility)
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        playlist.visibility = visibility
        if playlist.slug is None:
            playlist.slug = generate_slug(playlist.title)
        if playlist.token is None:
            playlist.token = generate_token()
        return await self.save(playlist)

    async def unpublish(
        self, playlist_id: UUID
    ) -> Optional[models.CuratedPlaylist]:
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        playlist.visibility = VISIBILITY_UNLISTED
        return await self.save(playlist)

    async def rotate_token(self, playlist_id: UUID) -> Optional[str]:
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        new_token = generate_token()
        playlist.token = new_token
        playlist.token_revoked_at = _now_utc()
        await self.save(playlist)
        return new_token

    async def get_by_slug(self, slug: str) -> Optional[models.CuratedPlaylist]:
        resp = await self._c.get_item(
            TableName=self._t, Key=_key(f"SLUG#{slug}", keys.META)
        )
        claim = resp.get("Item")
        if not claim:
            return None
        # A malformed claim (no playlist_id) resolves to None, not
        # KeyError (Linear: XIN-124).
        raw_pid = claim.get("playlist_id")
        if raw_pid is None:
            return None
        playlist_id = keys.uuid_from_str(
            str(codec.deserialize_plain(raw_pid))
        )
        playlist = await self.get_by_id(playlist_id)
        # Defense-in-depth (Linear: XIN-123 §4): the claim may be skewed
        # relative to the playlist row (e.g. a stale claim that survived
        # a crash). Never return a playlist whose slug differs from the
        # requested one — SQL's WHERE slug = ? cannot do this.
        if playlist is None or playlist.slug != slug:
            return None
        return playlist

    async def _episode_repo_get(self, episode_id: UUID) -> Optional[models.Episode]:
        # Thin wrapper kept for the XIN-107 batching seam; the query
        # itself lives in the shared _get_episode_by_id (XIN-125).
        return await _get_episode_by_id(self._c, self._t, episode_id)


# ---------------------------------------------------------------------------
# Progress repository
# ---------------------------------------------------------------------------


def _progress_item(progress: models.UserEpisodeProgress) -> dict:
    codec.apply_defaults(progress)
    key_attrs = keys.progress_keys(progress.user_id, progress.episode_id)
    return codec.model_to_item(progress, key_attrs, codec.TYPE_PROGRESS)


class _ProgressRepository(ProgressRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def get(
        self, user_id: UUID, episode_id: UUID
    ) -> Optional[models.UserEpisodeProgress]:
        resp = await self._c.get_item(
            TableName=self._t,
            Key=_key(f"USER#{user_id}", f"PROG#{episode_id}"),
        )
        item = resp.get("Item")
        return codec.item_to_model(models.UserEpisodeProgress, item) if item else None

    async def save(
        self, progress: models.UserEpisodeProgress
    ) -> models.UserEpisodeProgress:
        """Upsert by (user_id, episode_id).

        Atomicity: single-item ``PutItem`` — the composite key IS the item
        key, so the upsert is one atomic write (the DynamoDB analogue of
        the SQL merge() the SQL backend uses for this upsert contract).
        """
        await self._c.put_item(TableName=self._t, Item=_progress_item(progress))
        return progress


# ---------------------------------------------------------------------------
# TaskLog repository
# ---------------------------------------------------------------------------


def _task_log_item(task_log: models.TaskLog) -> dict:
    codec.apply_defaults(task_log)
    key_attrs = keys.task_log_keys(
        task_log.task_log_id,
        task_type=task_log.task_type,
        status=task_log.status,
        created_at=task_log.created_at,
    )
    return codec.model_to_item(task_log, key_attrs, codec.TYPE_TASK_LOG)


class _TaskLogRepository(TaskLogRepository):
    def __init__(self, client: Any, table_name: str) -> None:
        self._c = client
        self._t = table_name

    async def save(self, task_log: models.TaskLog) -> models.TaskLog:
        """Upsert by primary key.

        Atomicity: single-item ``PutItem`` — atomic by DynamoDB
        single-item write semantics. A changed ``status`` rewrites the
        gsi2 keys in the same write, so the type/status index can never
        go stale.
        """
        await self._c.put_item(TableName=self._t, Item=_task_log_item(task_log))
        return task_log

    async def list_by_type_status(
        self,
        task_type: str,
        status: str,
        limit: Optional[int] = None,
    ) -> list[models.TaskLog]:
        # gsi2sk = created_at, newest first. No filter expression here, so
        # Limit applies to returned rows directly — but paginate anyway
        # when a limit is given, stopping once it is satisfied.
        kwargs: dict[str, Any] = {
            "IndexName": "gsi2",
            "KeyConditionExpression": "gsi2pk = :p",
            "ExpressionAttributeValues": {
                ":p": _s(f"TASKTYPE#{task_type}#{status}")
            },
            "ScanIndexForward": False,
        }
        if limit is not None:
            items = await _query_until(self._c, self._t, limit, **kwargs)
        else:
            items = await _query_all(self._c, self._t, **kwargs)
        return [codec.item_to_model(models.TaskLog, i) for i in items]

    async def update_status(
        self,
        task_log_id: UUID,
        status: str,
        error_message: Optional[str] = None,
    ) -> Optional[models.TaskLog]:
        """Update a task log's status (and optional error message).

        Atomicity: single-entity read-modify-write via full-item
        ``PutItem`` — last-writer-wins, same as the SQL read-modify-flush.

        Consistency: the read is a main-table ``GetItem`` (strongly
        consistent by default), not a gsi1 query, so a read immediately
        after a prior write sees the latest data — no read-after-write
        staleness to self-heal here. Concurrent updates remain
        last-writer-wins.
        """
        resp = await self._c.get_item(
            TableName=self._t, Key=_key(f"TASK#{task_log_id}", keys.META)
        )
        item = resp.get("Item")
        if item is None:
            return None
        entry = codec.item_to_model(models.TaskLog, item)
        entry.status = status
        entry.error_message = error_message
        await self._c.put_item(TableName=self._t, Item=_task_log_item(entry))
        return entry

    async def get_by_type_and_episode(
        self,
        task_type: str,
        episode_id: UUID,
    ) -> Optional[models.TaskLog]:
        """XIN-45: idempotency lookup for the task outbox.

        gsi2 is keyed ``TASKTYPE#{type}#{status}``, so one query per
        status is needed; the episode_id match is a FilterExpression.
        ALL statuses are consulted — including terminal 'done'/'failed'
        rows — because the service resets a terminal-state task to
        'queued' in place (preserving the (task_type, episode_id)
        idempotency key). Returning None for terminal rows would make
        the service create a second TaskLog for the same episode,
        duplicating the outbox record. Active statuses are checked
        first so the common path stays cheap.
        """
        for status in ("queued", "processing", "pending", "failed", "done"):
            items = await _query_all(
                self._c,
                self._t,
                IndexName="gsi2",
                KeyConditionExpression="gsi2pk = :p",
                FilterExpression="episode_id = :e",
                ExpressionAttributeValues={
                    ":p": _s(f"TASKTYPE#{task_type}#{status}"),
                    ":e": _s(str(episode_id)),
                },
                ScanIndexForward=False,
            )
            if items:
                return codec.item_to_model(models.TaskLog, items[0])
        return None
