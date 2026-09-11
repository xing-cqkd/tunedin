"""TaskLog outbox columns + feed URL canonicalization dedup (XIN-44/XIN-45)

Revision ID: 2529fed59a29
Revises: 0f3a4b5c6d7e
Create Date: 2026-09-11

Schema (XIN-45)
--------------
``task_logs`` gains ``episode_id`` (nullable UUID, indexed) so a queued
task durably references the episode it was queued for. The unique index
``uq_task_log_type_episode(task_type, episode_id)`` is the idempotency
key: a re-enqueue of the same (task_type, episode) pair is a no-op
instead of a duplicate row. NULL episode_ids never conflict — both
Postgres and SQLite treat NULLs as distinct in unique indexes — so task
types not tied to an episode are unaffected. The model
(``backend/persistence/models/task_log.py``) declares the same column
and index so ``create_all`` test DBs agree with migrated DBs.

Data (XIN-44)
------------
Existing ``feeds.rss_url`` values are canonicalized in place and feeds
that canonicalize to the same URL are merged:

* group feeds by canonical URL (``_canonicalize_feed_url`` below mirrors
  ``backend.ingestion.canonicalize.canonicalize_feed_url`` as of this
  revision; it is inlined rather than imported so this migration stays
  immutable)
* keep the earliest-created feed per group (NULL ``created_at`` sorts
  last; ties break on ``feed_id`` for determinism)
* repoint the duplicate feed's episodes at the survivor
* when a repointed episode's ``guid`` collides with an episode already in
  the survivor feed (the same RSS feed synced under two URL spellings),
  the two episode rows are merged: the earliest-created episode row wins
  and the loser's dependent rows (insights, episode_tags,
  user_episode_progress, playlist_episodes, task_logs) are repointed at
  the winner. Repoint conflicts resolve by keeping the winner's row —
  the link itself is preserved, so nothing is lost:

  - episode_tags / playlist_episodes: a conflicting loser link is
    dropped (the winner already carries the same link)
  - user_episode_progress: conflicting rows merge — the winner keeps the
    greater ``position_seconds`` and ``completed`` ORs
  - task_logs: a conflicting loser row is dropped (the winner's task log
    for that task_type is preserved)

  This never violates ``uq_episode_feed_guid``: colliding episodes are
  merged before any ``feed_id`` update could create a duplicate pair.
* delete the duplicate feed rows (only ``episodes`` references
  ``feeds.feed_id``; all its rows have been repointed or merged away)
* write the canonical URL back onto every surviving feed row

Unparseable ``rss_url`` values are left untouched (the new code rejects
them at the boundary instead).

Downgrade drops the ``episode_id`` column and its indexes. The feed
dedup is one-way and is NOT reversed — like the XIN-68 guid backfill,
the downgrade only restores the schema.
"""

from typing import Sequence, Union
from urllib.parse import parse_qsl, urlsplit, urlunsplit, urlencode

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2529fed59a29'
down_revision: Union[str, Sequence[str], None] = '0f3a4b5c6d7e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ---------------------------------------------------------------------------
# Canonicalization (XIN-44). Mirrors
# backend.ingestion.canonicalize.canonicalize_feed_url as of this revision;
# inlined so the migration does not depend on future edits of that module.
# ---------------------------------------------------------------------------

_TRACKING_PARAM_PREFIXES = ("utm_",)
_TRACKING_PARAMS = frozenset(
    {
        "fbclid",
        "gclid",
        "gclsrc",
        "dclid",
        "msclkid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "_ga",
        "_gl",
        "ref",
        "source",
    }
)
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _is_tracking_param(name: str) -> bool:
    lowered = name.lower()
    return lowered in _TRACKING_PARAMS or lowered.startswith(
        _TRACKING_PARAM_PREFIXES
    )


def _canonicalize_feed_url(url: str) -> str:
    """Canonical form of a feed URL for identity comparison."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"Invalid feed URL {url!r}: empty")
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"Invalid feed URL {url!r}: expected an http(s) URL"
        )
    host = parts.hostname or ""
    if not host:
        raise ValueError(f"Invalid feed URL {url!r}: missing host")
    host = host.lower()
    port = parts.port
    if port is not None and port == _DEFAULT_PORTS[scheme]:
        port = None
    netloc = f"{host}:{port}" if port is not None else host
    if parts.username:
        userinfo = parts.username
        if parts.password:
            userinfo += f":{parts.password}"
        netloc = f"{userinfo}@{netloc}"
    path = parts.path.rstrip("/") or ""
    query_pairs = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not _is_tracking_param(k)
    ]
    query_pairs.sort(key=lambda kv: (kv[0], kv[1]))
    query = urlencode(query_pairs, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


# ---------------------------------------------------------------------------
# Feed dedup helpers
# ---------------------------------------------------------------------------


def _sort_key(created_at, feed_id):
    """Earliest-created wins; NULL created_at sorts last; feed_id tiebreak."""
    return (created_at is None, str(created_at or ""), str(feed_id))


def _repoint_dependents(bind, loser_id, winner_id) -> None:
    """Repoint every episode-dependent row from the loser to the winner.

    Conflict-safe: where the winner already has the row the merge would
    create, the winner's row is kept and the loser's conflicting row is
    dropped (progress merges instead — see below).
    """
    # insights: no unique constraint on episode_id — plain repoint.
    bind.execute(
        sa.text(
            "UPDATE insights SET episode_id = :winner "
            "WHERE episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    # task_logs: unique (task_type, episode_id) — keep the winner's row.
    bind.execute(
        sa.text(
            "DELETE FROM task_logs WHERE episode_id = :loser AND task_type IN "
            "(SELECT task_type FROM task_logs WHERE episode_id = :winner)"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    bind.execute(
        sa.text(
            "UPDATE task_logs SET episode_id = :winner "
            "WHERE episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    # episode_tags: PK (episode_id, tag_id) — the winner already carries a
    # conflicting link, so drop the loser's duplicate.
    bind.execute(
        sa.text(
            "DELETE FROM episode_tags WHERE episode_id = :loser AND tag_id IN "
            "(SELECT tag_id FROM episode_tags WHERE episode_id = :winner)"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    bind.execute(
        sa.text(
            "UPDATE episode_tags SET episode_id = :winner "
            "WHERE episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    # playlist_episodes: PK (playlist_id, episode_id) — same treatment.
    bind.execute(
        sa.text(
            "DELETE FROM playlist_episodes WHERE episode_id = :loser "
            "AND playlist_id IN (SELECT playlist_id FROM playlist_episodes "
            "WHERE episode_id = :winner)"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    bind.execute(
        sa.text(
            "UPDATE playlist_episodes SET episode_id = :winner "
            "WHERE episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    )
    # user_episode_progress: PK (user_id, episode_id) — merge on conflict:
    # keep the furthest playback position and OR the completed flag.
    conflicts = bind.execute(
        sa.text(
            "SELECT l.user_id, l.position_seconds, l.completed, "
            "w.position_seconds, w.completed "
            "FROM user_episode_progress l "
            "JOIN user_episode_progress w "
            "ON w.user_id = l.user_id AND w.episode_id = :winner "
            "WHERE l.episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    ).fetchall()
    for user_id, l_pos, l_done, w_pos, w_done in conflicts:
        bind.execute(
            sa.text(
                "UPDATE user_episode_progress SET position_seconds = :pos, "
                "completed = :done WHERE user_id = :user_id "
                "AND episode_id = :winner"
            ),
            {
                "pos": max(l_pos or 0, w_pos or 0),
                "done": bool(l_done or w_done),
                "user_id": user_id,
                "winner": winner_id,
            },
        )
        bind.execute(
            sa.text(
                "DELETE FROM user_episode_progress "
                "WHERE user_id = :user_id AND episode_id = :loser"
            ),
            {"user_id": user_id, "loser": loser_id},
        )
    bind.execute(
        sa.text(
            "UPDATE user_episode_progress SET episode_id = :winner "
            "WHERE episode_id = :loser"
        ),
        {"winner": winner_id, "loser": loser_id},
    )


def _merge_feed_into(bind, dup_feed_id, survivor_feed_id) -> None:
    """Fold a duplicate feed row into the survivor feed row."""
    survivor_guids = {
        row[0]: (row[1], row[2])
        for row in bind.execute(
            sa.text(
                "SELECT guid, episode_id, created_at FROM episodes "
                "WHERE feed_id = :feed_id"
            ),
            {"feed_id": survivor_feed_id},
        ).fetchall()
    }
    dup_episodes = bind.execute(
        sa.text(
            "SELECT episode_id, guid, created_at FROM episodes "
            "WHERE feed_id = :feed_id"
        ),
        {"feed_id": dup_feed_id},
    ).fetchall()
    for episode_id, guid, created_at in dup_episodes:
        if guid in survivor_guids:
            # Same episode synced under both URL spellings: merge the two
            # rows, keeping the earliest-created episode.
            winner_id, winner_created = survivor_guids[guid]
            if _sort_key(created_at, episode_id) < _sort_key(
                winner_created, winner_id
            ):
                winner_id, winner_created, loser_id = (
                    episode_id,
                    created_at,
                    winner_id,
                )
            else:
                loser_id = winner_id
            _repoint_dependents(bind, loser_id, winner_id)
            bind.execute(
                sa.text("DELETE FROM episodes WHERE episode_id = :episode_id"),
                {"episode_id": loser_id},
            )
            # The merged-away row is gone; refresh the survivor mapping so
            # a later duplicate feed merging into the same survivor sees
            # the winner (with the winner's own created_at for the
            # earliest-wins comparison).
            survivor_guids[guid] = (winner_id, winner_created)
        else:
            bind.execute(
                sa.text(
                    "UPDATE episodes SET feed_id = :survivor "
                    "WHERE episode_id = :episode_id"
                ),
                {"survivor": survivor_feed_id, "episode_id": episode_id},
            )
            survivor_guids[guid] = (episode_id, created_at)
    # A merge winner can be the duplicate feed's own episode row — repoint
    # any episode still referencing the duplicate feed before deleting it,
    # otherwise the FK's ON DELETE CASCADE would take the winner (and the
    # dependents just repointed at it) down with the feed row.
    bind.execute(
        sa.text(
            "UPDATE episodes SET feed_id = :survivor WHERE feed_id = :dup"
        ),
        {"survivor": survivor_feed_id, "dup": dup_feed_id},
    )
    bind.execute(
        sa.text("DELETE FROM feeds WHERE feed_id = :feed_id"),
        {"feed_id": dup_feed_id},
    )


def _dedup_feeds(bind) -> None:
    """Canonicalize feed URLs and merge canonical-duplicate feed rows."""
    feeds = bind.execute(
        sa.text("SELECT feed_id, rss_url, created_at FROM feeds")
    ).fetchall()
    canonical_urls = {}
    groups: dict[str, list] = {}
    for feed_id, rss_url, created_at in feeds:
        try:
            canonical = _canonicalize_feed_url(rss_url)
        except ValueError:
            continue  # leave unparseable URLs untouched
        canonical_urls[feed_id] = canonical
        groups.setdefault(canonical, []).append(
            (feed_id, rss_url, created_at)
        )
    for _canonical, rows in groups.items():
        if len(rows) > 1:
            rows.sort(key=lambda r: _sort_key(r[2], r[0]))
            survivor_id = rows[0][0]
            for dup_id, _, _ in rows[1:]:
                _merge_feed_into(bind, dup_id, survivor_id)
    # Write the canonical URL back onto every surviving feed row.
    for feed_id, canonical in canonical_urls.items():
        bind.execute(
            sa.text(
                "UPDATE feeds SET rss_url = :canonical "
                "WHERE feed_id = :feed_id AND rss_url != :canonical"
            ),
            {"canonical": canonical, "feed_id": feed_id},
        )


def upgrade() -> None:
    """Upgrade schema."""
    # XIN-45: TaskLog outbox linkage + idempotency key.
    op.add_column("task_logs", sa.Column("episode_id", sa.Uuid(), nullable=True))
    op.create_index("ix_task_logs_episode_id", "task_logs", ["episode_id"])
    op.create_index(
        "uq_task_log_type_episode",
        "task_logs",
        ["task_type", "episode_id"],
        unique=True,
    )
    # XIN-44: canonicalize feed URLs and merge canonical duplicates.
    _dedup_feeds(op.get_bind())


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_task_log_type_episode", table_name="task_logs")
    op.drop_index("ix_task_logs_episode_id", table_name="task_logs")
    op.drop_column("task_logs", "episode_id")
