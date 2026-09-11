"""episode guid NULL backfill + NOT NULL (XIN-68)

Revision ID: 0f3a4b5c6d7e
Revises: 9e2f3a4b5c6d
Create Date: 2026-09-11

``episodes.guid`` was nullable, which defeats the ``(feed_id, guid)``
dedup the ingestion relies on: both Postgres and SQLite treat NULLs as
distinct in unique constraints, so unlimited ``(feed_id, NULL)``
duplicates could coexist, and the service's ``known_guids`` set filters
NULLs out, so a NULL-guid episode would be re-inserted on every sync.

The upgrade backfills every NULL guid with a deterministic fallback,
then enforces NOT NULL:

* seed = ``audio_url`` when non-empty, else ``title`` + ``published_at``,
  else the row's own ``episode_id`` (always unique)
* fallback = ``"tunedin-fallback-" + sha1(feed_id + "|" + seed)``

The ``tunedin-fallback-`` prefix marks synthesized guids as such; the
feed_id salt keeps the fallback stable per feed. The backfill is
idempotent: re-running it computes the same value for the same row.

Note: if the database contains two NULL-guid rows in the same feed with
identical seeds (true duplicates the old schema allowed), the upgrade
fails loudly on the ``uq_episode_feed_guid`` constraint. De-duplicate
first in that case, mirroring the XIN-121 migration's convention — the
migration deliberately does not pick winners.

The parser already guarantees a non-empty guid (and its
``audio_url or ""`` contract keeps audio_url non-null), so new rows never
hit this path; the backfill exists only for legacy rows. The model
(``backend/persistence/models/episode.py``) declares guid as
non-nullable so ``create_all`` test DBs agree with migrated DBs.

Downgrade restores nullability. The backfilled guids are left in place
(they remain valid dedup keys); the downgrade does not attempt to
recover which rows were originally NULL.
"""

import hashlib
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0f3a4b5c6d7e'
down_revision: Union[str, Sequence[str], None] = '9e2f3a4b5c6d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

FALLBACK_PREFIX = "tunedin-fallback-"


def _fallback_guid(
    feed_id, episode_id, audio_url, title, published_at
) -> str:
    """Deterministic guid for a legacy NULL-guid episode row.

    Prefers the episode's natural identity (audio_url, then title +
    published_at); falls back to the row's own episode_id so the result is
    always unique within the feed.
    """
    if audio_url:
        seed = audio_url
    elif title or published_at:
        seed = f"{title or ''}|{published_at or ''}"
    else:
        seed = str(episode_id)
    digest = hashlib.sha1(
        f"{feed_id}|{seed}".encode("utf-8")
    ).hexdigest()
    return f"{FALLBACK_PREFIX}{digest}"


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    null_guid_rows = bind.execute(
        sa.text(
            "SELECT episode_id, feed_id, audio_url, title, published_at "
            "FROM episodes WHERE guid IS NULL"
        )
    ).fetchall()
    for episode_id, feed_id, audio_url, title, published_at in null_guid_rows:
        fallback = _fallback_guid(
            feed_id, episode_id, audio_url, title, published_at
        )
        bind.execute(
            sa.text(
                "UPDATE episodes SET guid = :guid "
                "WHERE episode_id = :episode_id"
            ),
            {"guid": fallback, "episode_id": episode_id},
        )
    # SQLite cannot ALTER nullability in place; batch mode rewrites the
    # table (preserving the uq_episode_feed_guid constraint).
    with op.batch_alter_table("episodes") as batch_op:
        batch_op.alter_column(
            "guid",
            existing_type=sa.String(512),
            existing_nullable=True,
            nullable=False,
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("episodes") as batch_op:
        batch_op.alter_column(
            "guid",
            existing_type=sa.String(512),
            existing_nullable=False,
            nullable=True,
        )
