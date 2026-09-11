"""hot-path indexes for the ingestion queries (XIN-46)

Revision ID: 8d1e2f3a4b5c
Revises: 742ddc0a7799
Create Date: 2026-09-11

``sync_all_pending_feeds``, ``batch_runner``, and the crawler all filter
``Feed`` by ``sync_status``, which had no index, and
``get_unprocessed_episodes`` filters ``Episode`` by ``processed``
(+ optional ``feed_id``), which had no index either (only ``published_at``
and ``feed_id`` were indexed). As feeds/episodes grow these become full
table scans on every batch run.

This migration adds:

* ``ix_feeds_sync_status`` — btree index on ``feeds.sync_status``
* ``ix_episodes_feed_processed`` — composite index on
  ``episodes(feed_id, processed)``

The models (``backend/persistence/models/feed.py``,
``backend/persistence/models/episode.py``) declare the same indexes so
``create_all`` test DBs agree with migrated DBs, mirroring the XIN-121
migration's convention.

The issue also floats an index on (TaskLog.status, TaskLog.created_at);
task_log.py is owned by a different cleanup batch, so that is left for it.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '8d1e2f3a4b5c'
down_revision: Union[str, Sequence[str], None] = '742ddc0a7799'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('feeds') as batch_op:
        batch_op.create_index('ix_feeds_sync_status', ['sync_status'])
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.create_index(
            'ix_episodes_feed_processed', ['feed_id', 'processed']
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.drop_index('ix_episodes_feed_processed')
    with op.batch_alter_table('feeds') as batch_op:
        batch_op.drop_index('ix_feeds_sync_status')
