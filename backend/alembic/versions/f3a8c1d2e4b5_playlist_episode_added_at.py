"""playlist_episode added_at (XIN-98)

Revision ID: f3a8c1d2e4b5
Revises: a49b13bc7cab
Create Date: 2026-09-09

Adds ``added_at`` to ``playlist_episodes`` — the date an episode was added
to a playlist. XIN-98's RSS endpoint uses it as the item ``<pubDate>`` so
podcatcher "new" badges fire when a curator adds episodes.

Existing rows backfill to the migration run time (SQLite has no
timezone-aware ``now()``; the column stays nullable at the DB level and the
model's Python-side default enforces non-null for new rows).
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f3a8c1d2e4b5'
down_revision: Union[str, Sequence[str], None] = 'a49b13bc7cab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'playlist_episodes',
        sa.Column('added_at', sa.DateTime(timezone=True), nullable=True),
    )
    # Backfill pre-existing links to the migration time; new rows get the
    # Python-side default from the model.
    op.execute(
        "UPDATE playlist_episodes SET added_at = CURRENT_TIMESTAMP "
        "WHERE added_at IS NULL"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('playlist_episodes', 'added_at')
