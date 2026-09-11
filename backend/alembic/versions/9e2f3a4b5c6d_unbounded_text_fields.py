"""unbounded Text for title/URL columns (XIN-47)

Revision ID: 9e2f3a4b5c6d
Revises: 8d1e2f3a4b5c
Create Date: 2026-09-11

``Episode.title`` (String(512)), ``Episode.audio_url`` (String(1024)),
``Feed.rss_url`` (String(1024)), and ``Feed.title`` (String(512)) were hard
caps. Postgres raises ``DataError`` on overflow — no silent truncation —
and real-world podcast titles and enclosure URLs routinely exceed these
limits. One overlong title could fail an entire per-feed episode commit,
marking the feed ``error``.

This migration widens all four columns to ``Text`` (unbounded on both
Postgres and SQLite). The models declare the same types so ``create_all``
test DBs agree with migrated DBs.

No data changes: narrowing to Text never truncates, so the upgrade is
lossless. The downgrade reinstates the old caps; values that grew past
them would fail on Postgres, so the downgrade is documented as
lossy-in-theory and offered only for schema-shape rollback.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9e2f3a4b5c6d'
down_revision: Union[str, Sequence[str], None] = '8d1e2f3a4b5c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # SQLite cannot ALTER COLUMN type; batch mode rewrites the tables.
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.alter_column(
            'title',
            existing_type=sa.String(512),
            type_=sa.Text(),
            existing_nullable=False,
        )
        batch_op.alter_column(
            'audio_url',
            existing_type=sa.String(1024),
            type_=sa.Text(),
            existing_nullable=False,
        )
    with op.batch_alter_table('feeds') as batch_op:
        batch_op.alter_column(
            'rss_url',
            existing_type=sa.String(1024),
            type_=sa.Text(),
            existing_nullable=False,
        )
        batch_op.alter_column(
            'title',
            existing_type=sa.String(512),
            type_=sa.Text(),
            existing_nullable=False,
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('feeds') as batch_op:
        batch_op.alter_column(
            'title',
            existing_type=sa.Text(),
            type_=sa.String(512),
            existing_nullable=False,
        )
        batch_op.alter_column(
            'rss_url',
            existing_type=sa.Text(),
            type_=sa.String(1024),
            existing_nullable=False,
        )
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.alter_column(
            'audio_url',
            existing_type=sa.Text(),
            type_=sa.String(1024),
            existing_nullable=False,
        )
        batch_op.alter_column(
            'title',
            existing_type=sa.Text(),
            type_=sa.String(512),
            existing_nullable=False,
        )
