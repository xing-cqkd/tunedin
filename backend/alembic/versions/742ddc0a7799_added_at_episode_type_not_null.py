"""align NOT NULL columns with the models (XIN-122)

Revision ID: 742ddc0a7799
Revises: 6ceb43cc6cf5
Create Date: 2026-09-09

Two columns were declared non-nullable in the models but nullable in the
migrated schema:

* ``playlist_episodes.added_at`` — the model says ``nullable=False`` (with
  a Python-side default), but migration ``f3a8c1d2e4b5`` added the column
  as nullable. Made NOT NULL with a ``CURRENT_TIMESTAMP`` server default
  (SQLite and Postgres both accept it).
* ``episodes.episode_type`` — the model pairs ``default="full"`` with
  ``nullable=True`` (contradictory); the write path coerces an unset/None
  value to ``"full"`` (parity with the DynamoDB backend's
  ``codec.apply_defaults``), so the column is now NOT NULL with
  ``default``/``server_default`` ``"full"``. Pre-existing NULLs are
  backfilled to ``'full'``.

The models declare the matching ``nullable=False`` / ``server_default``,
so ``create_all`` test DBs agree with migrated DBs.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '742ddc0a7799'
down_revision: Union[str, Sequence[str], None] = '6ceb43cc6cf5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Defensive backfills: the f3a8c1d2e4b5 migration backfilled added_at,
    # but an interrupted run could have left NULLs; episode_type NULLs are
    # pre-existing data from the old nullable declaration.
    op.execute(
        "UPDATE playlist_episodes SET added_at = CURRENT_TIMESTAMP "
        "WHERE added_at IS NULL"
    )
    op.execute(
        "UPDATE episodes SET episode_type = 'full' "
        "WHERE episode_type IS NULL"
    )
    # SQLite cannot ALTER a column's nullability; batch mode rewrites the
    # tables.
    with op.batch_alter_table('playlist_episodes') as batch_op:
        batch_op.alter_column(
            'added_at',
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('CURRENT_TIMESTAMP'),
        )
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.alter_column(
            'episode_type',
            existing_type=sa.String(length=50),
            nullable=False,
            server_default='full',
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('episodes') as batch_op:
        batch_op.alter_column(
            'episode_type',
            existing_type=sa.String(length=50),
            nullable=True,
            server_default=None,
        )
    with op.batch_alter_table('playlist_episodes') as batch_op:
        batch_op.alter_column(
            'added_at',
            existing_type=sa.DateTime(timezone=True),
            nullable=True,
            server_default=None,
        )
