"""tag NULL-category uniqueness via partial unique indexes (XIN-121)

Revision ID: 6ceb43cc6cf5
Revises: f3a8c1d2e4b5
Create Date: 2026-09-09

``uq_tag_name_category`` never enforced uniqueness for NULL categories:
both Postgres and SQLite treat NULLs as distinct in unique constraints,
so any number of ``('rust', NULL)`` rows could coexist. That made the
"concurrency backstop" claimed by ``TagRepository.get_or_create`` absent
for the common NULL-category case, and duplicate rows break
``list_tags_for_episode``'s one-row-per-(name, category) assumption.

This migration replaces the single unique constraint with two partial
unique indexes (supported by both Postgres and SQLite):

* ``uq_tag_name_null_category`` — ``UNIQUE(name) WHERE category IS NULL``
* ``uq_tag_name_category`` — ``UNIQUE(name, category) WHERE category IS NOT NULL``

The model (``backend/persistence/models/tag.py``) declares the same two
indexes so ``create_all`` test DBs agree with migrated DBs.

Note: if the database already contains duplicate ``('name', NULL)`` rows
(which the old constraint allowed), the upgrade fails loudly on the
``CREATE UNIQUE INDEX``. De-duplicate first in that case; the migration
deliberately does not pick winners.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6ceb43cc6cf5'
down_revision: Union[str, Sequence[str], None] = 'f3a8c1d2e4b5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _partial_index_args(where: str) -> dict:
    # Render the WHERE clause on both supported dialects.
    return {
        "sqlite_where": sa.text(where),
        "postgresql_where": sa.text(where),
    }


def upgrade() -> None:
    """Upgrade schema."""
    # SQLite cannot drop a constraint via ALTER; batch mode rewrites the
    # table without it, then creates the two partial indexes.
    with op.batch_alter_table('tags') as batch_op:
        batch_op.drop_constraint('uq_tag_name_category', type_='unique')
        batch_op.create_index(
            'uq_tag_name_null_category',
            ['name'],
            unique=True,
            **_partial_index_args('category IS NULL'),
        )
        batch_op.create_index(
            'uq_tag_name_category',
            ['name', 'category'],
            unique=True,
            **_partial_index_args('category IS NOT NULL'),
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('tags') as batch_op:
        batch_op.drop_index('uq_tag_name_category')
        batch_op.drop_index('uq_tag_name_null_category')
        batch_op.create_unique_constraint(
            'uq_tag_name_category', ['name', 'category']
        )
