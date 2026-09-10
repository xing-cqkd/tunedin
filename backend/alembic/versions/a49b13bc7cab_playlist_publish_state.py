"""playlist publish state (XIN-97)

Revision ID: a49b13bc7cab
Revises: 6243728f33d1
Create Date: 2026-09-09

Adds publish state to ``curated_playlists``: ``visibility`` ('unlisted' /
'public', default 'unlisted'), unique nullable ``slug``, nullable
``token`` (256-bit URL-safe secret), ``token_revoked_at``, and
``frozen_at``. Existing rows backfill to ``visibility='unlisted'`` via
the server default; ``slug`` stays NULL until the playlist is published.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a49b13bc7cab'
down_revision: Union[str, Sequence[str], None] = '6243728f33d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'curated_playlists',
        sa.Column(
            'visibility',
            sa.String(length=20),
            nullable=False,
            server_default='unlisted',
        ),
    )
    op.add_column(
        'curated_playlists',
        sa.Column('slug', sa.String(length=255), nullable=True),
    )
    op.add_column(
        'curated_playlists',
        sa.Column('token', sa.String(length=128), nullable=True),
    )
    op.add_column(
        'curated_playlists',
        sa.Column('token_revoked_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        'curated_playlists',
        sa.Column('frozen_at', sa.DateTime(timezone=True), nullable=True),
    )
    # SQLite cannot ALTER constraints; batch mode rewrites the table.
    with op.batch_alter_table('curated_playlists') as batch_op:
        batch_op.create_unique_constraint(
            'uq_curated_playlists_slug', ['slug']
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('curated_playlists') as batch_op:
        batch_op.drop_constraint('uq_curated_playlists_slug', type_='unique')
    op.drop_column('curated_playlists', 'frozen_at')
    op.drop_column('curated_playlists', 'token_revoked_at')
    op.drop_column('curated_playlists', 'token')
    op.drop_column('curated_playlists', 'slug')
    op.drop_column('curated_playlists', 'visibility')
