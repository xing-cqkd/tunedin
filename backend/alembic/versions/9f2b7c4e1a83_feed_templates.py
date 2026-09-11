"""feed templates + drift decisions (XIN-136)

Revision ID: 9f2b7c4e1a83
Revises: 6243728f33d1
Create Date: 2026-09-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9f2b7c4e1a83'
down_revision: Union[str, Sequence[str], None] = '2529fed59a29'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('feed_templates',
    sa.Column('template_id', sa.Uuid(), nullable=False),
    sa.Column('feed_id', sa.Uuid(), nullable=False),
    sa.Column('episode_type', sa.String(length=32), nullable=False),
    sa.Column('rev', sa.Integer(), nullable=False),
    sa.Column('labeler_version', sa.String(length=64), nullable=False),
    sa.Column('template_json', sa.Text(), nullable=False),
    sa.Column('confidence', sa.Float(), nullable=False),
    sa.Column('learned_from', sa.JSON(), nullable=False),
    sa.Column('notes', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['feed_id'], ['feeds.feed_id'], ),
    sa.PrimaryKeyConstraint('template_id'),
    sa.UniqueConstraint('feed_id', 'episode_type', 'rev', name='uq_feed_template_rev')
    )
    op.create_index(op.f('ix_feed_templates_feed_id'), 'feed_templates', ['feed_id'], unique=False)
    op.create_table('drift_decisions',
    sa.Column('decision_id', sa.Uuid(), nullable=False),
    sa.Column('feed_id', sa.Uuid(), nullable=False),
    sa.Column('episode_type', sa.String(length=32), nullable=False),
    sa.Column('template_rev', sa.Integer(), nullable=False),
    sa.Column('decision', sa.String(length=32), nullable=False),
    sa.Column('rationale', sa.Text(), nullable=False),
    sa.Column('decided_by', sa.String(length=64), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['feed_id'], ['feeds.feed_id'], ),
    sa.PrimaryKeyConstraint('decision_id')
    )
    op.create_index(op.f('ix_drift_decisions_feed_id'), 'drift_decisions', ['feed_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_drift_decisions_feed_id'), table_name='drift_decisions')
    op.drop_table('drift_decisions')
    op.drop_index(op.f('ix_feed_templates_feed_id'), table_name='feed_templates')
    op.drop_table('feed_templates')
