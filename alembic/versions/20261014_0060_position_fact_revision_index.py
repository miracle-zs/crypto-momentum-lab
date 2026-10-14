"""Index journal suffix proofs by scoped source revision.

Revision ID: 20261014_0060
Revises: 20261013_0059
"""

from alembic import op

revision = "20261014_0060"
down_revision = "20261013_0059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_position_fact_events_scope_revision "
            "ON position_fact_journal_events ("
            "environment, account_label, symbol, position_side, stream_id, "
            "stream_epoch, source_revision"
            ") INCLUDE (occurred_at)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_position_fact_events_scope_revision"
        )
