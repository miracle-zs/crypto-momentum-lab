"""Index the bounded hot decision evidence retention window.

Revision ID: 20261008_0054
Revises: 20261007_0053
"""

from alembic import op

revision = "20261008_0054"
down_revision = "20261007_0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_decision_traces_created "
            "ON decision_traces (created_at, decision_id)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_decision_traces_created")
