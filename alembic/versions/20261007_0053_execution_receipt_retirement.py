"""Fence retired execution epochs and remove redundant policy receipt digests.

Revision ID: 20261007_0053
Revises: 20261007_0052
"""

import sqlalchemy as sa

from alembic import op

revision = "20261007_0053"
down_revision = "20261007_0052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "execution_retired_streams",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(8), primary_key=True),
        sa.Column("stream_id", sa.String(256), primary_key=True),
        sa.Column("stream_epoch", sa.String(256), primary_key=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("receipts_archived_at", sa.DateTime(timezone=True)),
    )
    op.drop_column("durable_policy_commits", "prior_state_digest")
    op.drop_column("durable_policy_commits", "next_state_digest")


def downgrade() -> None:
    # Restoring old application code after retirement would admit archived
    # evidence again. Restoring the schema alone is not a safe rollback.
    raise RuntimeError("Execution retirement requires a forward-only rollback")
