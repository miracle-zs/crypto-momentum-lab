"""Bind operator commands to a durable recovery claim and action identity."""

import sqlalchemy as sa

from alembic import op

revision = "20261013_0058"
down_revision = "20261013_0057"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "live_operator_action_claims",
        sa.Column("command_id", sa.String(128), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("action_kind", sa.String(64), nullable=False),
        sa.Column("action_identity_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("holder_id", sa.String(192), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_live_operator_action_claims_scope",
        "live_operator_action_claims",
        ["environment", "account_label", "strategy_name", "claimed_at"],
    )


def downgrade():
    op.drop_index(
        "ix_live_operator_action_claims_scope",
        table_name="live_operator_action_claims",
    )
    op.drop_table("live_operator_action_claims")
