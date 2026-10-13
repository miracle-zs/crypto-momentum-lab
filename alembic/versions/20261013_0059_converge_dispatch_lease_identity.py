"""Converge dispatch ownership onto the account lease itself.

The per-process ``holder_id`` binding and the separate operator-action claim
table duplicated state that the account lease and the existing operator command
record already carry.  This migration removes them: the unique active
``trading_leases`` row is the dispatch identity, and the operator command's
``executing`` status plus its stable ``command_id`` is the recovery record.
"""

import sqlalchemy as sa

from alembic import op

revision = "20261013_0059"
down_revision = "20261013_0058"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout='5s'")
    op.drop_index(
        "ix_live_operator_action_claims_scope",
        table_name="live_operator_action_claims",
    )
    op.drop_table("live_operator_action_claims")
    op.drop_column("trading_leases", "holder_id")


def downgrade():
    op.add_column(
        "trading_leases",
        sa.Column("holder_id", sa.String(192), nullable=True),
    )
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
