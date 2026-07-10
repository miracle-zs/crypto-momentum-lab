from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0010"
down_revision: str | None = "20260704_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "live_operator_approvals",
        sa.Column("approval_id", sa.String(128), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_config_hash", sa.String(64), nullable=False),
        sa.Column("risk_config_hash", sa.String(64), nullable=False),
        sa.Column("git_commit_hash", sa.String(64), nullable=False),
        sa.Column("database_migration_revision", sa.String(32), nullable=False),
        sa.Column("approved_notional_cap", sa.Numeric(38, 18), nullable=False),
        sa.Column("approved_max_open_positions", sa.Integer(), nullable=False),
        sa.Column("approved_max_daily_loss", sa.Numeric(38, 18), nullable=False),
        sa.Column("approver_name", sa.String(128), nullable=False),
        sa.Column("approval_text", sa.String(128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("approval_id", name="pk_live_operator_approvals"),
    )
    op.create_index(
        "ix_live_approvals_account_strategy_expiry",
        "live_operator_approvals",
        ["account_label", "strategy_name", "expires_at"],
    )
    op.create_table(
        "live_session_transitions",
        sa.Column("transition_id", sa.String(128), nullable=False),
        sa.Column("session_id", sa.String(128), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operator", sa.String(128), nullable=False),
        sa.Column("strategy_config_hash", sa.String(64), nullable=False),
        sa.Column("risk_config_hash", sa.String(64), nullable=False),
        sa.Column("reason", sa.String(128), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("transition_id", name="pk_live_session_transitions"),
    )
    op.create_index(
        "ix_live_session_transitions_session_time",
        "live_session_transitions",
        ["session_id", "occurred_at"],
    )
    op.create_table(
        "live_rollback_commands",
        sa.Column("command_id", sa.String(128), nullable=False),
        sa.Column("command_type", sa.String(64), nullable=False),
        sa.Column("requested_by", sa.String(128), nullable=False),
        sa.Column("confirmation_text", sa.String(128), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("session_id", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("command_id", name="pk_live_rollback_commands"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_live_rollback_commands_idempotency_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("live_rollback_commands")
    op.drop_index(
        "ix_live_session_transitions_session_time",
        table_name="live_session_transitions",
    )
    op.drop_table("live_session_transitions")
    op.drop_index(
        "ix_live_approvals_account_strategy_expiry",
        table_name="live_operator_approvals",
    )
    op.drop_table("live_operator_approvals")
