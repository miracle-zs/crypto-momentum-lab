from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0007"
down_revision: str | None = "20260704_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "trading_leases",
        sa.Column("lease_id", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("owner", sa.String(128), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("lease_id", name="pk_trading_leases"),
    )
    op.create_index(
        "uq_trading_leases_active_account",
        "trading_leases",
        ["environment", "account_label"],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )
    op.create_index(
        "ix_trading_leases_account_expiry",
        "trading_leases",
        ["environment", "account_label", "expires_at"],
    )
    op.create_table(
        "risk_config_snapshots",
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("max_order_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("max_gross_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("max_daily_loss", sa.Numeric(38, 18), nullable=False),
        sa.Column("max_open_positions", sa.Integer(), nullable=False),
        sa.Column(
            "max_market_state_age_seconds", sa.Numeric(18, 6), nullable=False
        ),
        sa.Column(
            "max_account_state_age_seconds", sa.Numeric(18, 6), nullable=False
        ),
        sa.Column(
            "allow_reduce_only_while_draining", sa.Boolean(), nullable=False
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("config_hash", name="pk_risk_config_snapshots"),
    )
    op.create_table(
        "risk_evaluations",
        sa.Column("evaluation_id", sa.String(128), nullable=False),
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("evaluation_id", name="pk_risk_evaluations"),
    )
    op.create_index(
        "ix_risk_evaluations_candidate_time",
        "risk_evaluations",
        ["candidate_id", "evaluated_at"],
    )
    op.create_table(
        "risk_rejections",
        sa.Column("evaluation_id", sa.String(128), nullable=False),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["evaluation_id"],
            ["risk_evaluations.evaluation_id"],
            name="fk_risk_rejections_evaluation_id_risk_evaluations",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("evaluation_id", name="pk_risk_rejections"),
    )
    op.create_table(
        "risk_halts",
        sa.Column("halt_id", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("halt_id", name="pk_risk_halts"),
    )
    op.create_index(
        "ix_risk_halts_active_account",
        "risk_halts",
        ["environment", "account_label", "active"],
    )
    op.create_table(
        "strategy_live_states",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            "strategy_name",
            name="pk_strategy_live_states",
        ),
    )


def downgrade() -> None:
    op.drop_table("strategy_live_states")
    op.drop_index("ix_risk_halts_active_account", table_name="risk_halts")
    op.drop_table("risk_halts")
    op.drop_table("risk_rejections")
    op.drop_index(
        "ix_risk_evaluations_candidate_time",
        table_name="risk_evaluations",
    )
    op.drop_table("risk_evaluations")
    op.drop_table("risk_config_snapshots")
    op.drop_index(
        "ix_trading_leases_account_expiry",
        table_name="trading_leases",
    )
    op.drop_index(
        "uq_trading_leases_active_account",
        table_name="trading_leases",
    )
    op.drop_table("trading_leases")
