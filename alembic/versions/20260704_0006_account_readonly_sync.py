from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0006"
down_revision: str | None = "20260704_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "account_balance_snapshots",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("wallet_balance", sa.Numeric(38, 18), nullable=False),
        sa.Column("available_balance", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", name="pk_account_balance_snapshots"),
    )
    op.create_index(
        "ix_account_balance_latest",
        "account_balance_snapshots",
        ["environment", "account_label", "asset", "observed_at"],
    )
    op.create_table(
        "account_position_snapshots",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(16), nullable=False),
        sa.Column("position_amt", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("mark_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column("margin_type", sa.String(32), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", name="pk_account_position_snapshots"),
    )
    op.create_index(
        "ix_account_position_latest",
        "account_position_snapshots",
        ["environment", "account_label", "symbol", "observed_at"],
    )
    op.create_table(
        "account_open_orders",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("order_id", sa.String(64), nullable=False),
        sa.Column("client_order_id", sa.String(128), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("order_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("original_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("executed_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            "symbol",
            "order_id",
            name="pk_account_open_orders",
        ),
    )
    op.create_table(
        "account_fill_events",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=False),
        sa.Column("order_id", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee_asset", sa.String(32), nullable=False),
        sa.Column("trade_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            "symbol",
            "trade_id",
            name="pk_account_fill_events",
        ),
    )
    op.create_table(
        "account_config_snapshots",
        sa.Column("snapshot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("multi_assets_mode", sa.Boolean(), nullable=False),
        sa.Column("can_trade", sa.Boolean(), nullable=False),
        sa.Column("fee_tier", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_id", name="pk_account_config_snapshots"),
    )
    op.create_table(
        "account_reconciliation_runs",
        sa.Column("reconciliation_id", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("balance_count", sa.Integer(), nullable=False),
        sa.Column("position_count", sa.Integer(), nullable=False),
        sa.Column("open_order_count", sa.Integer(), nullable=False),
        sa.Column("fill_count", sa.Integer(), nullable=False),
        sa.Column("mismatch_count", sa.Integer(), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "reconciliation_id",
            name="pk_account_reconciliation_runs",
        ),
    )
    op.create_table(
        "execution_account_process_states",
        sa.Column("state_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint(
            "state_id",
            name="pk_execution_account_process_states",
        ),
    )


def downgrade() -> None:
    op.drop_table("execution_account_process_states")
    op.drop_table("account_reconciliation_runs")
    op.drop_table("account_config_snapshots")
    op.drop_table("account_fill_events")
    op.drop_table("account_open_orders")
    op.drop_index(
        "ix_account_position_latest",
        table_name="account_position_snapshots",
    )
    op.drop_table("account_position_snapshots")
    op.drop_index(
        "ix_account_balance_latest",
        table_name="account_balance_snapshots",
    )
    op.drop_table("account_balance_snapshots")
