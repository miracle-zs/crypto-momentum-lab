from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0013"
down_revision: str | None = "20260801_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "exchange_orders",
        sa.Column(
            "position_side",
            sa.String(length=8),
            nullable=False,
            server_default="BOTH",
        ),
    )
    op.add_column(
        "account_config_snapshots",
        sa.Column(
            "hedge_mode",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_index(
        "ix_account_balance_account_observed",
        "account_balance_snapshots",
        ["environment", "account_label", "observed_at"],
    )
    op.create_index(
        "ix_account_position_account_observed",
        "account_position_snapshots",
        ["environment", "account_label", "observed_at"],
    )
    op.create_index(
        "ix_account_fill_account_trade",
        "account_fill_events",
        ["environment", "account_label", "trade_at"],
    )
    op.create_index(
        "ix_account_reconciliation_latest",
        "account_reconciliation_runs",
        ["environment", "account_label", "status", "observed_at"],
    )
    op.create_index(
        "ix_execution_account_state_latest",
        "execution_account_process_states",
        ["environment", "account_label", "occurred_at"],
    )
    op.create_index(
        "ix_exchange_orders_run_updated",
        "exchange_orders",
        ["run_id", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_exchange_orders_run_updated", table_name="exchange_orders")
    op.drop_index(
        "ix_execution_account_state_latest",
        table_name="execution_account_process_states",
    )
    op.drop_index(
        "ix_account_reconciliation_latest",
        table_name="account_reconciliation_runs",
    )
    op.drop_index(
        "ix_account_fill_account_trade",
        table_name="account_fill_events",
    )
    op.drop_index(
        "ix_account_balance_account_observed",
        table_name="account_balance_snapshots",
    )
    op.drop_index(
        "ix_account_position_account_observed",
        table_name="account_position_snapshots",
    )
    op.drop_column("account_config_snapshots", "hedge_mode")
    op.drop_column("exchange_orders", "position_side")
