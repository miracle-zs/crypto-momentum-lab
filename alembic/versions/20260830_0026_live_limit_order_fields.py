"""Persist live limit-order expiry and cumulative execution state."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260830_0026"
down_revision: str | None = "20260829_0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "exchange_orders",
        sa.Column("time_in_force", sa.String(length=8), nullable=True),
    )
    op.add_column(
        "exchange_orders",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "exchange_orders",
        sa.Column(
            "executed_quantity",
            sa.Numeric(precision=38, scale=18),
            nullable=False,
            server_default="0",
        ),
    )
    op.create_index(
        "ix_account_fill_order_time",
        "account_fill_events",
        ["environment", "account_label", "order_id", "trade_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_account_fill_order_time",
        table_name="account_fill_events",
    )
    op.drop_column("exchange_orders", "executed_quantity")
    op.drop_column("exchange_orders", "expires_at")
    op.drop_column("exchange_orders", "time_in_force")
