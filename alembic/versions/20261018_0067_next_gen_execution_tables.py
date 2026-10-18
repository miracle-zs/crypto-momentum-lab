"""Create Next-Gen live_position_batches and live_orders execution tables.

Revision ID: 20261018_0067
Revises: 20261015_0066
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261018_0067"
down_revision: str | None = "20261015_0066"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. 活跃持仓批次快照表 (支持多账户隔离与同标的多批次重叠共存)
    op.create_table(
        "live_position_batches",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Numeric(28, 10), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("entry_bar_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_order_submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "realized_pnl", sa.Numeric(28, 10), nullable=False, server_default="0"
        ),
        sa.Column(
            "exiting_quantity", sa.Numeric(28, 10), nullable=False, server_default="0"
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("environment", "account_label", "batch_id"),
        sa.CheckConstraint("quantity >= 0", name="live_batches_quantity_non_negative"),
        sa.CheckConstraint(
            "exiting_quantity >= 0", name="live_batches_exiting_qty_non_negative"
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'EXITING', 'CLOSED')",
            name="live_batches_status_check",
        ),
    )
    op.create_index(
        "idx_live_batches_acc_sym_status",
        "live_position_batches",
        ["environment", "account_label", "symbol", "status"],
    )

    # 2. 统一订单生命周期表 (结构化列，逻辑关联与字符长度约束)
    op.create_table(
        "live_orders",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("exchange_order_id", sa.BigInteger(), nullable=True),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("position_side", sa.String(8), nullable=False, server_default="BOTH"),
        sa.Column("order_type", sa.String(16), nullable=False),
        sa.Column("quantity", sa.Numeric(28, 10), nullable=False),
        sa.Column("price", sa.Numeric(28, 10), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "filled_quantity", sa.Numeric(28, 10), nullable=False, server_default="0"
        ),
        sa.Column(
            "average_price", sa.Numeric(28, 10), nullable=False, server_default="0"
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("environment", "account_label", "client_order_id"),
        sa.CheckConstraint(
            "length(client_order_id) <= 36", name="live_orders_client_id_len_check"
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'SUBMITTED', 'OPEN', 'PARTIALLY_FILLED', "
            "'FILLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'RECONCILING')",
            name="live_orders_status_check",
        ),
    )
    op.create_index(
        "idx_live_orders_acc_sym_status",
        "live_orders",
        ["environment", "account_label", "symbol", "status"],
    )
    op.create_index(
        "idx_live_orders_acc_batch",
        "live_orders",
        ["environment", "account_label", "batch_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_live_orders_acc_batch", table_name="live_orders")
    op.drop_index("idx_live_orders_acc_sym_status", table_name="live_orders")
    op.drop_table("live_orders")
    op.drop_index("idx_live_batches_acc_sym_status", table_name="live_position_batches")
    op.drop_table("live_position_batches")
