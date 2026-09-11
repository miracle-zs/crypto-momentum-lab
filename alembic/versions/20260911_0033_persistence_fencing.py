"""Persist runtime gaps and add monotonic persistence fences."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260911_0033"
down_revision: str | None = "20260910_0032"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "runtime_market_state_gaps",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("previous_id", sa.BigInteger(), nullable=False),
        sa.Column("current_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "previous_event_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "current_event_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "first_bucket_start",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "last_bucket_start",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("missing_count", sa.Integer(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "missing_count > 0",
            name="runtime_market_state_gap_positive",
        ),
        sa.PrimaryKeyConstraint(
            "environment",
            "symbol",
            "previous_id",
            "current_id",
            name="pk_runtime_market_state_gaps",
        ),
    )
    op.create_index(
        "ix_runtime_market_state_gaps_bucket",
        "runtime_market_state_gaps",
        [
            "environment",
            "symbol",
            "first_bucket_start",
            "last_bucket_start",
        ],
    )
    op.create_index(
        "uq_exchange_fills_client_trade",
        "exchange_fills",
        ["client_order_id", "exchange_trade_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "uq_exchange_fills_client_trade",
        table_name="exchange_fills",
    )
    op.drop_index(
        "ix_runtime_market_state_gaps_bucket",
        table_name="runtime_market_state_gaps",
    )
    op.drop_table("runtime_market_state_gaps")
