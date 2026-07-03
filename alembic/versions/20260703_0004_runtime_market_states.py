from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260703_0004"
down_revision: str | None = "20260702_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "runtime_market_states_15s",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("bucket_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("high_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("low_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("close_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=False),
        sa.Column("trade_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column(
            "aggressive_buy_notional",
            sa.Numeric(38, 18),
            nullable=False,
        ),
        sa.Column(
            "aggressive_sell_notional",
            sa.Numeric(38, 18),
            nullable=False,
        ),
        sa.Column("last_bid_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("last_ask_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("spread", sa.Numeric(38, 18), nullable=True),
        sa.Column("midpoint", sa.Numeric(38, 18), nullable=True),
        sa.Column("liquidation_count", sa.Integer(), nullable=False),
        sa.Column("liquidation_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("mark_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("closed_kline_count", sa.Integer(), nullable=False),
        sa.Column("source_event_count", sa.Integer(), nullable=False),
        sa.Column("first_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_watermark_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closure_reason", sa.String(32), nullable=False),
        sa.Column("input_sequence_min", sa.Integer(), nullable=True),
        sa.Column("input_sequence_max", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint(
            "environment",
            "symbol",
            "bucket_start",
            name="pk_runtime_market_states_15s",
        ),
    )
    op.create_index(
        "ix_runtime_market_states_15s_polling",
        "runtime_market_states_15s",
        ["environment", "bucket_start", "symbol"],
    )
    op.create_index(
        "ix_runtime_market_states_15s_symbol_time",
        "runtime_market_states_15s",
        ["environment", "symbol", "bucket_start"],
    )
    op.create_index(
        "ix_runtime_market_states_15s_created",
        "runtime_market_states_15s",
        ["environment", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_runtime_market_states_15s_created",
        table_name="runtime_market_states_15s",
    )
    op.drop_index(
        "ix_runtime_market_states_15s_symbol_time",
        table_name="runtime_market_states_15s",
    )
    op.drop_index(
        "ix_runtime_market_states_15s_polling",
        table_name="runtime_market_states_15s",
    )
    op.drop_table("runtime_market_states_15s")
