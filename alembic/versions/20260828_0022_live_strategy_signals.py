"""Persist rich live strategy-signal observations."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260828_0022"
down_revision: str | None = "20260824_0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "live_strategy_signals",
        sa.Column("observation_id", sa.String(192), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("candidate_id", sa.String(128), nullable=True),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("code_commit", sa.String(64), nullable=False),
        sa.Column("signal_kind", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "source_state_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column(
            "quote_volume_24h",
            sa.Numeric(38, 18),
            nullable=True,
        ),
        sa.Column(
            "quote_volume_24h_quote_asset",
            sa.String(16),
            nullable=True,
        ),
        sa.Column("quote_volume_24h_source", sa.String(64), nullable=True),
        sa.Column(
            "quote_volume_24h_source_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "quote_volume_24h_fetched_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("quote_volume_24h_age_ms", sa.Integer(), nullable=True),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.Column("reference_prices", postgresql.JSONB(), nullable=False),
        sa.Column("market_context", postgresql.JSONB(), nullable=False),
        sa.Column("filter_context", postgresql.JSONB(), nullable=False),
        sa.Column("candidate_context", postgresql.JSONB(), nullable=False),
        sa.Column("account_context", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "observation_id",
            name="pk_live_strategy_signals",
        ),
    )
    op.create_index(
        "ix_live_strategy_signals_run_time_symbol",
        "live_strategy_signals",
        ["run_id", "detected_at", "symbol"],
    )
    op.create_index(
        "ix_live_strategy_signals_symbol_time",
        "live_strategy_signals",
        ["symbol", "detected_at"],
    )
    op.create_index(
        "ix_live_strategy_signals_signal_id",
        "live_strategy_signals",
        ["signal_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_live_strategy_signals_signal_id",
        table_name="live_strategy_signals",
    )
    op.drop_index(
        "ix_live_strategy_signals_symbol_time",
        table_name="live_strategy_signals",
    )
    op.drop_index(
        "ix_live_strategy_signals_run_time_symbol",
        table_name="live_strategy_signals",
    )
    op.drop_table("live_strategy_signals")
