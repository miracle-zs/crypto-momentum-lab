from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260702_0003"
down_revision: str | None = "20260615_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_runs",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("run_mode", sa.String(16), nullable=False),
        sa.Column("code_commit", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("source_paths", postgresql.JSONB(), nullable=False),
        sa.Column("source_description", sa.Text(), nullable=False),
        sa.Column("execution_config", postgresql.JSONB(), nullable=False),
        sa.Column("input_state_count", sa.Integer(), nullable=False),
        sa.Column("processed_symbol_count", sa.Integer(), nullable=False),
        sa.Column("signal_count", sa.Integer(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("fill_count", sa.Integer(), nullable=False),
        sa.Column("pending_candidate_count", sa.Integer(), nullable=False),
        sa.Column("rejection_summary", postgresql.JSONB(), nullable=False),
        sa.Column("summary_counts", postgresql.JSONB(), nullable=False),
        sa.Column("fill_summary", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("run_id", name="pk_strategy_runs"),
    )
    op.create_table(
        "strategy_signals",
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_state_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.Column("reference_prices", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_strategy_signals_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("signal_id", name="pk_strategy_signals"),
    )
    op.create_index(
        "ix_strategy_signals_run_time_symbol",
        "strategy_signals",
        ["run_id", "detected_at", "symbol"],
    )
    op.create_index(
        "ix_strategy_signals_run_symbol",
        "strategy_signals",
        ["run_id", "symbol"],
    )
    op.create_table(
        "order_intent_candidates",
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("entry_type", sa.String(16), nullable=False),
        sa.Column("limit_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("desired_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_order_intent_candidates_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["strategy_signals.signal_id"],
            name="fk_order_intent_candidates_signal_id_strategy_signals",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "candidate_id",
            name="pk_order_intent_candidates",
        ),
    )
    op.create_index(
        "ix_order_intent_candidates_run_created_symbol",
        "order_intent_candidates",
        ["run_id", "created_at", "symbol"],
    )
    op.create_index(
        "ix_order_intent_candidates_run_symbol",
        "order_intent_candidates",
        ["run_id", "symbol"],
    )
    op.create_table(
        "paper_fills",
        sa.Column("fill_id", sa.String(128), nullable=False),
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("target_fill_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("requested_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("filled_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=True),
        sa.Column("reference_midpoint", sa.Numeric(38, 18), nullable=True),
        sa.Column("spread", sa.Numeric(38, 18), nullable=True),
        sa.Column("fill_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_cost", sa.Numeric(38, 18), nullable=False),
        sa.Column("cost_bps", sa.Numeric(38, 18), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["order_intent_candidates.candidate_id"],
            name="fk_paper_fills_candidate_id_order_intent_candidates",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_paper_fills_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["strategy_signals.signal_id"],
            name="fk_paper_fills_signal_id_strategy_signals",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("fill_id", name="pk_paper_fills"),
    )
    op.create_index(
        "ix_paper_fills_run_target_symbol",
        "paper_fills",
        ["run_id", "target_fill_at", "symbol"],
    )
    op.create_index(
        "ix_paper_fills_run_status",
        "paper_fills",
        ["run_id", "status"],
    )
    op.create_table(
        "strategy_checkpoints",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column(
            "last_processed_at_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "warmup_buckets_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "cooldown_buckets_remaining_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("saved_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_strategy_checkpoints_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_strategy_checkpoints"),
    )


def downgrade() -> None:
    op.drop_table("strategy_checkpoints")
    op.drop_index("ix_paper_fills_run_status", table_name="paper_fills")
    op.drop_index("ix_paper_fills_run_target_symbol", table_name="paper_fills")
    op.drop_table("paper_fills")
    op.drop_index(
        "ix_order_intent_candidates_run_symbol",
        table_name="order_intent_candidates",
    )
    op.drop_index(
        "ix_order_intent_candidates_run_created_symbol",
        table_name="order_intent_candidates",
    )
    op.drop_table("order_intent_candidates")
    op.drop_index("ix_strategy_signals_run_symbol", table_name="strategy_signals")
    op.drop_index(
        "ix_strategy_signals_run_time_symbol",
        table_name="strategy_signals",
    )
    op.drop_table("strategy_signals")
    op.drop_table("strategy_runs")
