from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260726_0011"
down_revision: str | None = "20260704_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "paper_positions",
        sa.Column("position_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("entry_fill_id", sa.String(128), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("entry_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("exit_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("exit_fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("last_mark_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=True),
        sa.Column("return_pct", sa.Numeric(38, 18), nullable=True),
        sa.Column("close_reason", sa.String(64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["entry_fill_id"],
            ["paper_fills.fill_id"],
            name="fk_paper_positions_entry_fill_id_paper_fills",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_paper_positions_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("position_id", name="pk_paper_positions"),
        sa.UniqueConstraint(
            "entry_fill_id",
            name="uq_paper_positions_entry_fill_id",
        ),
    )
    op.create_index(
        "ix_paper_positions_run_status_updated",
        "paper_positions",
        ["run_id", "status", "updated_at"],
    )
    op.create_index(
        "ix_paper_positions_run_symbol_opened",
        "paper_positions",
        ["run_id", "symbol", "opened_at"],
    )
    op.create_table(
        "paper_equity_snapshots",
        sa.Column("snapshot_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("balance", sa.Numeric(38, 18), nullable=False),
        sa.Column("equity", sa.Numeric(38, 18), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_fees", sa.Numeric(38, 18), nullable=False),
        sa.Column("open_position_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_paper_equity_snapshots_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            name="pk_paper_equity_snapshots",
        ),
    )
    op.create_index(
        "ix_paper_equity_run_observed",
        "paper_equity_snapshots",
        ["run_id", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_paper_equity_run_observed",
        table_name="paper_equity_snapshots",
    )
    op.drop_table("paper_equity_snapshots")
    op.drop_index(
        "ix_paper_positions_run_symbol_opened",
        table_name="paper_positions",
    )
    op.drop_index(
        "ix_paper_positions_run_status_updated",
        table_name="paper_positions",
    )
    op.drop_table("paper_positions")
