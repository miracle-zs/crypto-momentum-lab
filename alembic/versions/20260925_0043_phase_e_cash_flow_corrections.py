"""Create tables for Phase E: cash_flow_corrections and account_performance_metrics.

Revision ID: 20260925_0043
Revises: 20260925_0042
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260925_0043"
down_revision: str | None = "20260925_0042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. cash_flow_corrections
    op.create_table(
        "cash_flow_corrections",
        sa.Column("correction_id", sa.String(128), primary_key=True),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("amount", sa.Numeric(18, 8), nullable=False),
        sa.Column("cash_flow_type", sa.String(32), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("approval_ref", sa.String(128), nullable=False),
        sa.Column("evidence_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_cash_flow_corrections_account",
        "cash_flow_corrections",
        ["account_label", "effective_at"],
    )

    # 2. account_performance_metrics
    op.create_table(
        "account_performance_metrics",
        sa.Column("metric_id", sa.String(128), primary_key=True),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("metric_name", sa.String(64), nullable=False),
        sa.Column("metric_family", sa.String(64), nullable=False),
        sa.Column("metric_version", sa.String(32), nullable=False),
        sa.Column("interval_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interval_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Numeric(18, 8), nullable=True),
        sa.Column("unit", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("source_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_refs", JSONB, nullable=False, server_default="[]"),
        sa.Column("details", JSONB, nullable=False, server_default="{}"),
        sa.Column(
            "calculated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_account_perf_metrics_lookup",
        "account_performance_metrics",
        ["account_label", "metric_name", "interval_start", "interval_end"],
    )


def downgrade() -> None:
    op.drop_table("account_performance_metrics")
    op.drop_table("cash_flow_corrections")
