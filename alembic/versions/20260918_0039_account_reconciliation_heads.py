"""Persist latest ready account reconciliation heads projection.

Revision ID: 20260918_0039
Revises: 20260918_0038
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260918_0039"
down_revision: str | None = "20260918_0038"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE_NAME = "account_reconciliation_heads"
_INDEX_NAME = "ix_account_reconciliation_heads_lookup"


def upgrade() -> None:
    op.create_table(
        _TABLE_NAME,
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("reconciliation_id", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("balance_count", sa.Integer(), nullable=False),
        sa.Column("position_count", sa.Integer(), nullable=False),
        sa.Column("open_order_count", sa.Integer(), nullable=False),
        sa.Column("fill_count", sa.Integer(), nullable=False),
        sa.Column("mismatch_count", sa.Integer(), nullable=False),
        sa.Column("details", JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "projection_schema_version",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column("projected_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            name="pk_account_reconciliation_heads",
        ),
    )
    op.create_index(
        _INDEX_NAME,
        _TABLE_NAME,
        ["environment", "status", "position_count"],
    )

    # Seed the head projection with the latest ready reconciliation run
    # per account/environment from existing immutable historical runs.
    op.execute(
        sa.text(
            f"""
            INSERT INTO {_TABLE_NAME} (
                environment,
                account_label,
                reconciliation_id,
                status,
                observed_at,
                balance_count,
                position_count,
                open_order_count,
                fill_count,
                mismatch_count,
                details,
                projection_schema_version,
                projected_at
            )
            SELECT DISTINCT ON (environment, account_label)
                environment,
                account_label,
                reconciliation_id,
                status,
                observed_at,
                balance_count,
                position_count,
                open_order_count,
                fill_count,
                mismatch_count,
                details,
                1,
                NOW()
            FROM account_reconciliation_runs
            WHERE status = 'ready'
            ORDER BY
                environment,
                account_label,
                observed_at DESC,
                reconciliation_id DESC
            ON CONFLICT (environment, account_label) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_index(_INDEX_NAME, table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
