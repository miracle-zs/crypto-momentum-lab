"""Drop the derived account reconciliation head table.

Revision ID: 20261015_0063
Revises: 20261015_0062
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20261015_0063"
down_revision: str | None = "20261015_0062"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_table("account_reconciliation_heads")


def downgrade() -> None:
    op.create_table(
        "account_reconciliation_heads",
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
        "ix_account_reconciliation_heads_lookup",
        "account_reconciliation_heads",
        ["environment", "status", "position_count"],
    )
    op.execute(
        sa.text(
            """
            INSERT INTO account_reconciliation_heads (
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
            """
        )
    )
