"""Persist the daily and flat-cycle entry sizing anchor for each account.

Revision ID: 20261024_0070
Revises: 20261021_0069
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261024_0070"
down_revision: str | None = "20261021_0069"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "entry_sizing_states",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("equity_fraction", sa.Numeric(), nullable=False),
        sa.Column("equity", sa.Numeric(), nullable=False),
        sa.Column("sampled_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("environment", "account_label"),
        sa.CheckConstraint(
            "mode IN ('daily', 'flat_reset')", name="entry_sizing_mode_check"
        ),
        sa.CheckConstraint(
            "equity_fraction > 0 AND equity_fraction <= 1",
            name="entry_sizing_fraction_check",
        ),
        sa.CheckConstraint("equity > 0", name="entry_sizing_equity_positive"),
    )


def downgrade() -> None:
    op.drop_table("entry_sizing_states")
