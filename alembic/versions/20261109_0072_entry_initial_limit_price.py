"""Preserve the initial limit price for entry orders.

Revision ID: 20261109_0072
Revises: 20261109_0071
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261109_0072"
down_revision: str | None = "20261109_0071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "live_orders",
        sa.Column("entry_initial_limit_price", sa.Numeric(28, 10), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_orders", "entry_initial_limit_price")
