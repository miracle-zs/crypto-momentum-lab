"""Persist one-time entry repricing attempts.

Revision ID: 20261109_0071
Revises: 20261024_0070
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261109_0071"
down_revision: str | None = "20261024_0070"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "live_orders",
        sa.Column("entry_reprice_attempted_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    op.drop_column("live_orders", "entry_reprice_attempted_at")
