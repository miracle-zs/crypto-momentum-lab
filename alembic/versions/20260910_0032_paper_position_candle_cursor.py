"""Persist the last official candle processed for paper exits."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260910_0032"
down_revision: str | None = "20260909_0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "paper_positions",
        sa.Column(
            "last_candle_end",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("paper_positions", "last_candle_end")
