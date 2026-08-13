from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260813_0014"
down_revision: str | None = "20260804_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "paper_positions",
        sa.Column(
            "grace_exit_started_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "paper_positions",
        sa.Column(
            "grace_exit_deadline",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("paper_positions", "grace_exit_deadline")
    op.drop_column("paper_positions", "grace_exit_started_at")
