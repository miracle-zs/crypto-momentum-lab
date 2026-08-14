from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260814_0016"
down_revision: str | None = "20260814_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "risk_config_snapshots",
        "max_daily_loss",
        existing_type=sa.Numeric(38, 18),
        nullable=True,
    )
    op.alter_column(
        "live_operator_approvals",
        "approved_max_daily_loss",
        existing_type=sa.Numeric(38, 18),
        nullable=True,
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE risk_config_snapshots
            SET max_daily_loss = COALESCE(
                max_daily_loss,
                99999999999999999999.999999999999999999
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_operator_approvals
            SET approved_max_daily_loss = COALESCE(
                approved_max_daily_loss,
                99999999999999999999.999999999999999999
            )
            """
        )
    )
    op.alter_column(
        "risk_config_snapshots",
        "max_daily_loss",
        existing_type=sa.Numeric(38, 18),
        nullable=False,
    )
    op.alter_column(
        "live_operator_approvals",
        "approved_max_daily_loss",
        existing_type=sa.Numeric(38, 18),
        nullable=False,
    )
