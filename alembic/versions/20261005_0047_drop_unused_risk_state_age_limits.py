from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261005_0047"
down_revision: str | None = "20261001_0046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("risk_config_snapshots", "max_market_state_age_seconds")
    op.drop_column("risk_config_snapshots", "max_account_state_age_seconds")


def downgrade() -> None:
    for name in (
        "max_account_state_age_seconds",
        "max_market_state_age_seconds",
    ):
        op.add_column(
            "risk_config_snapshots",
            sa.Column(
                name,
                sa.Numeric(18, 6),
                nullable=False,
                server_default=sa.text("30"),
            ),
        )
        op.alter_column("risk_config_snapshots", name, server_default=None)
