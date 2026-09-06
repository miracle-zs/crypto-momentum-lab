"""Remove the unsupported Binance canTrade account snapshot field."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260906_0030"
down_revision: str | None = "20260831_0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Account Information V3 does not return canTrade. Keeping a required
    # column here would turn an unavailable exchange field into false data.
    op.drop_column("account_config_snapshots", "can_trade")


def downgrade() -> None:
    op.add_column(
        "account_config_snapshots",
        sa.Column(
            "can_trade",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.alter_column("account_config_snapshots", "can_trade", server_default=None)
