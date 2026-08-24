"""Track runtime market-state completeness across aggTrade gaps."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260824_0021"
down_revision: str | None = "20260823_0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "data_complete",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "missing_agg_trade_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.create_check_constraint(
        "missing_agg_trade_nonnegative",
        "runtime_market_states_15s",
        "missing_agg_trade_count >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "missing_agg_trade_nonnegative",
        "runtime_market_states_15s",
        type_="check",
    )
    op.drop_column(
        "runtime_market_states_15s",
        "missing_agg_trade_count",
    )
    op.drop_column("runtime_market_states_15s", "data_complete")
