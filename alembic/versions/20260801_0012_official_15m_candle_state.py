from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260801_0012"
down_revision: str | None = "20260726_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "closed_kline_1m_open_time",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "closed_kline_1m_close_time",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "closed_kline_1m_open_price",
            sa.Numeric(38, 18),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_market_states_15s",
        sa.Column(
            "closed_kline_1m_close_price",
            sa.Numeric(38, 18),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("runtime_market_states_15s", "closed_kline_1m_close_price")
    op.drop_column("runtime_market_states_15s", "closed_kline_1m_open_price")
    op.drop_column("runtime_market_states_15s", "closed_kline_1m_close_time")
    op.drop_column("runtime_market_states_15s", "closed_kline_1m_open_time")
