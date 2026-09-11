"""Support time-bounded decision SLO telemetry queries."""

from collections.abc import Sequence

from alembic import op

revision: str = "20260911_0036"
down_revision: str | None = "20260911_0035"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_strategy_runtime_events_time",
        "strategy_runtime_events",
        ["occurred_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_strategy_runtime_events_time",
        table_name="strategy_runtime_events",
    )
