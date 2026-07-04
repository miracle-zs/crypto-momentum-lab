from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0005"
down_revision: str | None = "20260703_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_runtime_checkpoints",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column(
            "last_processed_at_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "warmup_buckets_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "cooldown_buckets_remaining_by_symbol",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("saved_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "run_id",
            name="pk_strategy_runtime_checkpoints",
        ),
    )
    op.create_table(
        "strategy_runtime_events",
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("event_id", name="pk_strategy_runtime_events"),
    )
    op.create_index(
        "ix_strategy_runtime_events_run_time",
        "strategy_runtime_events",
        ["run_id", "occurred_at"],
    )
    op.create_index(
        "ix_strategy_runtime_events_type_time",
        "strategy_runtime_events",
        ["event_type", "occurred_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_strategy_runtime_events_type_time",
        table_name="strategy_runtime_events",
    )
    op.drop_index(
        "ix_strategy_runtime_events_run_time",
        table_name="strategy_runtime_events",
    )
    op.drop_table("strategy_runtime_events")
    op.drop_table("strategy_runtime_checkpoints")
