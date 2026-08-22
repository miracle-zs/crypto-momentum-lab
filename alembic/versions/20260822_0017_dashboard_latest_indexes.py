from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260822_0017"
down_revision: str | None = "20260814_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_INDEXES = (
    (
        "ix_runtime_market_states_15s_latest_bucket",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_runtime_market_states_15s_latest_bucket "
        "ON runtime_market_states_15s (bucket_start) INCLUDE (bucket_end)",
    ),
    (
        "ix_execution_account_process_states_latest",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_execution_account_process_states_latest "
        "ON execution_account_process_states (occurred_at)",
    ),
    (
        "ix_strategy_runtime_checkpoints_latest",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_strategy_runtime_checkpoints_latest "
        "ON strategy_runtime_checkpoints (saved_at)",
    ),
    (
        "ix_live_session_transitions_latest",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_live_session_transitions_latest "
        "ON live_session_transitions (occurred_at)",
    ),
    (
        "ix_trading_leases_active_expiry",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_trading_leases_active_expiry "
        "ON trading_leases (expires_at) WHERE state = 'active'",
    ),
)


def upgrade() -> None:
    # The market-state table is several gigabytes on the server. Concurrent
    # builds keep writers running while the dashboard read-path is repaired.
    with op.get_context().autocommit_block():
        for _name, statement in _INDEXES:
            op.execute(sa.text(statement))


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _statement in reversed(_INDEXES):
            op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))
