"""Index live signal reads by account and observation time."""

from collections.abc import Sequence

from alembic import op

revision: str = "20260828_0023"
down_revision: str | None = "20260828_0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_live_strategy_signals_account_time"


def upgrade() -> None:
    # Dashboard reads must not block the live signal recorder while the index
    # is built on an already-running production table.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            f"{_INDEX_NAME} ON live_strategy_signals "
            "(account_label, detected_at, recorded_at)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}")
