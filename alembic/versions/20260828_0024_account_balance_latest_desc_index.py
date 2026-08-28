"""Index account balance latest reads in descending observation order."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260828_0024"
down_revision: str | None = "20260828_0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_account_balance_latest_desc"


def upgrade() -> None:
    # Retention and dashboard reads run while account snapshots are written.
    # Build this index without taking a write-blocking table lock.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"{_INDEX_NAME} ON account_balance_snapshots "
                "(environment, account_label, asset, observed_at DESC)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}"))
