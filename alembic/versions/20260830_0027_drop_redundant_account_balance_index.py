"""Drop the redundant ascending account-balance latest index."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260830_0027"
down_revision: str | None = "20260830_0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_INDEX_NAME = "ix_account_balance_latest"


def upgrade() -> None:
    # The descending index has the same equality prefix and serves both newest
    # and oldest range scans. Removing the unused ascending copy cuts roughly
    # 96 MB of index storage and avoids maintaining two copies on every write.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_OLD_INDEX_NAME}")
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"{_OLD_INDEX_NAME} ON account_balance_snapshots "
                "(environment, account_label, asset, observed_at)"
            )
        )
