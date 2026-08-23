"""Add time-ordered indexes for account snapshot retention."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260823_0020"
down_revision: str | None = "20260823_0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_INDEXES = (
    (
        "ix_account_config_account_observed",
        "account_config_snapshots",
    ),
    (
        "ix_account_reconciliation_account_observed",
        "account_reconciliation_runs",
    ),
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for index_name, table_name in _INDEXES:
            op.execute(
                sa.text(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                    f"{index_name} ON {table_name} "
                    "(environment, account_label, observed_at)"
                )
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for index_name, _table_name in reversed(_INDEXES):
            op.execute(
                sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
            )
