"""Index UTC-hour buckets used by account snapshot retention."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260830_0028"
down_revision: str | None = "20260830_0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_account_balance_asset_hour_observed"


def upgrade() -> None:
    # Retention ranks rows by this exact UTC-hour expression. The covering
    # order lets PostgreSQL stream the ranking scan without a large temp sort.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"{_INDEX_NAME} ON account_balance_snapshots "
                "(environment, account_label, asset, "
                "(date_trunc('hour', observed_at AT TIME ZONE 'UTC')), "
                "observed_at DESC)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}"))
