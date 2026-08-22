from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260822_0018"
down_revision: str | None = "20260822_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Retention runs in bounded batches.  These indexes keep the candidate
    # selection ordered by time instead of scanning the entire hot tables.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "ix_contract_metadata_effective_at "
                "ON contract_metadata (effective_at)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "DROP INDEX CONCURRENTLY IF EXISTS "
                "ix_contract_metadata_effective_at"
            )
        )
