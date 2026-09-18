"""Add index for latest ready account reconciliation discovery.

Revision ID: 20260918_0038
Revises: 20260918_0037
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260918_0038"
down_revision: str | None = "20260918_0037"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_account_reconciliation_active_discovery"


def upgrade() -> None:
    # Match the DISTINCT ON query in load_active_position_account_labels():
    # WHERE environment = :env AND status = 'ready'
    # ORDER BY account_label, observed_at DESC, reconciliation_id DESC
    # Including position_count enables an Index Only Scan without table heaps.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"{_INDEX_NAME} ON account_reconciliation_runs ("
                "environment, account_label, observed_at DESC, reconciliation_id DESC"
                ") "
                "INCLUDE (position_count) "
                "WHERE status = 'ready'"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}"))
