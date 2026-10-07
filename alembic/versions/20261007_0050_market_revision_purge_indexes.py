"""Add online-maintenance indexes for trace compaction and revision purge.

Revision ID: 20261007_0050
Revises: 20261005_0049
Create Date: 2026-10-07
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261007_0050"
down_revision: str | None = "20261005_0049"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Both indexes are allowed on a busy live database.  ``IF NOT EXISTS``
    # adopts the normal-hold index that was created during the emergency
    # compaction before this migration existed.
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
              ix_decision_traces_uncompacted_normal_hold
            ON decision_traces (created_at, decision_id)
            WHERE rejection_reason = 'holding_position_no_exit'
              AND coalesce(trace_payload ->> 'evidence_level', '') <> 'summary'
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
              ix_market_revision_refs_noncanonical_published
            ON market_revision_refs (published_at, revision_id)
            WHERE is_canonical = false
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_market_revision_refs_noncanonical_published"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_decision_traces_uncompacted_normal_hold"
        )
