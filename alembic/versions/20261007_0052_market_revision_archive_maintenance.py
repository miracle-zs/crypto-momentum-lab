"""Bound archive discovery and reclaim obsolete market rows promptly.

Revision ID: 20261007_0052
Revises: 20261007_0051
Create Date: 2026-10-07
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261007_0052"
down_revision: str | None = "20261007_0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE market_revision_refs SET ("
        "autovacuum_vacuum_scale_factor=0.01, "
        "autovacuum_vacuum_threshold=5000, "
        "autovacuum_vacuum_insert_scale_factor=0.02, "
        "autovacuum_vacuum_insert_threshold=5000, "
        "autovacuum_vacuum_cost_delay=5, "
        "autovacuum_vacuum_cost_limit=200)"
    )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_market_revision_refs_pending_archive "
            "ON market_revision_refs (bucket_start, scope, revision_id) "
            "WHERE payload IS NOT NULL AND payload_archive_path IS NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_market_revision_refs_pending_archive"
        )
    op.execute(
        "ALTER TABLE market_revision_refs RESET ("
        "autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, "
        "autovacuum_vacuum_insert_scale_factor, "
        "autovacuum_vacuum_insert_threshold, "
        "autovacuum_vacuum_cost_delay, autovacuum_vacuum_cost_limit)"
    )
