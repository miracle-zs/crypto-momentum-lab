"""Index bounded cold metadata selection and live decision reference protection."""

from alembic import op

revision = "20261009_0056"
down_revision = "20261008_0055"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_revision_refs_cold_metadata ON market_revision_refs (bucket_start,revision_id) WHERE payload IS NULL AND payload_archive_path IS NOT NULL AND payload_archive_sha256 IS NOT NULL"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_decision_traces_full_revision_ids ON decision_traces USING gin(evaluated_revision_ids) WHERE coalesce(trace_payload ->> 'evidence_level','') <> 'summary'"
        )
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(
        "ALTER TABLE market_revision_refs SET (autovacuum_vacuum_scale_factor=0.005,autovacuum_vacuum_threshold=1000)"
    )


def downgrade():
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS ix_market_revision_refs_cold_metadata"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS ix_decision_traces_full_revision_ids"
        )
    op.execute(
        "ALTER TABLE market_revision_refs SET (autovacuum_vacuum_scale_factor=0.01,autovacuum_vacuum_threshold=5000)"
    )
