"""Store a policy state once, referenced by exact decision evidence digests."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "20261008_0055"
down_revision = "20261008_0054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "decision_policy_evidence",
        sa.Column("state_digest", sa.String(64), primary_key=True),
        sa.Column("state_payload", JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        if_not_exists=True,
    )
    with op.get_context().autocommit_block():
        for field in ("prior", "next"):
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_decision_traces_policy_{field} "
                f"ON decision_traces ((trace_payload->'policy_state_refs'->>'{field}')) "
                "WHERE trace_payload ? 'policy_state_refs'"
            )
    op.execute("SET LOCAL lock_timeout='5s'")
    for table in ("position_fact_journal_events", "position_recovery_checkpoints"):
        op.execute(
            f"ALTER TABLE {table} SET (autovacuum_vacuum_scale_factor=0.005, "
            "autovacuum_vacuum_threshold=500, autovacuum_analyze_scale_factor=0.01, "
            "toast.autovacuum_vacuum_scale_factor=0.005, "
            "toast.autovacuum_vacuum_threshold=500)"
        )


def downgrade() -> None:
    raise RuntimeError(
        "Referenced policy evidence must be restored inline before downgrade"
    )
