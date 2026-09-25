"""Create tables for Phase C: market_revision_refs, dataset_manifests, decision_traces.

Revision ID: 20260925_0042
Revises: 20260925_0041
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260925_0042"
down_revision: str | None = "20260925_0041"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. market_revision_refs
    op.create_table(
        "market_revision_refs",
        sa.Column("revision_id", sa.String(128), primary_key=True),
        sa.Column("scope", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bucket_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_epoch", sa.String(64), nullable=False),
        sa.Column("visibility_mode", sa.String(32), nullable=False),
        sa.Column(
            "is_canonical",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("lineage", JSONB, nullable=False, server_default="{}"),
    )
    op.create_index(
        "ix_market_revisions_bucket",
        "market_revision_refs",
        ["scope", "symbol", "interval", "bucket_start"],
    )
    op.create_index(
        "ix_market_revisions_canonical",
        "market_revision_refs",
        ["scope", "symbol", "interval", "bucket_start", "is_canonical"],
    )

    # 2. dataset_manifests
    op.create_table(
        "dataset_manifests",
        sa.Column("manifest_id", sa.String(128), primary_key=True),
        sa.Column("scope", sa.String(32), nullable=False),
        sa.Column("symbols", sa.Text(), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("visibility_mode", sa.String(32), nullable=False),
        sa.Column(
            "schema_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column(
            "feature_algorithm_version",
            sa.String(64),
            nullable=False,
            server_default="v1",
        ),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column(
            "coverage_ratio",
            sa.Numeric(10, 4),
            nullable=False,
            server_default="1.0",
        ),
        sa.Column("revision_ids", JSONB, nullable=False),
        sa.Column("holes", JSONB, nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    # 3. decision_traces
    op.create_table(
        "decision_traces",
        sa.Column("decision_id", sa.String(128), primary_key=True),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("intent_produced", sa.Boolean(), nullable=False),
        sa.Column("intent_id", sa.String(128), nullable=True),
        sa.Column("rejection_reason", sa.String(128), nullable=True),
        sa.Column("evaluated_revision_ids", JSONB, nullable=False),
        sa.Column("trace_payload", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_decision_traces_strategy_account_time",
        "decision_traces",
        ["strategy_name", "account_label", "decision_time"],
    )


def downgrade() -> None:
    op.drop_table("decision_traces")
    op.drop_table("dataset_manifests")
    op.drop_table("market_revision_refs")
