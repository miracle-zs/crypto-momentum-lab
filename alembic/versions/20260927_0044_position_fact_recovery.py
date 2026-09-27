"""Persist scoped account facts and position recovery checkpoints.

Revision ID: 20260927_0044
Revises: 20260925_0043
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260927_0044"
down_revision: str | None = "20260925_0043"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "position_fact_journal_events",
        sa.Column("event_record_id", sa.String(64), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(16), nullable=False),
        sa.Column("stream_id", sa.String(256), nullable=False),
        sa.Column("stream_epoch", sa.String(256), nullable=False),
        sa.Column("event_kind", sa.String(32), nullable=False),
        sa.Column("event_id", sa.String(256), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("source_revision", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("payload", JSONB, nullable=False),
    )
    op.create_index(
        "ix_position_fact_events_scope_occurred",
        "position_fact_journal_events",
        [
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "occurred_at",
        ],
    )
    op.create_index(
        "ix_position_fact_events_scope_recorded",
        "position_fact_journal_events",
        [
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "recorded_at",
        ],
    )

    op.create_table(
        "position_recovery_checkpoints",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(16), primary_key=True),
        sa.Column("stream_id", sa.String(256), primary_key=True),
        sa.Column("stream_epoch", sa.String(256), primary_key=True),
        sa.Column("checkpoint_id", sa.String(128), primary_key=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("event_cut", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_revision", sa.Integer(), nullable=False),
        sa.Column("facts_hash", sa.String(64), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("payload", JSONB, nullable=False),
    )
    op.create_index(
        "ix_position_recovery_checkpoints_latest",
        "position_recovery_checkpoints",
        [
            "environment",
            "account_label",
            "symbol",
            "position_side",
            "stream_id",
            "stream_epoch",
            "event_cut",
        ],
    )


def downgrade() -> None:
    op.drop_table("position_recovery_checkpoints")
    op.drop_table("position_fact_journal_events")
