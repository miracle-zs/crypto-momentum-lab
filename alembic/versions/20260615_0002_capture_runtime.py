from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260615_0002"
down_revision: str | None = "20260614_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_archive_manifests",
        sa.Column(
            "manifest_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("route", sa.String(16), nullable=False),
        sa.Column("stream", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column("utc_date", sa.Date(), nullable=False),
        sa.Column("utc_hour", sa.Integer(), nullable=False),
        sa.Column(
            "connection_session_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("subscription_generation_min", sa.Integer(), nullable=False),
        sa.Column("subscription_generation_max", sa.Integer(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("compressed_bytes", sa.Integer(), nullable=False),
        sa.Column(
            "first_exchange_event_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_exchange_event_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "first_received_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "last_received_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("capture_version", sa.String(64), nullable=False),
        sa.Column("recovery_status", sa.String(32), nullable=False),
        sa.Column("known_gap_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "manifest_id",
            name="pk_raw_archive_manifests",
        ),
        sa.UniqueConstraint(
            "relative_path",
            name="uq_raw_archive_manifests_relative_path",
        ),
    )
    op.create_table(
        "market_data_quality_events",
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route", sa.String(16), nullable=True),
        sa.Column("stream", sa.String(32), nullable=True),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column(
            "connection_session_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("local_sequence", sa.Integer(), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "event_id",
            name="pk_market_data_quality_events",
        ),
    )
    op.create_table(
        "market_data_process_states",
        sa.Column(
            "state_id",
            sa.Integer(),
            sa.Identity(),
            nullable=False,
        ),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint(
            "state_id",
            name="pk_market_data_process_states",
        ),
    )


def downgrade() -> None:
    op.drop_table("market_data_process_states")
    op.drop_table("market_data_quality_events")
    op.drop_table("raw_archive_manifests")
