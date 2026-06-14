from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260614_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "contract_metadata",
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("contract_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("quote_asset", sa.String(16), nullable=False),
        sa.Column("margin_asset", sa.String(16), nullable=False),
        sa.Column("onboard_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "symbol",
            "effective_at",
            name="pk_contract_metadata",
        ),
    )
    op.create_table(
        "daily_open_prices",
        sa.Column("utc_day", sa.Date(), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("open_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "utc_day",
            "symbol",
            name="pk_daily_open_prices",
        ),
    )
    op.create_table(
        "universe_snapshots",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("utc_day", sa.Date(), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("activated", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            name="pk_universe_snapshots",
        ),
        sa.UniqueConstraint(
            "observed_at",
            name="uq_universe_snapshots_observed_at",
        ),
    )
    op.create_table(
        "universe_entries",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("open_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("current_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("price_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("utc_day_return", sa.Numeric(38, 18), nullable=True),
        sa.Column("gainer_rank", sa.Integer(), nullable=True),
        sa.Column("loser_rank", sa.Integer(), nullable=True),
        sa.Column("is_target", sa.Boolean(), nullable=False),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["universe_snapshots.snapshot_id"],
            name="fk_universe_entries_snapshot_id_universe_snapshots",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "symbol",
            name="pk_universe_entries",
        ),
    )
    op.create_index(
        "ix_universe_entries_snapshot_target",
        "universe_entries",
        ["snapshot_id", "is_target"],
    )
    op.create_table(
        "monitoring_memberships",
        sa.Column(
            "snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("side", sa.String(16), nullable=True),
        sa.Column(
            "left_target_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["universe_snapshots.snapshot_id"],
            name="fk_monitoring_memberships_snapshot_id_universe_snapshots",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "symbol",
            name="pk_monitoring_memberships",
        ),
    )


def downgrade() -> None:
    op.drop_table("monitoring_memberships")
    op.drop_index(
        "ix_universe_entries_snapshot_target",
        table_name="universe_entries",
    )
    op.drop_table("universe_entries")
    op.drop_table("universe_snapshots")
    op.drop_table("daily_open_prices")
    op.drop_table("contract_metadata")
