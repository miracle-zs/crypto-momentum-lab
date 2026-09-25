"""Create tables for Phase B: reservations, dependencies, prune plans.

Revision ID: 20260925_0041
Revises: 20260925_0040
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260925_0041"
down_revision: str | None = "20260925_0040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. position_reservations
    op.create_table(
        "position_reservations",
        sa.Column("reservation_id", sa.String(128), primary_key=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(8), nullable=False),
        sa.Column("batch_id", sa.String(128), nullable=False),
        sa.Column("command_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=True),
        sa.Column("reserved_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column(
            "consumed_quantity",
            sa.Numeric(38, 18),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "released_quantity",
            sa.Numeric(38, 18),
            nullable=False,
            server_default="0",
        ),
        sa.Column("expected_projection_version", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("release_reason", sa.String(64), nullable=True),
    )
    op.create_index(
        "ix_position_reservations_batch_active",
        "position_reservations",
        ["environment", "account_label", "symbol", "batch_id", "status"],
    )
    op.create_index(
        "ix_position_reservations_command",
        "position_reservations",
        ["command_id"],
    )

    # 2. consumer_dependencies
    op.create_table(
        "consumer_dependencies",
        sa.Column("consumer_id", sa.String(128), nullable=False),
        sa.Column("dataset_name", sa.String(64), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("recovery_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("earliest_checkpoint_id", sa.String(128), nullable=True),
        sa.Column("recovery_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "cold_recovery_supported",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("dependency_version", sa.String(64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False, server_default=""),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "consumer_id", "dataset_name", name="pk_consumer_dependencies"
        ),
    )

    # 3. prune_plans
    op.create_table(
        "prune_plans",
        sa.Column("plan_id", sa.String(128), primary_key=True),
        sa.Column("dataset_name", sa.String(64), nullable=False),
        sa.Column("requested_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "is_constrained", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.Column("binding_consumer_id", sa.String(128), nullable=True),
        sa.Column("manifest_hash", sa.String(64), nullable=True),
        sa.Column("expected_dependency_version", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("rows_archived", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("rows_deleted", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("prune_plans")
    op.drop_table("consumer_dependencies")
    op.drop_table("position_reservations")
