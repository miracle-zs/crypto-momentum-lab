"""Align strategy_runtime_events primary key with partitioned schema.

Revision ID: 20260918_0037
Revises: 20260911_0036
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260918_0037"
down_revision: str | None = "20260911_0036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    pk_constraint = inspector.get_pk_constraint("strategy_runtime_events")
    constrained_columns = pk_constraint.get("constrained_columns", [])
    if "occurred_at" not in constrained_columns:
        constraint_name = pk_constraint.get("name")
        if constraint_name:
            op.drop_constraint(
                constraint_name, "strategy_runtime_events", type_="primary"
            )
        op.create_primary_key(
            "pk_strategy_runtime_events",
            "strategy_runtime_events",
            ["event_id", "occurred_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    pk_constraint = inspector.get_pk_constraint("strategy_runtime_events")
    constrained_columns = pk_constraint.get("constrained_columns", [])
    if "occurred_at" in constrained_columns:
        constraint_name = pk_constraint.get("name")
        if constraint_name:
            op.drop_constraint(
                constraint_name, "strategy_runtime_events", type_="primary"
            )
        op.create_primary_key(
            "pk_strategy_runtime_events",
            "strategy_runtime_events",
            ["event_id"],
        )
