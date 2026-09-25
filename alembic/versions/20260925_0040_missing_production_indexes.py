"""Create missing production indexes for universe entries and exchange orders.

Revision ID: 20260925_0040
Revises: 20260918_0039
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260925_0040"
down_revision: str | None = "20260918_0039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEXES = (
    (
        "ix_universe_entries_price_time",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_universe_entries_price_time "
        "ON universe_entries (price_time)",
    ),
    (
        "ix_exchange_orders_unresolved_partial",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_exchange_orders_unresolved_partial "
        "ON exchange_orders (run_id, updated_at, client_order_id) "
        "WHERE state NOT IN ('filled', 'canceled', 'absent_reconciled', 'rejected', 'expired', 'suppressed')",
    ),
)


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            for _name, statement in _INDEXES:
                op.execute(sa.text(statement))
    else:
        op.create_index(
            "ix_universe_entries_price_time",
            "universe_entries",
            ["price_time"],
            if_not_exists=True,
        )
        op.create_index(
            "ix_exchange_orders_unresolved_partial",
            "exchange_orders",
            ["run_id", "updated_at", "client_order_id"],
            postgresql_where=sa.text(
                "state NOT IN ('filled', 'canceled', 'absent_reconciled', 'rejected', 'expired', 'suppressed')"
            ),
            if_not_exists=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            for name, _statement in reversed(_INDEXES):
                op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))
    else:
        op.drop_index(
            "ix_exchange_orders_unresolved_partial",
            table_name="exchange_orders",
            if_exists=True,
        )
        op.drop_index(
            "ix_universe_entries_price_time",
            table_name="universe_entries",
            if_exists=True,
        )
