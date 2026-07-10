from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0008"
down_revision: str | None = "20260704_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "order_intents",
        sa.Column("intent_id", sa.String(128), nullable=False),
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("risk_evaluation_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("intent_id", name="pk_order_intents"),
        sa.UniqueConstraint("candidate_id", name="uq_order_intents_candidate_id"),
        sa.UniqueConstraint(
            "risk_evaluation_id",
            name="uq_order_intents_risk_evaluation_id",
        ),
    )
    op.create_table(
        "order_intent_claims",
        sa.Column("intent_id", sa.String(128), nullable=False),
        sa.Column("worker_id", sa.String(128), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            ["order_intents.intent_id"],
            name="fk_order_intent_claims_intent_id_order_intents",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("intent_id", name="pk_order_intent_claims"),
    )
    op.create_table(
        "exchange_orders",
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("intent_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("order_type", sa.String(32), nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("state", sa.String(48), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            ["order_intents.intent_id"],
            name="fk_exchange_orders_intent_id_order_intents",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("client_order_id", name="pk_exchange_orders"),
        sa.UniqueConstraint("intent_id", name="uq_exchange_orders_intent_id"),
    )
    op.create_index(
        "ix_exchange_orders_state_updated",
        "exchange_orders",
        ["state", "updated_at"],
    )
    op.create_index(
        "ix_exchange_orders_symbol_state",
        "exchange_orders",
        ["symbol", "state"],
    )
    op.create_table(
        "exchange_order_events",
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("state", sa.String(48), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["client_order_id"],
            ["exchange_orders.client_order_id"],
            name=(
                "fk_exchange_order_events_client_order_id_exchange_orders"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_exchange_order_events"),
    )
    op.create_index(
        "ix_exchange_order_events_order_time",
        "exchange_order_events",
        ["client_order_id", "occurred_at"],
    )
    op.create_table(
        "exchange_fills",
        sa.Column("fill_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("exchange_trade_id", sa.String(64), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee_asset", sa.String(32), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["client_order_id"],
            ["exchange_orders.client_order_id"],
            name="fk_exchange_fills_client_order_id_exchange_orders",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("fill_id", name="pk_exchange_fills"),
    )
    op.create_index(
        "ix_exchange_fills_order_time",
        "exchange_fills",
        ["client_order_id", "filled_at"],
    )
    op.create_table(
        "execution_commands",
        sa.Column("command_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=True),
        sa.Column("command", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("command_id", name="pk_execution_commands"),
    )
    op.create_table(
        "execution_reconciliation_events",
        sa.Column("reconciliation_event_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("outcome", sa.String(64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint(
            "reconciliation_event_id",
            name="pk_execution_reconciliation_events",
        ),
    )


def downgrade() -> None:
    op.drop_table("execution_reconciliation_events")
    op.drop_table("execution_commands")
    op.drop_index("ix_exchange_fills_order_time", table_name="exchange_fills")
    op.drop_table("exchange_fills")
    op.drop_index(
        "ix_exchange_order_events_order_time",
        table_name="exchange_order_events",
    )
    op.drop_table("exchange_order_events")
    op.drop_index("ix_exchange_orders_symbol_state", table_name="exchange_orders")
    op.drop_index("ix_exchange_orders_state_updated", table_name="exchange_orders")
    op.drop_table("exchange_orders")
    op.drop_table("order_intent_claims")
    op.drop_table("order_intents")
