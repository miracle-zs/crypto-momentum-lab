from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260704_0009"
down_revision: str | None = "20260704_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "shadow_sessions",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_config_hash", sa.String(64), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("account_readiness", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("run_id", name="pk_shadow_sessions"),
    )
    op.create_table(
        "shadow_order_plans",
        sa.Column("order_plan_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("order_intent_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("decision_state", sa.String(32), nullable=False),
        sa.Column("account_readiness", sa.String(32), nullable=False),
        sa.Column("market_freshness", sa.String(32), nullable=False),
        sa.Column("risk_result", sa.String(32), nullable=False),
        sa.Column("state_closed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("order_payload", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["shadow_sessions.run_id"],
            name="fk_shadow_order_plans_run_id_shadow_sessions",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("order_plan_id", name="pk_shadow_order_plans"),
    )
    op.create_index(
        "uq_shadow_order_plans_run_intent",
        "shadow_order_plans",
        ["run_id", "order_intent_id"],
        unique=True,
    )
    op.create_index(
        "ix_shadow_order_plans_run_created",
        "shadow_order_plans",
        ["run_id", "created_at"],
    )
    op.create_index(
        "ix_shadow_order_plans_symbol_decision",
        "shadow_order_plans",
        ["symbol", "decision_state"],
    )
    op.create_table(
        "shadow_suppression_events",
        sa.Column("order_plan_id", sa.String(128), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("suppressed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("order_payload", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["order_plan_id"],
            ["shadow_order_plans.order_plan_id"],
            name=(
                "fk_shadow_suppression_events_order_plan_id_shadow_order_plans"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "order_plan_id",
            name="pk_shadow_suppression_events",
        ),
    )
    op.create_table(
        "shadow_decision_metrics",
        sa.Column("metric_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("reason", sa.String(128), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["shadow_sessions.run_id"],
            name="fk_shadow_decision_metrics_run_id_shadow_sessions",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("metric_id", name="pk_shadow_decision_metrics"),
    )
    op.create_index(
        "ix_shadow_decision_metrics_run_category",
        "shadow_decision_metrics",
        ["run_id", "category"],
    )
    op.create_table(
        "shadow_drill_results",
        sa.Column("drill_result_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("drill_name", sa.String(64), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["shadow_sessions.run_id"],
            name="fk_shadow_drill_results_run_id_shadow_sessions",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("drill_result_id", name="pk_shadow_drill_results"),
    )


def downgrade() -> None:
    op.drop_table("shadow_drill_results")
    op.drop_index(
        "ix_shadow_decision_metrics_run_category",
        table_name="shadow_decision_metrics",
    )
    op.drop_table("shadow_decision_metrics")
    op.drop_table("shadow_suppression_events")
    op.drop_index(
        "ix_shadow_order_plans_symbol_decision",
        table_name="shadow_order_plans",
    )
    op.drop_index(
        "ix_shadow_order_plans_run_created",
        table_name="shadow_order_plans",
    )
    op.drop_index(
        "uq_shadow_order_plans_run_intent",
        table_name="shadow_order_plans",
    )
    op.drop_table("shadow_order_plans")
    op.drop_table("shadow_sessions")
