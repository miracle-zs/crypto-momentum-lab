"""Add durable live exit and exposure claims."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260911_0034"
down_revision: str | None = "20260911_0033"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "exit_episode_reservations",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(8), nullable=False),
        sa.Column("episode_key", sa.String(256), nullable=False),
        sa.Column(
            "intent_id",
            sa.String(128),
            sa.ForeignKey("order_intents.intent_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("state", sa.String(48), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            "strategy_name",
            "symbol",
            "position_side",
            "episode_key",
            name="pk_exit_episode_reservations",
        ),
        sa.UniqueConstraint("intent_id", name="uq_exit_episode_intent"),
        sa.UniqueConstraint(
            "client_order_id",
            name="uq_exit_episode_client_order",
        ),
    )
    op.create_index(
        "ix_exit_episode_reservations_active",
        "exit_episode_reservations",
        ["environment", "account_label", "strategy_name", "active"],
    )

    op.create_table(
        "live_exposure_claims",
        sa.Column(
            "intent_id",
            sa.String(128),
            sa.ForeignKey("order_intents.intent_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("position_side", sa.String(8), nullable=False),
        sa.Column("notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("intent_id", name="pk_live_exposure_claims"),
    )
    op.create_index(
        "ix_live_exposure_claims_scope_active",
        "live_exposure_claims",
        ["environment", "account_label", "strategy_name", "active"],
    )
    op.create_index(
        "ix_live_exposure_claims_scope_symbol_active",
        "live_exposure_claims",
        [
            "environment",
            "account_label",
            "strategy_name",
            "symbol",
            "active",
        ],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_live_exposure_claims_scope_symbol_active",
        table_name="live_exposure_claims",
    )
    op.drop_index(
        "ix_live_exposure_claims_scope_active",
        table_name="live_exposure_claims",
    )
    op.drop_table("live_exposure_claims")
    op.drop_index(
        "ix_exit_episode_reservations_active",
        table_name="exit_episode_reservations",
    )
    op.drop_table("exit_episode_reservations")
