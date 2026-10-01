"""Persist account WS receipts independently of the account projection."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20261001_0046"
down_revision: str | None = "20260927_0045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "account_user_data_journal",
        sa.Column("sequence", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("receiver_session_id", sa.String(64), nullable=False),
        sa.Column("stream_token", sa.BigInteger(), nullable=True),
        sa.Column("event_id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exchange_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("exchange_update_id", sa.BigInteger(), nullable=True),
        sa.Column("exchange_previous_update_id", sa.BigInteger(), nullable=True),
        sa.Column("payload", JSONB, nullable=False),
        sa.UniqueConstraint(
            "environment",
            "account_label",
            "event_id",
            name="uq_account_user_data_journal_event",
        ),
    )
    op.create_index(
        "ix_account_user_data_journal_cursor",
        "account_user_data_journal",
        ["environment", "account_label", "sequence"],
    )


def downgrade() -> None:
    op.drop_table("account_user_data_journal")
