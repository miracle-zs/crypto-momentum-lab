"""Add atomic execution heads and durable decision commit state.

Revision ID: 20260927_0045
Revises: 20260927_0044
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260927_0045"
down_revision: str | None = "20260927_0044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_book_heads",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(8), primary_key=True),
        sa.Column("stream_id", sa.String(256), nullable=False),
        sa.Column("stream_epoch", sa.String(256), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("projection_version", sa.String(64), nullable=False),
        sa.Column("state_payload", JSONB, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("revision >= 1", name="ck_execution_book_head_revision"),
    )

    op.create_table(
        "execution_evidence_receipts",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(8), primary_key=True),
        sa.Column("stream_id", sa.String(256), primary_key=True),
        sa.Column("stream_epoch", sa.String(256), primary_key=True),
        sa.Column("evidence_id", sa.String(128), primary_key=True),
        sa.Column("sequence", sa.BigInteger(), nullable=True),
        sa.Column("payload_digest", sa.String(64), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_execution_evidence_receipts_position_sequence",
        "execution_evidence_receipts",
        ["environment", "account_label", "symbol", "position_side", "sequence"],
    )

    op.create_table(
        "execution_trade_identities",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(8), primary_key=True),
        sa.Column("trade_id", sa.String(128), primary_key=True),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("payload_digest", sa.String(64), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_execution_trade_quantity"),
        sa.CheckConstraint("price > 0", name="ck_execution_trade_price"),
    )
    op.create_index(
        "ix_execution_trade_identities_order",
        "execution_trade_identities",
        ["environment", "account_label", "symbol", "position_side", "order_id"],
    )

    op.create_table(
        "execution_order_watermarks",
        sa.Column("environment", sa.String(32), primary_key=True),
        sa.Column("account_label", sa.String(64), primary_key=True),
        sa.Column("symbol", sa.String(32), primary_key=True),
        sa.Column("position_side", sa.String(8), primary_key=True),
        sa.Column("order_id", sa.String(128), primary_key=True),
        sa.Column("cumulative_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("cumulative_quote", sa.Numeric(38, 18), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "cumulative_quantity >= 0", name="ck_execution_watermark_quantity"
        ),
        sa.CheckConstraint(
            "cumulative_quote >= 0", name="ck_execution_watermark_quote"
        ),
    )

    op.create_table(
        "durable_policy_states",
        sa.Column("policy_key", sa.String(256), primary_key=True),
        sa.Column("policy_revision", sa.Integer(), nullable=False),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("state_digest", sa.String(64), nullable=False),
        sa.Column("state_payload", JSONB, nullable=False),
        sa.Column("last_decision_id", sa.String(128), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("policy_revision >= 1", name="ck_durable_policy_revision"),
    )

    op.create_table(
        "durable_policy_commits",
        sa.Column("decision_id", sa.String(128), primary_key=True),
        sa.Column("policy_key", sa.String(256), nullable=False),
        sa.Column("prior_state_digest", sa.String(64), nullable=False),
        sa.Column("next_state_digest", sa.String(64), nullable=False),
        sa.Column("commit_digest", sa.String(64), nullable=False),
        sa.Column("policy_revision", sa.Integer(), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("policy_revision >= 1", name="ck_durable_commit_revision"),
    )
    op.create_index(
        "ix_durable_policy_commits_policy_revision",
        "durable_policy_commits",
        ["policy_key", "policy_revision"],
        unique=True,
    )

    op.create_table(
        "durable_decision_exits",
        sa.Column("decision_id", sa.String(128), primary_key=True),
        sa.Column("policy_key", sa.String(256), nullable=False),
        sa.Column("command_id", sa.String(128), nullable=False, unique=True),
        sa.Column("command_payload", JSONB, nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_receipt", sa.Text(), nullable=True),
        sa.Column("disposition_reason", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'DISPATCHED', 'SUPERSEDED')",
            name="ck_durable_exit_status",
        ),
    )
    op.create_index(
        "ix_durable_decision_exits_pending",
        "durable_decision_exits",
        ["policy_key", "created_at"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_table("durable_decision_exits")
    op.drop_index(
        "ix_durable_policy_commits_policy_revision",
        table_name="durable_policy_commits",
    )
    op.drop_table("durable_policy_commits")
    op.drop_table("durable_policy_states")
    op.drop_table("execution_order_watermarks")
    op.drop_index(
        "ix_execution_trade_identities_order",
        table_name="execution_trade_identities",
    )
    op.drop_table("execution_trade_identities")
    op.drop_index(
        "ix_execution_evidence_receipts_position_sequence",
        table_name="execution_evidence_receipts",
    )
    op.drop_table("execution_evidence_receipts")
    op.drop_table("execution_book_heads")
