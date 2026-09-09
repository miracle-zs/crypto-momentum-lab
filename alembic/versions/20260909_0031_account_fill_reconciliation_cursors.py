"""Persist per-symbol account-fill reconciliation cursors."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260909_0031"
down_revision: str | None = "20260906_0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "account_fill_reconciliation_cursors",
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("from_id", sa.BigInteger(), nullable=True),
        sa.Column("start_time_ms", sa.BigInteger(), nullable=True),
        sa.Column(
            "last_checked_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(from_id IS NULL) <> (start_time_ms IS NULL)",
            name="ck_account_fill_cursor_one_position",
        ),
        sa.PrimaryKeyConstraint(
            "environment",
            "account_label",
            "symbol",
            name="pk_account_fill_reconciliation_cursors",
        ),
    )
    op.create_index(
        "ix_account_fill_cursor_due",
        "account_fill_reconciliation_cursors",
        ["environment", "account_label", "last_checked_at"],
    )

    # Existing fill rows are the durable baseline.  Seeding from their
    # highest numeric trade id prevents the first post-migration restart from
    # replaying the latest 1,000 trades for every historical symbol.  A normal
    # incremental reconciliation still verifies each symbol later.
    op.execute(
        sa.text(
            """
            INSERT INTO account_fill_reconciliation_cursors (
                environment,
                account_label,
                symbol,
                from_id,
                start_time_ms,
                last_checked_at
            )
            SELECT
                environment,
                account_label,
                symbol,
                MAX(trade_id::bigint) + 1,
                NULL,
                NOW()
            FROM account_fill_events
            WHERE trade_id ~ '^[0-9]+$'
            GROUP BY environment, account_label, symbol
            ON CONFLICT (environment, account_label, symbol) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_index(
        "ix_account_fill_cursor_due",
        table_name="account_fill_reconciliation_cursors",
    )
    op.drop_table("account_fill_reconciliation_cursors")
