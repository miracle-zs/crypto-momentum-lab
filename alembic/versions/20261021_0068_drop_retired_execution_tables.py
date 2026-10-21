"""Drop archived tables from the retired execution stack.

Revision ID: 20261021_0068
Revises: 20261018_0067

Populated table contents were exported and verified under table-archive, and
empty tables were confirmed empty before this forward-only schema retirement.
The current NextGen runtime has no consumers for these tables.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261021_0068"
down_revision: str | None = "20261018_0067"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop foreign-key dependent tables before their parents. The remaining
    # retired tables are independent.
    for table in (
        "order_intent_claims",
        "risk_rejections",
        "exit_episode_reservations",
        "live_exposure_claims",
        "execution_reconciliation_events",
        "execution_commands",
        "live_operator_approvals",
        "live_rollback_commands",
        "risk_evaluations",
        "strategy_live_states",
        "execution_account_process_states",
        "execution_evidence_receipts",
        "execution_retired_streams",
        "execution_order_watermarks",
        "execution_trade_identities",
        "durable_policy_states",
        "durable_policy_commits",
        "durable_decision_exits",
        "position_reservations",
        "execution_book_heads",
    ):
        op.drop_table(table)


def downgrade() -> None:
    # The archived records live outside PostgreSQL. Recreating empty tables
    # would make an application rollback appear successful while losing their
    # history, so this retirement is forward-only.
    raise RuntimeError("Retired execution tables require an explicit data restore")
