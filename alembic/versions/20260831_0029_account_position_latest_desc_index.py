"""Index latest account-position snapshots by side."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260831_0029"
down_revision: str | None = "20260830_0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_account_position_latest_desc"


def upgrade() -> None:
    # Retention keeps the latest row for every symbol/position side.  Matching
    # the DISTINCT ON ordering avoids sorting the full account history.
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"{_INDEX_NAME} ON account_position_snapshots "
                "(environment, account_label, symbol, position_side, "
                "observed_at DESC)"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}"))
