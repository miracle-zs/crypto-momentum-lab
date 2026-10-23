"""Remove the exact duplicate runtime-state index.

The primary key already provides ``(environment, symbol, bucket_start)``.
Keeping the second index with the same key order makes every hot-state insert
and retention delete maintain an unnecessary B-tree.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260823_0019"
down_revision: str | None = "20260822_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_runtime_market_states_15s_symbol_time"


def upgrade() -> None:
    # The index belongs to a partitioned parent, so CONCURRENTLY is unsupported.
    op.drop_index(_INDEX_NAME, if_exists=True)


def downgrade() -> None:
    op.create_index(
        _INDEX_NAME,
        "runtime_market_states_15s",
        ["environment", "symbol", "bucket_start"],
        if_not_exists=True,
    )
