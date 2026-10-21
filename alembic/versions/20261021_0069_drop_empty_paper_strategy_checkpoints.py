"""Drop the unused, empty paper strategy checkpoint table.

Revision ID: 20261021_0069
Revises: 20261021_0068
"""

from collections.abc import Sequence

from sqlalchemy import text

from alembic import op

revision: str = "20261021_0069"
down_revision: str | None = "20261021_0068"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    has_rows = op.get_bind().execute(
        text("SELECT EXISTS (SELECT 1 FROM strategy_checkpoints LIMIT 1)")
    ).scalar_one()
    if has_rows:
        raise RuntimeError(
            "strategy_checkpoints is not empty; archive its rows before retirement"
        )
    op.drop_table("strategy_checkpoints")


def downgrade() -> None:
    raise RuntimeError(
        "Retired strategy checkpoints require an explicit data restore"
    )
