"""Record the partitioned strategy-event schema.

Revision ID: 20260918_0037
Revises: 20260911_0036
Create Date: 2026-09-18
"""

from collections.abc import Sequence

revision: str = "20260918_0037"
down_revision: str | None = "20260911_0036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
