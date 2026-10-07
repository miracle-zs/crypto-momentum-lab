"""Allow immutable market payloads to live in verified zstd archives.

Revision ID: 20261007_0051
Revises: 20261007_0050
Create Date: 2026-10-07
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20261007_0051"
down_revision: str | None = "20261007_0050"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column("market_revision_refs", "payload", nullable=True)
    op.add_column(
        "market_revision_refs",
        sa.Column("payload_archive_path", sa.Text(), nullable=True),
    )
    op.add_column(
        "market_revision_refs",
        sa.Column("payload_archive_sha256", sa.String(length=64), nullable=True),
    )
def downgrade() -> None:
    # A downgrade must not erase the only pointer to archived market payloads.
    bind = op.get_bind()
    archived_count = bind.execute(
        sa.text(
            "SELECT count(*) FROM market_revision_refs "
            "WHERE payload IS NULL AND payload_archive_path IS NOT NULL"
        )
    ).scalar_one()
    if archived_count:
        raise RuntimeError(
            "Cannot downgrade market revision payload archive migration while "
            f"{archived_count} payloads are archived; restore them to PostgreSQL first"
        )

    op.drop_column("market_revision_refs", "payload_archive_sha256")
    op.drop_column("market_revision_refs", "payload_archive_path")
    op.alter_column("market_revision_refs", "payload", nullable=False)
