"""Fence live order writers by the lease's deployed code generation."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260911_0035"
down_revision: str | None = "20260911_0034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Existing active leases cannot be attributed safely to the generation
    # applying this migration. Expire them before making the writer identity
    # mandatory; the next worker must acquire a fresh lease after deployment.
    op.add_column(
        "trading_leases",
        sa.Column("code_generation", sa.String(128), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE trading_leases "
            "SET state = 'expired' "
            "WHERE state = 'active'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE trading_leases "
            "SET code_generation = 'legacy-unknown' "
            "WHERE code_generation IS NULL"
        )
    )
    op.alter_column(
        "trading_leases",
        "code_generation",
        existing_type=sa.String(128),
        nullable=False,
    )
    op.create_check_constraint(
        "trading_leases_code_generation_nonempty",
        "trading_leases",
        "length(trim(code_generation)) > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "trading_leases_code_generation_nonempty",
        "trading_leases",
        type_="check",
    )
    op.drop_column("trading_leases", "code_generation")
