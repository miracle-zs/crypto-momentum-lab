"""Remove the database dispatch lease for single-process accounts.

The deployment runs one live process per account, so process ownership does
not need a second representation in PostgreSQL.
"""

import sqlalchemy as sa

from alembic import op

revision = "20261014_0061"
down_revision = "20261014_0060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("trading_leases")


def downgrade() -> None:
    op.create_table(
        "trading_leases",
        sa.Column("lease_id", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(32), nullable=False),
        sa.Column("account_label", sa.String(64), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("owner", sa.String(128), nullable=False),
        sa.Column("code_generation", sa.String(128), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("lease_id", name="pk_trading_leases"),
    )
    op.create_check_constraint(
        "trading_leases_code_generation_nonempty",
        "trading_leases",
        "length(trim(code_generation)) > 0",
    )
    op.create_index(
        "uq_trading_leases_active_account",
        "trading_leases",
        ["environment", "account_label"],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )
    op.create_index(
        "ix_trading_leases_account_expiry",
        "trading_leases",
        ["environment", "account_label", "expires_at"],
    )
    op.create_index(
        "ix_trading_leases_active_expiry",
        "trading_leases",
        ["expires_at"],
        postgresql_where=sa.text("state = 'active'"),
    )
