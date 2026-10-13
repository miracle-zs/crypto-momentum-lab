"""Bind a trading lease to the per-process holder that owns it."""

import sqlalchemy as sa

from alembic import op

revision = "20261013_0057"
down_revision = "20261009_0056"
branch_labels = None
depends_on = None


def upgrade():
    # Nullable: existing leases predate per-process holders and are treated as
    # unbound, so they cannot be adopted by a new holder without a fresh
    # ownership check.
    op.execute("SET LOCAL lock_timeout='5s'")
    op.add_column(
        "trading_leases",
        sa.Column("holder_id", sa.String(192), nullable=True),
    )


def downgrade():
    op.drop_column("trading_leases", "holder_id")
