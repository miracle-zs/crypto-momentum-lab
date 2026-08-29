"""Install the PostgreSQL query-statistics extension."""

from collections.abc import Sequence

from alembic import op

revision: str = "20260829_0025"
down_revision: str | None = "20260828_0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The Compose service preloads pg_stat_statements before migrations run.
    # CREATE EXTENSION is idempotent so an existing database is safe to upgrade.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS pg_stat_statements")
