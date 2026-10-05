from collections.abc import Sequence
from importlib import import_module

from alembic import op

revision: str = "20261005_0048"
down_revision: str | None = "20261005_0047"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for table in (
        "shadow_drill_results",
        "shadow_decision_metrics",
        "shadow_suppression_events",
        "shadow_order_plans",
        "shadow_sessions",
    ):
        op.drop_table(table)


def downgrade() -> None:
    migration = import_module("alembic.versions.20260704_0009_shadow_operation")
    migration.upgrade()
