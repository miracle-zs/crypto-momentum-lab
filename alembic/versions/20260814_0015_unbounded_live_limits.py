from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260814_0015"
down_revision: str | None = "20260813_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for column_name in (
        "max_order_notional",
        "max_gross_notional",
        "max_open_positions",
    ):
        op.alter_column(
            "risk_config_snapshots",
            column_name,
            existing_type=_risk_column_type(column_name),
            nullable=True,
        )
    for column_name in (
        "approved_notional_cap",
        "approved_max_open_positions",
        "expires_at",
    ):
        op.alter_column(
            "live_operator_approvals",
            column_name,
            existing_type=_approval_column_type(column_name),
            nullable=True,
        )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE risk_config_snapshots
            SET max_order_notional = COALESCE(
                    max_order_notional,
                    99999999999999999999.999999999999999999
                ),
                max_gross_notional = COALESCE(
                    max_gross_notional,
                    99999999999999999999.999999999999999999
                ),
                max_open_positions = COALESCE(max_open_positions, 2147483647)
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_operator_approvals
            SET approved_notional_cap = COALESCE(
                    approved_notional_cap,
                    99999999999999999999.999999999999999999
                ),
                approved_max_open_positions = COALESCE(
                    approved_max_open_positions,
                    2147483647
                ),
                expires_at = COALESCE(
                    expires_at,
                    TIMESTAMPTZ '9999-12-31 23:59:59+00'
                )
            """
        )
    )
    for column_name in (
        "approved_notional_cap",
        "approved_max_open_positions",
        "expires_at",
    ):
        op.alter_column(
            "live_operator_approvals",
            column_name,
            existing_type=_approval_column_type(column_name),
            nullable=False,
        )
    for column_name in (
        "max_order_notional",
        "max_gross_notional",
        "max_open_positions",
    ):
        op.alter_column(
            "risk_config_snapshots",
            column_name,
            existing_type=_risk_column_type(column_name),
            nullable=False,
        )


def _risk_column_type(column_name: str) -> sa.types.TypeEngine:
    if column_name == "max_open_positions":
        return sa.Integer()
    return sa.Numeric(38, 18)


def _approval_column_type(column_name: str) -> sa.types.TypeEngine:
    if column_name == "approved_max_open_positions":
        return sa.Integer()
    if column_name == "expires_at":
        return sa.DateTime(timezone=True)
    return sa.Numeric(38, 18)
