"""Repair known legacy WS digests, then keep the trade key and digest.

Revision ID: 20261015_0066
Revises: 20261015_0065
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

import sqlalchemy as sa

from alembic import op

revision: str = "20261015_0066"
down_revision: str | None = "20261015_0065"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_LEGACY_WS_DIGEST_REPAIRS = (
    (
        "live",
        "account-2",
        "WALUSDT",
        "LONG",
        "79880175",
        "1066164265",
        "1667",
        "0.05991",
        "BUY",
        "2026-10-15 04:17:01.374+00",
        "cb0386c702f25b4b9c6dc293b4aa7e94a100129c625d0f7e18bdb1622e8f5837",
        "da60c243139fa576341eefaaed983b8d58f9e402b9596d8ccfd0f330da996471",
    ),
    (
        "live",
        "account-3",
        "WALUSDT",
        "LONG",
        "79880195",
        "1066164411",
        "1667",
        "0.05992",
        "BUY",
        "2026-10-15 04:17:01.622+00",
        "1dbcfdd172c369a2b8b39ed02543fd0ef09030f173fe9192217d706f3131c226",
        "569e9dc122cbc815337086fc02e095b282e3e547a5f59a2982d5ebfb5a90f745",
    ),
    (
        "live",
        "account-4",
        "WALUSDT",
        "LONG",
        "79424638",
        "1062531975",
        "325",
        "0.06123",
        "SELL",
        "2026-10-15 02:45:01.618+00",
        "6affcc67ac1a5d06d0e15e97f23c00c198e72af0bfae882ff75e690d7e751572",
        "c84f7d11e9d82e08255db1f9658959957af709643aa49cf47276a58f08d4b77b",
    ),
    (
        "live",
        "account-4",
        "ZKUSDT",
        "LONG",
        "228488174",
        "3764625760",
        "856",
        "0.017987",
        "BUY",
        "2026-10-15 04:52:03.175+00",
        "8b01e1805debbf7d827a980ddeacc22889e2c17996c4f09e7433a4d22e28c11e",
        "2d944d45e79199092d6190a8035c460456be0285bb302f3da410179ccd1ef7f6",
    ),
    (
        "live",
        "primary",
        "ZKUSDT",
        "LONG",
        "228488176",
        "3764625830",
        "5507",
        "0.017987",
        "BUY",
        "2026-10-15 04:52:03.190+00",
        "bfa1c0dfbc4a801d474ca9daeaa61e5112815e4649fb33c279237a9b692f1b47",
        "48b2667485b59bf17a9c230cf8b047bbe88dae6cb1eee4b2e203bcee37e9fadb",
    ),
)


def _repair_legacy_ws_trade_digests(connection) -> None:
    update = sa.text(
        """
        UPDATE execution_trade_identities
        SET payload_digest = :correct_digest
        WHERE environment = :environment
          AND account_label = :account_label
          AND symbol = :symbol
          AND position_side = :position_side
          AND trade_id = :trade_id
          AND order_id = :order_id
          AND quantity = :quantity
          AND price = :price
          AND side = :side
          AND first_seen_at = :first_seen_at
          AND payload_digest = :legacy_digest
        """
    ).bindparams(
        sa.bindparam("quantity", type_=sa.Numeric(38, 18)),
        sa.bindparam("price", type_=sa.Numeric(38, 18)),
        sa.bindparam("first_seen_at", type_=sa.DateTime(timezone=True)),
    )
    select = sa.text(
        """
        SELECT payload_digest
        FROM execution_trade_identities
        WHERE environment = :environment
          AND account_label = :account_label
          AND symbol = :symbol
          AND position_side = :position_side
          AND trade_id = :trade_id
        """
    )
    for row in _LEGACY_WS_DIGEST_REPAIRS:
        (
            environment,
            account_label,
            symbol,
            position_side,
            trade_id,
            order_id,
            quantity,
            price,
            side,
            first_seen_at,
            legacy_digest,
            correct_digest,
        ) = row
        params = {
            "environment": environment,
            "account_label": account_label,
            "symbol": symbol,
            "position_side": position_side,
            "trade_id": trade_id,
            "order_id": order_id,
            "quantity": Decimal(quantity),
            "price": Decimal(price),
            "side": side,
            "first_seen_at": datetime.fromisoformat(first_seen_at),
            "legacy_digest": legacy_digest,
            "correct_digest": correct_digest,
        }
        result = connection.execute(update, params)
        if result.rowcount == 1:
            continue
        existing = connection.execute(select, params).scalar_one_or_none()
        if existing not in (None, correct_digest):
            raise RuntimeError(
                f"legacy trade digest changed for {account_label}:{symbol}:{trade_id}"
            )


def upgrade() -> None:
    _repair_legacy_ws_trade_digests(op.get_bind())
    op.drop_index(
        "ix_execution_trade_identities_order",
        table_name="execution_trade_identities",
    )
    op.drop_constraint(
        "ck_execution_trade_quantity",
        "execution_trade_identities",
        type_="check",
    )
    op.drop_constraint(
        "ck_execution_trade_price",
        "execution_trade_identities",
        type_="check",
    )
    op.drop_column("execution_trade_identities", "order_id")
    op.drop_column("execution_trade_identities", "quantity")
    op.drop_column("execution_trade_identities", "price")
    op.drop_column("execution_trade_identities", "side")
    op.drop_column("execution_trade_identities", "first_seen_at")


def downgrade() -> None:
    raise RuntimeError(
        "Execution trade detail columns were discarded; restore the pre-0066 "
        "database backup to run an older application."
    )
