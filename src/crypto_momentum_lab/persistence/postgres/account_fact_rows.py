"""Convert stored account facts to domain values without loading a journal store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.account.models import AccountFillEvent

if TYPE_CHECKING:
    from crypto_momentum_lab.persistence.postgres.models import AccountFillEventRow


def account_fill_from_row(row: AccountFillEventRow) -> AccountFillEvent:
    return AccountFillEvent(
        environment=row.environment,
        account_label=row.account_label,
        symbol=row.symbol,
        trade_id=row.trade_id,
        order_id=row.order_id,
        side=row.side,
        price=row.price,
        quantity=row.quantity,
        realized_pnl=row.realized_pnl,
        fee=row.fee,
        fee_asset=row.fee_asset,
        trade_at=row.trade_at,
        raw_payload=row.raw_payload,
    )


