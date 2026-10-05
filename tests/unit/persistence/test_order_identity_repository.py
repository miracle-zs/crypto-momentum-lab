from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderRow,
)
from crypto_momentum_lab.persistence.postgres.order_identity_repository import (
    load_position_account_fills,
)


async def test_position_account_fills_map_complete_fill_values():
    now = datetime(2026, 9, 30, tzinfo=UTC)
    fill = AccountFillEventRow(
        environment="live",
        account_label="account-3",
        symbol="BTCUSDT",
        trade_id="trade-1",
        order_id="order-1",
        side="SELL",
        price=Decimal("100"),
        quantity=Decimal("2"),
        realized_pnl=Decimal("1"),
        fee=Decimal("0.1"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"source": "exchange"},
    )
    session = AsyncMock()
    session.scalars.return_value = Mock(all=lambda: [fill])
    order = ExchangeOrderRow(
        client_order_id="client-1",
        exchange_order_id="order-1",
        symbol="BTCUSDT",
        created_at=now,
    )
    account_fills = await load_position_account_fills(
        session,
        [order],
        account_label="account-3",
    )
    assert isinstance(account_fills[0], AccountFillEvent)
    assert account_fills[0].quantity == Decimal("2")
    assert account_fills[0].trade_id == "trade-1"
    assert account_fills[0].raw_payload == {
        "source": "exchange",
        "is_system": True,
    }
    assert session.scalars.await_count == 1
