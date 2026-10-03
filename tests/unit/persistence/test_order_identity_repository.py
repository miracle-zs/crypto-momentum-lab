from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_read_models import OrderIdentityEvent
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
)
from crypto_momentum_lab.persistence.postgres.order_identity_repository import (
    load_order_identity_metadata,
)


async def test_order_identity_adapter_detaches_events_and_complete_fill_values():
    now = datetime(2026, 9, 30, tzinfo=UTC)
    event = ExchangeOrderEventRow(
        client_order_id="client-1",
        exchange_order_id="order-1",
        state="filled",
        occurred_at=now,
        details={"executed_quantity": "2"},
    )
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
    session.scalars.side_effect = [Mock(all=lambda: [event]), Mock(all=lambda: [fill])]
    order = ExchangeOrderRow(
        client_order_id="client-1",
        exchange_order_id="order-1",
        symbol="BTCUSDT",
        created_at=now,
    )
    metadata = await load_order_identity_metadata(
        session,
        [order],
        account_label="account-3",
    )
    observation = metadata.events_by_client_order_id["client-1"][0]
    assert isinstance(observation, OrderIdentityEvent)
    assert observation.exchange_order_id == "order-1"
    assert observation.details == {"executed_quantity": "2"}
    event.details["executed_quantity"] = "999"
    assert observation.details["executed_quantity"] == "2"
    assert isinstance(metadata.account_fills[0], AccountFillEvent)
    assert metadata.account_fills[0].quantity == Decimal("2")
    assert metadata.account_fills[0].trade_id == "trade-1"
    assert metadata.account_fills[0].raw_payload == {
        "source": "exchange",
        "is_system": True,
    }
    assert session.scalars.await_count == 2


@pytest.mark.parametrize("details", [None, ["malformed"], {"unrelated": "value"}])
def test_malformed_identity_details_remain_missing_fill_evidence(details):
    from crypto_momentum_lab.persistence.postgres.order_identity_repository import (
        order_identity_event,
    )

    event = ExchangeOrderEventRow(
        client_order_id="malformed",
        exchange_order_id="order-1",
        state="filled",
        occurred_at=datetime(2026, 9, 30, tzinfo=UTC),
        details=details,
    )
    observation = order_identity_event(event)
    assert observation.details == (details if isinstance(details, dict) else {})
