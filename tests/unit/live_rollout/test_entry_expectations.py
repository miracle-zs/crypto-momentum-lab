from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    LiveEntryExpectationRegistrar,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_registrar_publishes_scoped_expectation_from_entry_plan() -> None:
    published: list[AccountPositionExpectation] = []

    class Publisher:
        async def register(
            self,
            expectation: AccountPositionExpectation,
        ) -> None:
            published.append(expectation)

    registrar = LiveEntryExpectationRegistrar(
        account_event_hub_url="ws://account-hub",
        account_label="account-1",
        publisher=Publisher(),
    )

    await registrar(_plan(), NOW)

    assert len(published) == 1
    expectation = published[0]
    assert expectation.environment == "live"
    assert expectation.account_label == "account-1"
    assert expectation.client_order_id == "entry-1"
    assert expectation.quantity == Decimal("0.25")
    assert expectation.created_at == NOW


@pytest.mark.asyncio
async def test_registrar_fails_closed_when_expectation_publish_fails() -> None:
    class Publisher:
        async def register(
            self,
            _expectation: AccountPositionExpectation,
        ) -> None:
            raise OSError("hub unavailable")

    registrar = LiveEntryExpectationRegistrar(
        account_event_hub_url="ws://account-hub",
        account_label="account-1",
        publisher=Publisher(),
    )

    with pytest.raises(
        OrderPreSubmissionError,
        match="account position expectation registration failed: OSError",
    ):
        await registrar(_plan(), NOW)


def _plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="entry-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.25"),
        price=None,
        reduce_only=False,
        created_at=NOW,
        quantized=True,
    )
