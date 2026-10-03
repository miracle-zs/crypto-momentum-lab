from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    LiveEntryExpectationRegistrar,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_expectation_registry_discard_and_ttl_behavior() -> None:
    clock_time = NOW
    registry = AccountPositionExpectationRegistry(
        environment="live",
        account_label="account-1",
        clock=lambda: clock_time,
    )
    plan = _plan()
    expectation = AccountPositionExpectation.from_plan(
        plan,
        environment="live",
        account_label="account-1",
        registered_at=NOW,
    )
    registry.register(expectation)
    assert registry.pending_count == 1

    # When order is rejected by fence, discard prevents expectation matching
    registry.discard(plan.client_order_id)
    assert registry.pending_count == 0
    assert (
        registry.consume(
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amt=Decimal("0.25"),
            observed_at=NOW,
        )
        is None
    )


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
