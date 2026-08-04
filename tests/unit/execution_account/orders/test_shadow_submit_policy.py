from dataclasses import replace

import pytest

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from tests.unit.execution_account.orders.test_state_machine import (
    NOW,
    FakeExchange,
    FakeOrderRepository,
    _plan,
    _snapshot,
)


async def test_shadow_submit_policy_records_suppression_without_submit() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )
    repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
        live_submit_enabled=False,
        clock=lambda: NOW,
    )

    result = await machine.execute_approved_intent(_plan())

    assert result.suppressed is True
    assert result.state is ExchangeOrderState.SUPPRESSED
    assert exchange.calls == []
    assert len(repository.suppressions) == 1
    assert repository.suppressions[0].order_payload["symbol"] == "BTCUSDT"


async def test_live_submit_policy_uses_submit_boundary() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )
    repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )

    await machine.execute_approved_intent(_plan())

    assert exchange.calls == ["submit"]


async def test_shadow_policy_still_requires_quantized_order_plan() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )
    repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
        live_submit_enabled=False,
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="must be quantized"):
        await machine.execute_approved_intent(replace(_plan(), quantized=False))

    assert repository.suppressions == []
