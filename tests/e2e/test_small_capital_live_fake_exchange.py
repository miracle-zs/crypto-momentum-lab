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


async def test_small_capital_live_uses_single_submit_boundary() -> None:
    exchange = FakeExchange(submit_result=_snapshot(ExchangeOrderState.FILLED))
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=FakeOrderRepository(),
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )

    result = await machine.execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.FILLED
    assert exchange.calls == ["submit"]
