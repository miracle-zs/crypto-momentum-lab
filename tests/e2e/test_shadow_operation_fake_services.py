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


async def test_shadow_operation_never_reaches_exchange_write_boundary() -> None:
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
    assert exchange.calls == []
    assert repository.suppressions[0].reason == "shadow_submit_policy"
