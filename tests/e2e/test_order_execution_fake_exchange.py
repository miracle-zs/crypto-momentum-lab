from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeSubmissionTimeoutError,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _machine,
    _plan,
)


async def test_timeout_then_not_found_is_durable_unknown_state() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError(),
        query_result=None,
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert exchange.calls == [
        "submit",
        "query",
        "query",
        "query",
        "query",
        "query",
    ]
    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert (
        repository.events[-1].state
        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    )
