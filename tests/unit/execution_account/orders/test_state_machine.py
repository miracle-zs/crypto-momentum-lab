from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    OrderExecutionStateMachine,
    SubmitPolicy,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def test_timeout_queries_by_client_order_id_before_retry() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError(),
        query_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED),
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert exchange.calls == ["submit", "query"]
    assert result.state is ExchangeOrderState.ACKNOWLEDGED


async def test_clear_reject_persists_rejected_state() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeOrderRejectedError("insufficient margin")
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.REJECTED
    assert repository.events[-1].state is ExchangeOrderState.REJECTED
    assert repository.events[-1].details["reason"] == "insufficient margin"


async def test_partial_fill_remains_unresolved() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.PARTIALLY_FILLED)
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.PARTIALLY_FILLED
    assert not result.state.terminal


async def test_terminal_fill_updates_order_state_and_persists_fill() -> None:
    fill = ExchangeOrderFill(
        fill_id="fill-1",
        client_order_id=_plan().client_order_id,
        exchange_trade_id="trade-1",
        price=Decimal("30000"),
        quantity=Decimal("0.003"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        filled_at=NOW,
        details={},
    )
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.FILLED, fills=(fill,))
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.FILLED
    assert repository.fills == [fill]
    assert repository.events[-1].state is ExchangeOrderState.FILLED


class FakeExchange:
    def __init__(
        self,
        *,
        submit_result: ExchangeOrderSnapshot | Exception,
        query_result: ExchangeOrderSnapshot | None = None,
    ) -> None:
        self.submit_result = submit_result
        self.query_result = query_result
        self.calls: list[str] = []

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        self.calls.append("submit")
        if isinstance(self.submit_result, Exception):
            raise self.submit_result
        return self.submit_result

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        self.calls.append("query")
        return self.query_result


class FakeOrderRepository:
    def __init__(self) -> None:
        self.plans: list[OrderExecutionPlan] = []
        self.events: list[ExchangeOrderEvent] = []
        self.fills: list[ExchangeOrderFill] = []
        self.suppressions: list[ShadowSuppressionEvent] = []

    async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
        self.plans.append(plan)

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        self.events.append(event)
        return True

    async def save_fill(self, fill: ExchangeOrderFill) -> bool:
        self.fills.append(fill)
        return True

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        self.suppressions.append(event)


def _machine(
    exchange: FakeExchange,
    repository: FakeOrderRepository,
) -> OrderExecutionStateMachine:
    return OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )


def _plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="candidate-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=NOW,
        quantized=True,
    )


def _snapshot(
    state: ExchangeOrderState,
    *,
    fills: tuple[ExchangeOrderFill, ...] = (),
) -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        client_order_id=_plan().client_order_id,
        exchange_order_id="exchange-1",
        state=state,
        observed_at=NOW,
        executed_quantity=Decimal("0.003") if fills else Decimal("0"),
        average_price=Decimal("30000") if fills else Decimal("0"),
        fills=fills,
    )
