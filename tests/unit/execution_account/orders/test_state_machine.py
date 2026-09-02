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
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeOrderQueryUnknownError,
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    OrderExecutionStateMachine,
    OrderPreSubmissionError,
    PreparedOrderSubmission,
    SubmitPolicy,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def test_prepared_submission_does_not_duplicate_write_ahead_journal() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED),
    )
    repository = FakeOrderRepository()
    callback_events: list[ExchangeOrderEvent] = []

    async def on_event(_plan: OrderExecutionPlan, event: ExchangeOrderEvent) -> None:
        callback_events.append(event)

    plan = _plan()
    prepared_event = ExchangeOrderEvent(
        event_id="submitting-1",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.SUBMITTING,
        occurred_at=NOW,
        exchange_order_id=None,
        details={},
    )
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_event=on_event,
        serialize_commands=False,
    )

    result = await machine.execute_approved_intent(
        plan,
        prepared_submission=PreparedOrderSubmission(
            plan=plan,
            submitting_event=prepared_event,
        ),
    )

    assert result.state is ExchangeOrderState.ACKNOWLEDGED
    assert repository.plans == []
    assert [event.state for event in repository.events] == [
        ExchangeOrderState.ACKNOWLEDGED
    ]
    assert [event.state for event in callback_events] == [
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.ACKNOWLEDGED,
    ]


async def test_pre_submission_callback_runs_before_exchange_write() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED),
    )
    repository = FakeOrderRepository()
    phases: list[str] = []

    async def before_submit(_plan: OrderExecutionPlan, _occurred_at: datetime) -> None:
        phases.append("pre_submission")

    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_before_submit=before_submit,
        serialize_commands=False,
    )

    result = await machine.execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.ACKNOWLEDGED
    assert phases == ["pre_submission"]
    assert exchange.calls == ["submit"]


async def test_failed_pre_submission_callback_blocks_exchange_write() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED),
    )
    repository = FakeOrderRepository()

    async def before_submit(
        _plan: OrderExecutionPlan,
        _occurred_at: datetime,
    ) -> None:
        raise OrderPreSubmissionError("account hub unavailable")

    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_before_submit=before_submit,
        serialize_commands=False,
    )

    result = await machine.execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.REJECTED
    assert exchange.calls == []
    assert repository.events[-1].details == {
        "reason": "account hub unavailable",
        "phase": "before_exchange_submit",
    }


async def test_timeout_queries_by_client_order_id_before_retry() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError(),
        query_result=None,
    )
    exchange.query_results = [None, _snapshot(ExchangeOrderState.ACKNOWLEDGED)]
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert exchange.calls == ["submit", "query", "query"]
    assert result.state is ExchangeOrderState.ACKNOWLEDGED


async def test_timeout_with_failed_lookup_is_marked_for_reconciliation() -> None:
    exchange = QueryFailExchange()
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).execute_approved_intent(_plan())

    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert repository.events[-1].details["reason"] == "lookup unavailable"
    assert repository.events[-1].details["reconciliation_attempts"] == 5


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


async def test_reconcile_order_promotes_acknowledged_order_to_filled() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED),
        query_result=_snapshot(ExchangeOrderState.FILLED),
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).reconcile_order(_plan())

    assert exchange.calls == ["query"]
    assert result.state is ExchangeOrderState.FILLED
    assert repository.events[-1].state is ExchangeOrderState.FILLED


async def test_reconcile_order_retries_transient_lookup_until_snapshot() -> None:
    exchange = SequencedQueryExchange(
        query_results=[
            ExchangeOrderQueryUnknownError("lookup timed out"),
            None,
            _snapshot(ExchangeOrderState.FILLED),
        ]
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).reconcile_order(_plan())

    assert exchange.calls == ["query", "query", "query"]
    assert result.state is ExchangeOrderState.FILLED
    assert all(
        event.state is not ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
        for event in repository.events
    )


async def test_cancel_order_persists_cancel_and_returns_partial_fill_quantity() -> None:
    exchange = CancelExchange(
        _snapshot(
            ExchangeOrderState.CANCELED,
            executed_quantity=Decimal("0.0005"),
        )
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).cancel_order(_plan())

    assert exchange.calls == ["cancel"]
    assert result.state is ExchangeOrderState.CANCELED
    assert result.executed_quantity == Decimal("0.0005")
    assert [event.state for event in repository.events] == [
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.CANCELED,
    ]


async def test_cancel_timeout_is_fail_closed() -> None:
    exchange = CancelExchange(ExchangeCancellationUnknownError("timed out"))
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).cancel_order(_plan())

    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert (
        repository.events[-1].state
        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    )


async def test_cancel_timeout_reconciles_before_marking_order_unknown() -> None:
    exchange = CancelExchange(
        ExchangeCancellationUnknownError("timed out"),
        query_results=[None, _snapshot(ExchangeOrderState.CANCELED)],
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).cancel_order(_plan())

    assert exchange.calls == ["cancel", "query", "query"]
    assert result.state is ExchangeOrderState.CANCELED


async def test_cancel_not_found_is_terminal_absent_and_not_rejected() -> None:
    exchange = CancelExchange(
        ExchangeOrderAlreadyAbsentError(
            "Unknown order sent.",
            exchange_code=-2011,
            open_orders_checked=True,
        ),
        query_results=[None, None, None, None, None],
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).cancel_order(_plan())

    assert result.state is ExchangeOrderState.ABSENT_RECONCILED
    assert result.executed_quantity == Decimal("0")
    assert [event.state for event in repository.events] == [
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.ABSENT_RECONCILED,
    ]
    assert repository.events[-1].details == {
        "reason": "Unknown order sent.",
        "exchange_code": -2011,
        "exchange_message": "Unknown order sent.",
        "open_orders_checked": True,
        "reconciliation_reason": "cancel_result_order_not_found",
        "reconciliation_attempts": 5,
        "confirmed_absent": True,
    }


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
        self.query_results: list[ExchangeOrderSnapshot | Exception | None] = []

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
        if self.query_results:
            result = self.query_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return self.query_result


class CancelExchange(FakeExchange):
    def __init__(
        self,
        cancel_result: ExchangeOrderSnapshot | Exception,
        *,
        query_results: list[ExchangeOrderSnapshot | Exception | None] | None = None,
    ) -> None:
        super().__init__(submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED))
        self.cancel_result = cancel_result
        self.query_results = query_results or []

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        del symbol, client_order_id
        self.calls.append("cancel")
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        del symbol, client_order_id
        self.calls.append("query")
        if not self.query_results:
            return None
        result = self.query_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class QueryFailExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__(submit_result=ExchangeSubmissionTimeoutError())

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        del symbol, client_order_id
        raise ExchangeOrderQueryUnknownError("lookup unavailable")


class SequencedQueryExchange(FakeExchange):
    def __init__(
        self,
        *,
        query_results: list[ExchangeOrderSnapshot | Exception | None],
    ) -> None:
        super().__init__(submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED))
        self.query_results = query_results

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        del symbol, client_order_id
        self.calls.append("query")
        result = self.query_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


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
        reconciliation_retry_delays=(0.0, 0.0, 0.0, 0.0),
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
    executed_quantity: Decimal | None = None,
) -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        client_order_id=_plan().client_order_id,
        exchange_order_id="exchange-1",
        state=state,
        observed_at=NOW,
        executed_quantity=(
            executed_quantity
            if executed_quantity is not None
            else Decimal("0.003")
            if fills
            else Decimal("0")
        ),
        average_price=Decimal("30000") if fills else Decimal("0"),
        fills=fills,
    )
