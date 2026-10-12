"""SYSTEM.md section 8 acceptance: order facts, submission uncertainty, exits.

These tests exercise the real execution engine (``OrderExecutionStateMachine``,
``LiveOrderReconciliation``, ``ExecutionBook``/``ReservationRegistry``).  Only
external boundaries are substituted: the exchange network, the clock and the
durable stores (in-memory mirrors of the PostgreSQL repositories, or the real
PostgreSQL test database where durable responsibility must be proven).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeSubmissionTimeoutError,
)
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    Blocked,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    ExitAllocation,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.risk import RiskGateway
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
    live_symbol_position_key,
)
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveSubmissionConfig,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExecutionCommandRow,
    PositionReservationRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.fixtures.live_market import NOW as MARKET_NOW
from tests.fixtures.live_market import _intent, _state
from tests.fixtures.prepared_submission import submit_prepared
from tests.integration.persistence.test_authority_book_transactions import (
    _book as _authority_book,
)
from tests.integration.persistence.test_order_terminal_convergence import (
    _setup_book_and_repos,
)
from tests.unit.execution_account.orders.test_coordinator import (
    OrderExecutionCoordinator as MemoryOrderExecutionCoordinator,
)
from tests.unit.execution_account.orders.test_coordinator import _submission_preparation
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _machine,
)
from tests.unit.live_rollout.test_daemon import _runtime_context
from tests.unit.live_rollout.test_lock_free_execution import RecordingPreparedRepo
from tests.unit.live_rollout.test_order_reconciliation import _ws_order_case

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)
ACCOUNT = "acceptance"


# --------------------------------------------------------------------------- #
# Shared wiring
# --------------------------------------------------------------------------- #
class UniqueOrderEventRepository(FakeOrderRepository):
    """``FakeOrderRepository`` plus the durable unique-constraint behavior.

    The real PostgreSQL repository deduplicates an observation by ``event_id``
    and a fill by ``(client_order_id, exchange_trade_id)``.  This subclass adds
    only that storage-level idempotence so a duplicate fact can be observed to
    be counted once; it holds no decision, settlement or recovery logic.
    """

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        if any(existing.event_id == event.event_id for existing in self.events):
            return False
        self.events.append(event)
        return True

    async def save_fill(self, fill) -> bool:
        if any(
            existing.client_order_id == fill.client_order_id
            and existing.exchange_trade_id == fill.exchange_trade_id
            for existing in self.fills
        ):
            return False
        self.fills.append(fill)
        return True


class ScenarioOrderRepository:
    """Read-only durable-order boundary returning one scenario snapshot."""

    def __init__(self, order: PersistedExchangeOrder | None = None) -> None:
        self._order = order

    async def load_order(self, client_order_id: str):
        if self._order is None or self._order.plan.client_order_id != client_order_id:
            return None
        return self._order

    async def load_unresolved_orders(self, run_id: str | None = None):
        return ()


def _ws_machine(
    exchange: FakeExchange,
    repository: FakeOrderRepository,
    *,
    applied: list[ExchangeOrderEvent] | None = None,
) -> OrderExecutionStateMachine:
    async def on_event(_plan: OrderExecutionPlan, event: ExchangeOrderEvent) -> None:
        if applied is not None:
            applied.append(event)

    return OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=repository,
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_event=on_event,
    )


def _reconciliation(
    order: PersistedExchangeOrder | None,
    machine: OrderExecutionStateMachine,
    *,
    execution_book: ExecutionBook | None = None,
) -> LiveOrderReconciliation:
    return LiveOrderReconciliation(
        order_repository=ScenarioOrderRepository(order),
        state_machine=machine,
        run_id="run-1",
        execution_book=execution_book,
    )


def _family_status(reconciliation: LiveOrderReconciliation) -> dict[str, dict]:
    return {status["family"]: status for status in reconciliation.recovery_status()}


def _scope(
    symbol: str = "BTCUSDT",
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH,
) -> ExecutionScope:
    return ExecutionScope(
        environment="live",
        account_label=ACCOUNT,
        symbol=symbol,
        position_side=position_side,
    )


def _entry_command(
    command_id: str,
    key: PositionKey,
    quantity: Decimal,
    *,
    created_at: datetime = NOW,
) -> TradeCommand:
    return TradeCommand(
        command_id=command_id,
        position_key=key,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.LIMIT,
        requested_quantity=quantity,
        limit_price=Decimal("100"),
        created_at=created_at,
    )


def _book_with_unsent_entry(
    *,
    symbol: str = "BTCUSDT",
    command_id: str = "entry-1",
    quantity: Decimal = Decimal("10"),
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH,
) -> tuple[ExecutionBook, PositionKey, PositionReservation]:
    """Register an accepted-but-unsent entry command with its reservation."""
    book = ExecutionBook()
    key = _scope(symbol, position_side).to_position_key()
    reservation = PositionReservation(
        reservation_id=f"res-{command_id}",
        command_id=command_id,
        position_key=key,
        batch_id=f"batch-{command_id}",
        reserved_quantity=quantity,
        created_at=NOW,
    )
    book.coordinator.register_reservation(reservation)
    book.register_prepared_command(
        _entry_command(command_id, key, quantity),
        _scope(symbol, position_side),
        [reservation.reservation_id],
    )
    return book, key, reservation


def _fill(
    *,
    trade_id: str,
    order_id: str,
    quantity: Decimal,
    symbol: str = "BTCUSDT",
    account_label: str = ACCOUNT,
    position_side: str = "BOTH",
    price: Decimal = Decimal("100"),
    trade_at: datetime = NOW,
    reduce_only: bool = False,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label=account_label,
        symbol=symbol,
        trade_id=trade_id,
        order_id=order_id,
        side="BUY",
        price=price,
        quantity=quantity,
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=trade_at,
        raw_payload={
            "positionSide": position_side,
            **({"reduce_only": True} if reduce_only else {}),
        },
    )


def _order_event(
    *,
    command_id: str,
    state: ExchangeOrderState,
    occurred_at: datetime,
    executed_quantity: str,
    quote_quantity: str,
    exchange_order_id: str = "ex-1",
) -> ExchangeOrderEvent:
    return ExchangeOrderEvent(
        event_id=f"order-event-{command_id}-{state.value}-{occurred_at.isoformat()}",
        client_order_id=command_id,
        state=state,
        occurred_at=occurred_at,
        exchange_order_id=exchange_order_id,
        details={
            "executed_quantity": executed_quantity,
            "cumulative_quote_quantity": quote_quantity,
            "average_price": "100",
            "is_reduce_only": False,
        },
    )


# --------------------------------------------------------------------------- #
# Row: WS terminal before REST ACK; duplicates and lower cumulative quantity
# --------------------------------------------------------------------------- #
async def test_mixed_clock_terminal_ws_fact_is_applied_before_the_rest_ack() -> None:
    """A correct-identity terminal fact wins even when its clock is earlier."""
    order, event = _ws_order_case()
    rest_ack_at = order.updated_at + timedelta(seconds=1)
    order = PersistedExchangeOrder(
        plan=order.plan,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="123",
        updated_at=rest_ack_at,
        executed_quantity=Decimal("0"),
    )
    exchange = FakeExchange(submit_result=order.plan)
    repository = UniqueOrderEventRepository()
    applied: list[ExchangeOrderEvent] = []
    machine = _ws_machine(exchange, repository, applied=applied)
    reconciliation = _reconciliation(order, machine)

    terminal_at = order.updated_at - timedelta(seconds=1)
    event.order_update.update(
        X="CANCELED",
        z="0",
        T=int(terminal_at.timestamp() * 1000),
    )

    await reconciliation.reconcile_account_event(event)

    assert [recorded.state for recorded in repository.events] == [
        ExchangeOrderState.CANCELED
    ]
    assert repository.events[0].occurred_at == terminal_at
    assert repository.events[0].occurred_at < rest_ack_at
    assert [recorded.state for recorded in applied] == [ExchangeOrderState.CANCELED]
    # A complete WS fact must not trigger any REST traffic.
    assert exchange.calls == []


async def test_duplicate_terminal_updates_are_counted_exactly_once() -> None:
    order, event = _ws_order_case()
    order = PersistedExchangeOrder(
        plan=order.plan,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="123",
        updated_at=NOW - timedelta(seconds=5),
        executed_quantity=Decimal("0"),
    )
    exchange = FakeExchange(submit_result=order.plan)
    repository = UniqueOrderEventRepository()
    applied: list[ExchangeOrderEvent] = []
    machine = _ws_machine(exchange, repository, applied=applied)
    reconciliation = _reconciliation(order, machine)

    event.order_update.update(X="FILLED", z="2", ap="100")

    await reconciliation.reconcile_account_event(event)
    await reconciliation.reconcile_account_event(event)

    assert [recorded.state for recorded in repository.events] == [
        ExchangeOrderState.FILLED
    ]
    assert [recorded.state for recorded in applied] == [ExchangeOrderState.FILLED]
    assert repository.events[0].details["executed_quantity"] == "2"
    assert exchange.calls == []


async def test_lower_cumulative_terminal_never_rolls_back_and_requests_repair() -> None:
    order, event = _ws_order_case()
    order = PersistedExchangeOrder(
        plan=replace(order.plan, quantity=Decimal("10")),
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="123",
        updated_at=NOW,
        executed_quantity=Decimal("5"),
    )
    exchange = FakeExchange(submit_result=order.plan)
    repository = UniqueOrderEventRepository()
    applied: list[ExchangeOrderEvent] = []
    machine = _ws_machine(exchange, repository, applied=applied)
    reconciliation = _reconciliation(order, machine)

    # Exchange reports the order canceled with only 3 executed, while the local
    # watermark already knows about 5.
    event.order_update.update(q="10", X="CANCELED", z="3", ap="100")

    await reconciliation.reconcile_account_event(event)

    assert repository.events == []
    assert applied == []
    assert exchange.calls == []
    assert _family_status(reconciliation)["orders"]["requested"] is True


async def test_wrong_exchange_identity_is_rejected_and_never_applied() -> None:
    order, event = _ws_order_case()
    order = PersistedExchangeOrder(
        plan=order.plan,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="123",
        updated_at=NOW,
        executed_quantity=Decimal("0"),
    )
    exchange = FakeExchange(submit_result=order.plan)
    repository = UniqueOrderEventRepository()
    applied: list[ExchangeOrderEvent] = []
    machine = _ws_machine(exchange, repository, applied=applied)
    reconciliation = _reconciliation(order, machine)

    event.order_update.update(i=999999)

    with pytest.raises(ValueError, match="conflicts with its durable identity"):
        await reconciliation.reconcile_account_event(event)

    assert repository.events == []
    assert applied == []
    assert exchange.calls == []
    assert _family_status(reconciliation)["orders"]["requested"] is False


# --------------------------------------------------------------------------- #
# Row: quantity 10 fills 3 then cancels; a late fill after the terminal state
# --------------------------------------------------------------------------- #
async def test_partial_fill_then_cancel_releases_only_the_proven_unfilled_seven() -> (
    None
):
    book, key, reservation = _book_with_unsent_entry(quantity=Decimal("10"))
    scope = _scope()
    command_id = "entry-1"

    filled = await book.observe(
        ExecutionEvidence(
            evidence_id="fill-3",
            scope=scope,
            observed_at=NOW + timedelta(seconds=10),
            fill=_fill(
                trade_id="trade-1",
                order_id=command_id,
                quantity=Decimal("3"),
            ),
            cumulative_order=ExecutionCumulativeOrderReport(
                order_id=command_id,
                cumulative_quantity=Decimal("3"),
                cumulative_quote=Decimal("300"),
                observed_at=NOW + timedelta(seconds=10),
            ),
        )
    )
    assert isinstance(filled, Applied)
    assert filled.consumed_quantity == Decimal("3")
    settled = book.coordinator.get_reservation(reservation.reservation_id)
    assert settled is not None
    assert settled.consumed_quantity == Decimal("3")
    assert settled.active_quantity == Decimal("7")

    canceled = await book.observe(
        ExecutionEvidence(
            evidence_id="cancel-3",
            scope=scope,
            observed_at=NOW + timedelta(seconds=20),
            order_event=_order_event(
                command_id=command_id,
                state=ExchangeOrderState.CANCELED,
                occurred_at=NOW + timedelta(seconds=20),
                executed_quantity="3",
                quote_quantity="300",
            ),
        )
    )
    assert isinstance(canceled, Applied)
    assert canceled.released_quantity == Decimal("7")

    released = book.coordinator.get_reservation(reservation.reservation_id)
    assert released is not None
    assert released.consumed_quantity == Decimal("3")
    assert released.released_quantity == Decimal("7")
    assert released.active_quantity == Decimal("0")
    assert book.get_active_reservations(key) == ()
    assert book.get_outbox(command_id).state is DispatchState.TERMINAL

    view = await book.read(scope)
    assert view.total_quantity == Decimal("3")


async def test_late_fill_after_terminal_is_booked_once_and_keeps_exit_duty() -> None:
    book, key, reservation = _book_with_unsent_entry(quantity=Decimal("10"))
    scope = _scope()
    command_id = "entry-1"

    await book.observe(
        ExecutionEvidence(
            evidence_id="fill-3",
            scope=scope,
            observed_at=NOW + timedelta(seconds=10),
            fill=_fill(trade_id="trade-1", order_id=command_id, quantity=Decimal("3")),
            cumulative_order=ExecutionCumulativeOrderReport(
                order_id=command_id,
                cumulative_quantity=Decimal("3"),
                cumulative_quote=Decimal("300"),
                observed_at=NOW + timedelta(seconds=10),
            ),
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="cancel-3",
            scope=scope,
            observed_at=NOW + timedelta(seconds=20),
            order_event=_order_event(
                command_id=command_id,
                state=ExchangeOrderState.CANCELED,
                occurred_at=NOW + timedelta(seconds=20),
                executed_quantity="3",
                quote_quantity="300",
            ),
        )
    )

    late = _fill(
        trade_id="trade-late",
        order_id=command_id,
        quantity=Decimal("2"),
        trade_at=NOW + timedelta(seconds=30),
    )
    evidence = ExecutionEvidence(
        evidence_id="late-fill-2",
        scope=scope,
        observed_at=NOW + timedelta(seconds=30),
        fill=late,
    )

    applied = await book.observe(evidence)
    assert isinstance(applied, Applied)
    assert applied.consumed_quantity == Decimal("0")
    assert applied.released_quantity == Decimal("0")

    released = book.coordinator.get_reservation(reservation.reservation_id)
    assert released is not None
    assert released.consumed_quantity == Decimal("3")
    assert released.released_quantity == Decimal("7")

    view = await book.read(scope)
    assert view.total_quantity == Decimal("5")
    # The late units still carry exit responsibility: they sit in a batch for
    # which no exit order has been submitted.
    assert sum(
        (
            batch.quantity
            for batch in view.batches
            if batch.exit_order_submitted_at is None
        ),
        Decimal("0"),
    ) == Decimal("5")

    replayed = await book.observe(evidence)
    assert isinstance(replayed, Duplicate)
    assert (await book.read(scope)).total_quantity == Decimal("5")


# --------------------------------------------------------------------------- #
# Row: POST timeout or caller cancellation, hanging GET, same-key exit
# --------------------------------------------------------------------------- #
def _exit_plan(
    *,
    command_id: str,
    symbol: str,
    quantity: Decimal,
    allocation: ExitAllocation,
    view_token: str,
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH,
) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{command_id}",
        run_id="run-1",
        client_order_id=f"cml-{command_id}"[:36],
        symbol=symbol,
        side="SELL",
        order_type="MARKET",
        quantity=quantity,
        price=None,
        reduce_only=True,
        created_at=NOW,
        position_side=position_side,
        quantized=True,
        batch_id=allocation.batch_id,
        allocations=(allocation,),
        projection_version=view_token,
        strategy_name="orderflow_impulse",
        strategy_version="v1",
    )


async def _open_position(
    book: ExecutionBook,
    *,
    symbol: str = "BTCUSDT",
    quantity: Decimal = Decimal("2"),
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH,
) -> ExitAllocation:
    scope = _scope(symbol, position_side)
    applied = await book.observe(
        ExecutionEvidence(
            evidence_id=f"open-{symbol}-{quantity}",
            scope=scope,
            observed_at=NOW,
            fill=_fill(
                trade_id=f"open-trade-{symbol}",
                order_id=f"open-order-{symbol}",
                quantity=quantity,
                symbol=symbol,
            ),
        )
    )
    assert isinstance(applied, Applied)
    view = await book.read(scope)
    assert view.total_quantity == quantity
    batch = view.batches[0]
    return ExitAllocation(
        batch_id=batch.batch_id,
        allocated_quantity=quantity,
        entry_price=batch.entry_price,
    )


class _TimeoutExitExchange:
    """Exchange boundary: exit POST times out, order lookup can hang."""

    def __init__(
        self,
        *,
        hang_lookup: bool = False,
        acknowledge: frozenset[str] = frozenset(),
    ) -> None:
        self.entry_posts = 0
        self.exit_posts = 0
        self.lookup_started = asyncio.Event()
        self.lookup_release = asyncio.Event()
        self.hang_lookup = hang_lookup
        self.acknowledge = acknowledge
        self.calls: list[str] = []

    def set_exchange_boundary_callbacks(self, *, on_request=None, on_response=None):
        return None

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        if not plan.reduce_only:
            self.entry_posts += 1
            self.calls.append("submit_entry")
            raise ExchangeSubmissionTimeoutError("entry POST timed out")
        self.exit_posts += 1
        self.calls.append(f"submit_exit:{plan.client_order_id}")
        if plan.client_order_id in self.acknowledge:
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id=f"ex-{plan.client_order_id}",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )
        raise ExchangeSubmissionTimeoutError("exit POST timed out")

    async def query_order_by_client_id(self, symbol: str, client_order_id: str):
        self.calls.append(f"query:{client_order_id}")
        self.lookup_started.set()
        if self.hang_lookup:
            await asyncio.wait_for(self.lookup_release.wait(), timeout=5.0)
        return None


def _memory_coordinator(
    book: ExecutionBook,
    exchange,
    *,
    retry_lookup_on_submit_timeout: bool = True,
    repository=None,
) -> MemoryOrderExecutionCoordinator:
    state_machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=FakeOrderRepository(),
        live_submit_enabled=True,
        clock=lambda: NOW,
        retry_lookup_on_submit_timeout=retry_lookup_on_submit_timeout,
        reconciliation_retry_delays=(0.0, 0.0),
    )
    coordinator = MemoryOrderExecutionCoordinator(
        environment="live",
        backend=state_machine,
        account_label=ACCOUNT,
        execution_book=book,
    )
    coordinator.configure_submission(
        repository or RecordingPreparedRepo(), clock=lambda: NOW
    )
    return coordinator


def _exit_request(
    *,
    plan: OrderExecutionPlan,
    scope: ExecutionScope,
    allocation: ExitAllocation,
    view_token: str,
) -> ExecutionRequest:
    return ExecutionRequest(
        request_id=plan.client_order_id,
        scope=scope,
        strategy_name="orderflow_impulse",
        run_id=plan.run_id,
        decision_ref=plan.client_order_id,
        expected_view_token=view_token,
        action=TradeCommandType.EXIT,
        requested_quantity=allocation.allocated_quantity,
        side=StrategySide.LONG,
        order_type="MARKET",
        reduce_only=True,
        target_batch_ids=(allocation.batch_id,),
        batch_quantities={allocation.batch_id: allocation.allocated_quantity},
        exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
        created_at=NOW,
    )


async def _accept_exit(
    book: ExecutionBook,
    *,
    plan: OrderExecutionPlan,
    scope: ExecutionScope,
    allocation: ExitAllocation,
):
    """Accept one exit command through the real Book acceptance path."""
    view = await book.read(scope)
    accepted = await book.act(
        _exit_request(
            plan=plan,
            scope=scope,
            allocation=allocation,
            view_token=view.projection_version,
        )
    )
    assert isinstance(accepted, Accepted)
    return accepted


async def test_exit_post_timeout_posts_once_and_keeps_unknown_with_reservation() -> (
    None
):
    book = ExecutionBook()
    allocation = await _open_position(book, quantity=Decimal("2"))
    exchange = _TimeoutExitExchange()
    coordinator = _memory_coordinator(book, exchange)
    try:
        view = await book.read(_scope())
        plan = _exit_plan(
            command_id="exit-unknown",
            symbol="BTCUSDT",
            quantity=Decimal("1"),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
            view_token=view.projection_version,
        )
        await _accept_exit(
            book,
            plan=plan,
            scope=_scope(),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
        )

        result = await coordinator.prepare_and_execute(
            plan, preparation=_submission_preparation(plan)
        )

        assert result is not None
        assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
        assert exchange.exit_posts == 1
        assert exchange.calls.count(f"submit_exit:{plan.client_order_id}") == 1
        entry = book.get_outbox(plan.client_order_id)
        assert entry is not None
        assert entry.state is DispatchState.UNKNOWN
        # The reservation for the unproven exit is retained, not released.
        reservation = book.get_active_reservations(_scope().to_position_key())
        assert len(reservation) == 1
        assert reservation[0].active_quantity == Decimal("1")
        assert reservation[0].consumed_quantity == Decimal("0")
        assert reservation[0].released_quantity == Decimal("0")
        assert book.command_requires_recovery(plan.client_order_id) is True
    finally:
        await coordinator.aclose()


async def test_proven_remaining_exit_dispatches_while_same_key_lookup_hangs() -> None:
    book = ExecutionBook()
    allocation = await _open_position(book, quantity=Decimal("2"))
    key = _scope().to_position_key()
    exchange = _TimeoutExitExchange(
        hang_lookup=True, acknowledge=frozenset({"cml-exit-proven"})
    )
    coordinator = _memory_coordinator(
        book, exchange, retry_lookup_on_submit_timeout=False
    )
    try:
        view = await book.read(_scope())
        unknown_plan = _exit_plan(
            command_id="exit-unknown",
            symbol="BTCUSDT",
            quantity=Decimal("1"),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
            view_token=view.projection_version,
        )
        await _accept_exit(
            book,
            plan=unknown_plan,
            scope=_scope(),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
        )
        result = await coordinator.prepare_and_execute(
            unknown_plan, preparation=_submission_preparation(unknown_plan)
        )
        assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
        assert exchange.exit_posts == 1

        # The recovery GET for the unknown exit starts and hangs.
        lookup_task = asyncio.create_task(coordinator.reconcile_order(unknown_plan))
        await asyncio.wait_for(exchange.lookup_started.wait(), timeout=2.0)

        # The remaining proven quantity can still be dispatched for the same key
        # while the network wait is outstanding.
        remaining = await book.read(_scope())
        proven_plan = _exit_plan(
            command_id="exit-proven",
            symbol="BTCUSDT",
            quantity=Decimal("1"),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
            view_token=remaining.projection_version,
        )
        await _accept_exit(
            book,
            plan=proven_plan,
            scope=_scope(),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
        )
        proven_result = await asyncio.wait_for(
            coordinator.prepare_and_execute(
                proven_plan, preparation=_submission_preparation(proven_plan)
            ),
            timeout=2.0,
        )

        assert proven_result is not None
        assert proven_result.state is ExchangeOrderState.ACKNOWLEDGED
        assert exchange.exit_posts == 2
        assert exchange.lookup_release.is_set() is False
        active = book.get_active_reservations(key)
        assert sum(
            (reservation.active_quantity for reservation in active), Decimal("0")
        ) == Decimal("2")

        exchange.lookup_release.set()
        await asyncio.wait_for(lookup_task, timeout=2.0)
    finally:
        exchange.lookup_release.set()
        await coordinator.aclose()


async def test_caller_cancellation_after_post_keeps_one_post_and_unknown_state() -> (
    None
):
    post_started = asyncio.Event()
    post_release = asyncio.Event()
    fact_applied = asyncio.Event()

    class CancellableExchange:
        def __init__(self) -> None:
            self.posts = 0

        def set_exchange_boundary_callbacks(self, *, on_request=None, on_response=None):
            return None

        async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
            self.posts += 1
            post_started.set()
            await asyncio.wait_for(post_release.wait(), timeout=5.0)
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="ex-cancel",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

        async def query_order_by_client_id(self, symbol: str, client_order_id: str):
            return None

    async def on_event(_plan: OrderExecutionPlan, event: ExchangeOrderEvent) -> None:
        if event.state is ExchangeOrderState.ACKNOWLEDGED:
            fact_applied.set()

    book = ExecutionBook()
    allocation = await _open_position(book, quantity=Decimal("2"))
    exchange = CancellableExchange()
    state_machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=FakeOrderRepository(),
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_event=on_event,
    )
    coordinator = MemoryOrderExecutionCoordinator(
        environment="live",
        backend=state_machine,
        account_label=ACCOUNT,
        execution_book=book,
    )
    coordinator.configure_submission(RecordingPreparedRepo(), clock=lambda: NOW)
    try:
        view = await book.read(_scope())
        plan = _exit_plan(
            command_id="exit-cancel",
            symbol="BTCUSDT",
            quantity=Decimal("1"),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
            view_token=view.projection_version,
        )
        await _accept_exit(
            book,
            plan=plan,
            scope=_scope(),
            allocation=replace(allocation, allocated_quantity=Decimal("1")),
        )
        task = asyncio.create_task(
            coordinator.prepare_and_execute(
                plan, preparation=_submission_preparation(plan)
            )
        )
        await asyncio.wait_for(post_started.wait(), timeout=2.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert exchange.posts == 1

        # The in-flight POST cannot be erased by the caller cancel: its fact is
        # still applied exactly once and the reservation is untouched.
        post_release.set()
        await asyncio.wait_for(fact_applied.wait(), timeout=2.0)

        assert exchange.posts == 1
        entry = book.get_outbox(plan.client_order_id)
        assert entry is not None
        assert entry.state is DispatchState.ACKNOWLEDGED
        reservation = book.get_active_reservations(_scope().to_position_key())
        assert len(reservation) == 1
        assert reservation[0].active_quantity == Decimal("1")
        assert reservation[0].released_quantity == Decimal("0")
    finally:
        post_release.set()
        await coordinator.aclose()


async def test_lost_submit_response_recovers_by_lookup_without_second_post() -> None:
    order, _event = _ws_order_case()
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError("response lost"),
        query_result=ExchangeOrderSnapshot(
            client_order_id=order.plan.client_order_id,
            exchange_order_id="123",
            state=ExchangeOrderState.ACKNOWLEDGED,
            observed_at=NOW,
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
        ),
    )
    repository = UniqueOrderEventRepository()

    result = await submit_prepared(_machine(exchange, repository), order.plan)

    assert result.state is ExchangeOrderState.ACKNOWLEDGED
    assert exchange.calls == ["submit", "query"]
    assert [recorded.state for recorded in repository.events] == [
        ExchangeOrderState.ACKNOWLEDGED
    ]


async def test_order_not_found_is_uncertainty_and_not_terminal_proof() -> None:
    order, _event = _ws_order_case()
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError("POST timed out"),
        query_result=None,
    )
    repository = UniqueOrderEventRepository()
    machine = _machine(exchange, repository)

    result = await submit_prepared(machine, order.plan)

    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert result.state.terminal is False
    assert [recorded.state for recorded in repository.events] == [
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    ]

    # Absence only becomes terminal through an explicit proof.
    proven = await machine.mark_absent_reconciled(
        order.plan, details={"reason": "operator_confirmed_absent"}
    )
    assert proven.state is ExchangeOrderState.ABSENT_RECONCILED
    assert [recorded.state for recorded in repository.events] == [
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        ExchangeOrderState.ABSENT_RECONCILED,
    ]


# --------------------------------------------------------------------------- #
# Row: an unsent entry candidate expires or its projection normally advances
# --------------------------------------------------------------------------- #
def _live_submission(
    coordinator: MemoryOrderExecutionCoordinator,
) -> LiveCandidateSubmission:
    return LiveCandidateSubmission(
        risk_gateway=RiskGateway(
            limits=FixedLiveLimits(
                notional_cap=Decimal("100000"),
                max_open_positions=10,
                max_daily_loss=Decimal("10000"),
                max_gross_exposure=Decimal("200000"),
            )
        ),
        state_machine=coordinator,
        config=LiveSubmissionConfig(
            run_id="run-1",
            account_label=ACCOUNT,
            resize_tolerance=Decimal("0.05"),
            hedge_mode=False,
            entry_order_type=EntryType.MARKET,
            entry_limit_ttl_seconds=605,
        ),
        clock=lambda: MARKET_NOW,
        pending_entry_reservation=lambda orders: (Decimal("0"), frozenset()),
        pending_entry_plans=lambda: (),
        pending_entry_uncertainty=lambda symbol: False,
        remember_pending_entry=lambda plan, result: None,
        record_signal_candidate=lambda **kwargs: None,
        position_locks=PositionLifecycleLocks(),
    )


async def test_expired_unsent_candidate_posts_nothing_and_starts_no_recovery() -> None:
    book = ExecutionBook()
    exchange = _TimeoutExitExchange()
    coordinator = _memory_coordinator(book, exchange)
    reconciliation = _reconciliation(None, coordinator, execution_book=book)
    submission = _live_submission(coordinator)
    context = replace(
        _runtime_context(),
        risk_config=replace(
            _runtime_context().risk_config,
            max_open_positions=10,
            max_order_notional=Decimal("10000"),
            max_gross_notional=Decimal("100000"),
        ),
    )
    try:
        expired = replace(
            _intent(),
            symbol="BTCUSDT",
            created_at=MARKET_NOW - timedelta(minutes=10),
            expires_at=MARKET_NOW - timedelta(seconds=1),
        )

        assert (
            await submission.execute(
                expired,
                requested_quantity=None,
                state=_state(),
                context=context,
            )
            is None
        )
        # A repeated evaluation of the same expired signal must not chase it.
        assert (
            await submission.execute(
                expired,
                requested_quantity=None,
                state=_state(),
                context=context,
            )
            is None
        )

        assert exchange.entry_posts == 0
        assert exchange.calls == []
        assert book.list_outbox() == ()
        assert book.get_active_reservations() == ()
        assert {status["requested"] for status in reconciliation.recovery_status()} == {
            False
        }
    finally:
        await coordinator.aclose()


async def test_unsent_command_settles_its_reservation_once_across_normal_change() -> (
    None
):
    book, key, reservation = _book_with_unsent_entry(quantity=Decimal("10"))
    scope = _scope()
    command_id = "entry-1"
    exchange = FakeExchange(submit_result=OrderExecutionPlan)

    token_before = (await book.read(scope)).projection_version
    entry = book.get_outbox(command_id)
    assert entry is not None
    assert entry.state is DispatchState.PREPARED
    assert entry.attempt_count == 0

    # A normal projection advance (a new real fill) is not a fact conflict.
    applied = await book.observe(
        ExecutionEvidence(
            evidence_id="normal-advance",
            scope=scope,
            observed_at=NOW + timedelta(seconds=5),
            fill=_fill(
                trade_id="trade-other",
                order_id="other-order",
                quantity=Decimal("1"),
            ),
        )
    )
    assert isinstance(applied, Applied)
    token_after = (await book.read(scope)).projection_version
    assert token_after != token_before
    assert book.get_outbox(command_id).state is DispatchState.PREPARED
    # Nothing was sent and no exchange call was made for the unsent command.
    assert exchange.calls == []

    released = await book.mark_rejected(
        command_id, reason="entry_candidate_expired_unsent"
    )
    assert released.state is DispatchState.REJECTED
    settled = book.coordinator.get_reservation(reservation.reservation_id)
    assert settled is not None
    assert settled.consumed_quantity == Decimal("0")
    assert settled.released_quantity == Decimal("10")
    assert settled.active_quantity == Decimal("0")
    assert book.get_active_reservations(key) == ()

    replayed = await book.mark_rejected(
        command_id, reason="entry_candidate_expired_unsent"
    )
    assert replayed.state is DispatchState.REJECTED
    assert replayed.updated_at == released.updated_at
    still = book.coordinator.get_reservation(reservation.reservation_id)
    assert still is not None
    assert still.released_quantity == Decimal("10")
    assert book.command_requires_recovery(command_id) is False


# --------------------------------------------------------------------------- #
# Row: two exits racing for one quantity; CAS/commit failure; cross-key state
# --------------------------------------------------------------------------- #
async def test_two_exits_racing_for_the_same_quantity_cannot_exceed_reservation() -> (
    None
):
    book = ExecutionBook()
    allocation = await _open_position(book, quantity=Decimal("10"))
    scope = _scope()
    key = scope.to_position_key()
    token = (await book.read(scope)).projection_version

    first = _exit_plan(
        command_id="exit-race-1",
        symbol="BTCUSDT",
        quantity=Decimal("10"),
        allocation=allocation,
        view_token=token,
    )
    second = _exit_plan(
        command_id="exit-race-2",
        symbol="BTCUSDT",
        quantity=Decimal("10"),
        allocation=allocation,
        view_token=token,
    )

    results = await asyncio.gather(
        book.act(
            _exit_request(
                plan=first, scope=scope, allocation=allocation, view_token=token
            )
        ),
        book.act(
            _exit_request(
                plan=second, scope=scope, allocation=allocation, view_token=token
            )
        ),
    )

    accepted = [result for result in results if isinstance(result, Accepted)]
    refused = [result for result in results if not isinstance(result, Accepted)]
    assert len(accepted) == 1
    assert len(refused) == 1
    assert isinstance(refused[0], Blocked)
    assert "insufficient available quantity" in refused[0].reason

    winner = accepted[0].receipt.command.command_id
    loser = (
        second.client_order_id
        if winner == first.client_order_id
        else first.client_order_id
    )
    active = book.get_active_reservations(key)
    assert len(active) == 1
    assert sum(
        (reservation.active_quantity for reservation in active), Decimal("0")
    ) == Decimal("10")
    assert active[0].command_id == winner
    assert all(reservation.command_id != loser for reservation in active)
    assert (await book.read(scope)).total_quantity == Decimal("10")


async def test_cross_key_candidate_does_not_pollute_other_position_state() -> None:
    book = ExecutionBook()
    btc_allocation = await _open_position(
        book, symbol="BTCUSDT", quantity=Decimal("10")
    )
    eth_allocation = await _open_position(book, symbol="ETHUSDT", quantity=Decimal("4"))
    btc_scope = _scope("BTCUSDT")
    eth_scope = _scope("ETHUSDT")
    btc_key = btc_scope.to_position_key()
    eth_key = eth_scope.to_position_key()

    # The ETHUSDT command is accepted first and must stay untouched.
    eth_plan = _exit_plan(
        command_id="exit-eth",
        symbol="ETHUSDT",
        quantity=Decimal("4"),
        allocation=eth_allocation,
        view_token=(await book.read(eth_scope)).projection_version,
    )
    await _accept_exit(book, plan=eth_plan, scope=eth_scope, allocation=eth_allocation)
    eth_token = (await book.read(eth_scope)).projection_version
    eth_entry = book.get_outbox(eth_plan.client_order_id)
    assert eth_entry is not None
    eth_reservations = book.get_active_reservations(eth_key)
    assert len(eth_reservations) == 1

    # A candidate for another key is accepted while ETHUSDT state is retained.
    btc_plan = _exit_plan(
        command_id="exit-btc",
        symbol="BTCUSDT",
        quantity=Decimal("3"),
        allocation=replace(btc_allocation, allocated_quantity=Decimal("3")),
        view_token=(await book.read(btc_scope)).projection_version,
    )
    await _accept_exit(
        book,
        plan=btc_plan,
        scope=btc_scope,
        allocation=replace(btc_allocation, allocated_quantity=Decimal("3")),
    )

    assert (await book.read(eth_scope)).projection_version == eth_token
    assert book.get_outbox(eth_plan.client_order_id) == eth_entry
    assert book.get_active_reservations(eth_key) == eth_reservations
    assert len(book.get_active_reservations(btc_key)) == 1
    assert book.get_active_reservations(btc_key)[0].active_quantity == Decimal("3")


# --------------------------------------------------------------------------- #
# Durable proof: PostgreSQL-backed responsibility for rows 2 and 5
# --------------------------------------------------------------------------- #
_DURABLE_STREAM = ("ws-acceptance", "epoch-1")
# Must match the strategy name used by the reused durable-book helpers.
_RESERVATION_STRATEGY = "terminal-convergence-test"


def _flat_snapshot(
    *, account: str, symbol: str, at: datetime
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label=account,
        symbol=symbol,
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=2,
        margin_type="cross",
        observed_at=at,
        raw_payload={"include_flat": True},
    )


async def test_partial_fill_then_cancel_settlement_is_durable(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"acc-settle-{uuid4().hex[:10]}"
    command_id = f"entry-{uuid4().hex[:16]}"
    reservation_id = f"res-{command_id}"
    batch_id = f"batch-{command_id}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    key = scope.to_position_key()
    try:
        (
            book,
            _commands,
            reservations,
            _uow,
            _events,
            _reads,
        ) = await _setup_book_and_repos(factory, account)
        book.register_active_stream(
            environment="live",
            account_label=account,
            stream_id=_DURABLE_STREAM[0],
            stream_epoch=_DURABLE_STREAM[1],
        )
        await book.observe(
            ExecutionEvidence(
                evidence_id=f"flat-{command_id}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW,
                snapshot=_flat_snapshot(account=account, symbol="BTCUSDT", at=NOW),
            )
        )
        reservation = PositionReservation(
            reservation_id=reservation_id,
            command_id=command_id,
            position_key=key,
            batch_id=batch_id,
            reserved_quantity=Decimal("10"),
            created_at=NOW,
        )
        async with factory() as session, session.begin():
            session.add(
                PositionReservationRow(
                    reservation_id=reservation_id,
                    environment="live",
                    account_label=account,
                    strategy_name=_RESERVATION_STRATEGY,
                    symbol="BTCUSDT",
                    position_side="LONG",
                    batch_id=batch_id,
                    command_id=command_id,
                    client_order_id=command_id,
                    reserved_quantity=Decimal("10"),
                    consumed_quantity=Decimal("0"),
                    released_quantity=Decimal("0"),
                    status="ACTIVE",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
        book.coordinator.register_reservation(reservation)
        book.register_prepared_command(
            _entry_command(command_id, key, Decimal("10")), scope, [reservation_id]
        )

        filled = await book.observe(
            ExecutionEvidence(
                evidence_id=f"fill-{command_id}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW + timedelta(seconds=10),
                fill=_fill(
                    trade_id=f"trade-{command_id}",
                    order_id=command_id,
                    quantity=Decimal("3"),
                    account_label=account,
                    position_side="LONG",
                ),
                cumulative_order=ExecutionCumulativeOrderReport(
                    order_id=command_id,
                    cumulative_quantity=Decimal("3"),
                    cumulative_quote=Decimal("300"),
                    observed_at=NOW + timedelta(seconds=10),
                ),
            )
        )
        assert isinstance(filled, Applied)
        assert filled.consumed_quantity == Decimal("3")

        canceled = await book.observe(
            ExecutionEvidence(
                evidence_id=f"cancel-{command_id}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW + timedelta(seconds=20),
                order_event=_order_event(
                    command_id=command_id,
                    state=ExchangeOrderState.CANCELED,
                    occurred_at=NOW + timedelta(seconds=20),
                    executed_quantity="3",
                    quote_quantity="300",
                    exchange_order_id=f"ex-{command_id}",
                ),
            )
        )
        assert isinstance(canceled, Applied)
        assert canceled.released_quantity == Decimal("7")

        # A restart must recover exactly the settled responsibility.
        restarted = (await _setup_book_and_repos(factory, account))[0]
        durable = await reservations.load_reservation(reservation_id)
        assert durable is not None
        assert durable.consumed_quantity == Decimal("3")
        assert durable.released_quantity == Decimal("7")
        assert durable.active_quantity == Decimal("0")
        assert (await restarted.read(scope)).total_quantity == Decimal("3")
    finally:
        await engine.dispose()


class FailingCommitUnitOfWork(AsyncPostgresExecutionUnitOfWork):
    """Real durable unit of work with an explicit commit-failure switch."""

    commit_failure: OSError | None = None

    @asynccontextmanager
    async def transaction(self, key, account_scope=None):
        async with super().transaction(key, account_scope=account_scope) as tx:
            yield tx
            if type(self).commit_failure is not None:
                raise type(self).commit_failure


async def test_failed_commit_publishes_nothing(async_database_url: str) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"acc-commit-{uuid4().hex[:10]}"
    command_id = f"exit-{uuid4().hex[:16]}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    key = scope.to_position_key()
    FailingCommitUnitOfWork.commit_failure = None
    try:
        book = _authority_book(factory, uow_type=FailingCommitUnitOfWork)
        await book.restore(account_label=account)
        book.register_active_stream(
            environment="live",
            account_label=account,
            stream_id=_DURABLE_STREAM[0],
            stream_epoch=_DURABLE_STREAM[1],
        )
        observed = await book.observe(
            ExecutionEvidence(
                evidence_id=f"open-{command_id}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW,
                fill=_fill(
                    trade_id=f"open-{command_id}",
                    order_id=f"open-{command_id}",
                    quantity=Decimal("10"),
                    account_label=account,
                    position_side="LONG",
                ),
            )
        )
        assert isinstance(observed, Applied)
        view = await book.read(scope)
        batch = view.batches[0]
        token_before = view.projection_version
        async with factory() as session:
            head_before = await session.get(
                ExecutionBookHeadRow, ("live", account, "BTCUSDT", "LONG")
            )
        assert head_before is not None

        FailingCommitUnitOfWork.commit_failure = OSError(
            "execution storage commit failed"
        )
        allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("10"),
            entry_price=batch.entry_price,
        )
        plan = _exit_plan(
            command_id=command_id,
            symbol="BTCUSDT",
            quantity=Decimal("10"),
            allocation=allocation,
            view_token=token_before,
        )
        result = await book.act(
            _exit_request(
                plan=plan,
                scope=scope,
                allocation=allocation,
                view_token=token_before,
            )
        )

        assert isinstance(result, Blocked)
        assert result.reason == "Execution command was not durably accepted"
        assert book.get_outbox(plan.client_order_id) is None
        assert book.list_outbox() == ()
        assert book.get_active_reservations(key) == ()
        assert (await book.read(scope)).projection_version == token_before

        async with factory() as session:
            command_row = await session.get(ExecutionCommandRow, plan.client_order_id)
            reservation_rows = (
                await session.scalars(
                    select(PositionReservationRow).where(
                        PositionReservationRow.command_id == plan.client_order_id
                    )
                )
            ).all()
            head_after = await session.get(
                ExecutionBookHeadRow, ("live", account, "BTCUSDT", "LONG")
            )
        assert command_row is None
        assert reservation_rows == []
        assert head_after is not None
        assert head_after.revision == head_before.revision
        assert head_after.projection_version == head_before.projection_version
    finally:
        FailingCommitUnitOfWork.commit_failure = None
        await engine.dispose()


async def test_successful_command_versions_are_monotonic(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"acc-version-{uuid4().hex[:10]}"
    command_id = f"exit-{uuid4().hex[:16]}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    key = scope.to_position_key()
    try:
        (
            book,
            _commands,
            _reservations,
            uow,
            _events,
            _reads,
        ) = await _setup_book_and_repos(factory, account)
        book.register_active_stream(
            environment="live",
            account_label=account,
            stream_id=_DURABLE_STREAM[0],
            stream_epoch=_DURABLE_STREAM[1],
        )
        await book.observe(
            ExecutionEvidence(
                evidence_id=f"open-{command_id}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW,
                fill=_fill(
                    trade_id=f"open-{command_id}",
                    order_id=f"open-{command_id}",
                    quantity=Decimal("10"),
                    account_label=account,
                    position_side="LONG",
                ),
            )
        )
        view = await book.read(scope)
        batch = view.batches[0]
        allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("5"),
            entry_price=batch.entry_price,
        )
        plan = _exit_plan(
            command_id=command_id,
            symbol="BTCUSDT",
            quantity=Decimal("5"),
            allocation=allocation,
            view_token=view.projection_version,
        )

        revisions = [
            (await uow.load_position(key, as_of=datetime.now(UTC))).head.revision
        ]
        accepted = await book.act(
            _exit_request(
                plan=plan,
                scope=scope,
                allocation=allocation,
                view_token=view.projection_version,
            )
        )
        assert isinstance(accepted, Accepted)
        assert book.get_outbox(plan.client_order_id).state is DispatchState.PREPARED
        revisions.append(
            (await uow.load_position(key, as_of=datetime.now(UTC))).head.revision
        )

        acknowledged = await book.mark_acknowledged(
            plan.client_order_id, external_order_id="ex-1"
        )
        assert acknowledged.state is DispatchState.ACKNOWLEDGED
        revisions.append(
            (await uow.load_position(key, as_of=datetime.now(UTC))).head.revision
        )

        terminal = await book.mark_terminal(plan.client_order_id, reason="exit_settled")
        assert terminal.state is DispatchState.TERMINAL
        revisions.append(
            (await uow.load_position(key, as_of=datetime.now(UTC))).head.revision
        )

        assert revisions == sorted(set(revisions))
        assert len(revisions) == 4
        assert terminal.updated_at >= acknowledged.updated_at
        assert book.get_active_reservations(key) == ()
    finally:
        await engine.dispose()


async def test_concurrent_cross_key_candidates_do_not_pollute_each_other(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"acc-crosskey-{uuid4().hex[:10]}"
    btc_scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    eth_scope = ExecutionScope("live", account, "ETHUSDT", FuturesPositionSide.LONG)
    btc_key = btc_scope.to_position_key()
    eth_key = eth_scope.to_position_key()
    try:
        (
            book,
            _commands,
            reservations,
            uow,
            _events,
            _reads,
        ) = await _setup_book_and_repos(factory, account)
        book.register_active_stream(
            environment="live",
            account_label=account,
            stream_id=_DURABLE_STREAM[0],
            stream_epoch=_DURABLE_STREAM[1],
        )
        for symbol, quantity in (("BTCUSDT", Decimal("10")), ("ETHUSDT", Decimal("4"))):
            observed = await book.observe(
                ExecutionEvidence(
                    evidence_id=f"open-{symbol}-{account}",
                    scope=ExecutionScope(
                        "live", account, symbol, FuturesPositionSide.LONG
                    ),
                    stream_id=_DURABLE_STREAM[0],
                    stream_epoch=_DURABLE_STREAM[1],
                    observed_at=NOW,
                    fill=_fill(
                        trade_id=f"open-{symbol}-{account}",
                        order_id=f"open-{symbol}-{account}",
                        quantity=quantity,
                        symbol=symbol,
                        account_label=account,
                        position_side="LONG",
                    ),
                )
            )
            assert isinstance(observed, Applied)

        btc_view = await book.read(btc_scope)
        eth_view = await book.read(eth_scope)
        btc_allocation = ExitAllocation(
            batch_id=btc_view.batches[0].batch_id,
            allocated_quantity=Decimal("3"),
            entry_price=btc_view.batches[0].entry_price,
        )
        eth_allocation = ExitAllocation(
            batch_id=eth_view.batches[0].batch_id,
            allocated_quantity=Decimal("4"),
            entry_price=eth_view.batches[0].entry_price,
        )
        btc_plan = _exit_plan(
            command_id="cross-btc",
            symbol="BTCUSDT",
            quantity=Decimal("3"),
            allocation=btc_allocation,
            view_token=btc_view.projection_version,
        )
        eth_plan = _exit_plan(
            command_id="cross-eth",
            symbol="ETHUSDT",
            quantity=Decimal("4"),
            allocation=eth_allocation,
            view_token=eth_view.projection_version,
        )

        results = await asyncio.gather(
            book.act(
                _exit_request(
                    plan=btc_plan,
                    scope=btc_scope,
                    allocation=btc_allocation,
                    view_token=btc_view.projection_version,
                )
            ),
            book.act(
                _exit_request(
                    plan=eth_plan,
                    scope=eth_scope,
                    allocation=eth_allocation,
                    view_token=eth_view.projection_version,
                )
            ),
        )
        assert all(isinstance(result, Accepted) for result in results)

        # Neither candidate erased the other key's accepted command.
        assert book.get_outbox(btc_plan.client_order_id).state is DispatchState.PREPARED
        assert book.get_outbox(eth_plan.client_order_id).state is DispatchState.PREPARED
        btc_active = book.get_active_reservations(btc_key)
        eth_active = book.get_active_reservations(eth_key)
        assert sum(
            (item.active_quantity for item in btc_active), Decimal("0")
        ) == Decimal("3")
        assert sum(
            (item.active_quantity for item in eth_active), Decimal("0")
        ) == Decimal("4")

        durable_btc = await reservations.load_active_reservations(btc_key)
        durable_eth = await reservations.load_active_reservations(eth_key)
        assert sum(
            (item.active_quantity for item in durable_btc), Decimal("0")
        ) == Decimal("3")
        assert sum(
            (item.active_quantity for item in durable_eth), Decimal("0")
        ) == Decimal("4")
        assert (await uow.load_position(btc_key, as_of=datetime.now(UTC))) is not None
        assert (await uow.load_position(eth_key, as_of=datetime.now(UTC))) is not None
    finally:
        await engine.dispose()


async def test_entry_network_post_does_not_hold_the_application_lock() -> None:
    book = ExecutionBook()
    locks = PositionLifecycleLocks()
    lock_key = live_symbol_position_key(ACCOUNT, "BTCUSDT")
    locked_during_post: list[bool] = []

    class LockProbeExchange:
        def __init__(self) -> None:
            self.posts = 0

        def set_exchange_boundary_callbacks(self, *, on_request=None, on_response=None):
            return None

        async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
            self.posts += 1
            locked_during_post.append(locks.is_locked(lock_key))
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="ex-entry",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=MARKET_NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

        async def query_order_by_client_id(self, symbol: str, client_order_id: str):
            return None

    exchange = LockProbeExchange()
    state_machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=FakeOrderRepository(),
        live_submit_enabled=True,
        clock=lambda: MARKET_NOW,
    )
    coordinator = MemoryOrderExecutionCoordinator(
        environment="live",
        backend=state_machine,
        account_label=ACCOUNT,
        execution_book=book,
    )
    coordinator.configure_submission(RecordingPreparedRepo(), clock=lambda: MARKET_NOW)
    submission = LiveCandidateSubmission(
        risk_gateway=RiskGateway(
            limits=FixedLiveLimits(
                notional_cap=Decimal("100000"),
                max_open_positions=10,
                max_daily_loss=Decimal("10000"),
                max_gross_exposure=Decimal("200000"),
            )
        ),
        state_machine=coordinator,
        config=LiveSubmissionConfig(
            run_id="run-1",
            account_label=ACCOUNT,
            resize_tolerance=Decimal("0.05"),
            hedge_mode=False,
            entry_order_type=EntryType.MARKET,
            entry_limit_ttl_seconds=605,
        ),
        clock=lambda: MARKET_NOW,
        pending_entry_reservation=lambda orders: (Decimal("0"), frozenset()),
        pending_entry_plans=lambda: (),
        pending_entry_uncertainty=lambda symbol: False,
        remember_pending_entry=lambda plan, result: None,
        record_signal_candidate=lambda **kwargs: None,
        position_locks=locks,
    )
    base = _runtime_context()
    context = replace(
        base,
        risk_config=replace(
            base.risk_config,
            max_open_positions=10,
            max_order_notional=Decimal("10000"),
            max_gross_notional=Decimal("100000"),
        ),
    )
    try:
        result = await submission.execute(
            replace(
                _intent(),
                symbol="BTCUSDT",
                features={"position_side": "BOTH"},
            ),
            requested_quantity=Decimal("0.001"),
            state=_state(),
            context=context,
            reference_price=Decimal("30000"),
        )

        assert result is not None
        assert result.state is ExchangeOrderState.ACKNOWLEDGED
        assert exchange.posts == 1
        assert locked_during_post == [False]
    finally:
        await coordinator.aclose()


async def test_durable_proven_remaining_exit_is_accepted_on_the_same_batch(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"acc-proven-{uuid4().hex[:10]}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    key = scope.to_position_key()
    try:
        (
            book,
            _commands,
            reservations,
            _uow,
            _events,
            _reads,
        ) = await _setup_book_and_repos(factory, account)
        book.register_active_stream(
            environment="live",
            account_label=account,
            stream_id=_DURABLE_STREAM[0],
            stream_epoch=_DURABLE_STREAM[1],
        )
        await book.observe(
            ExecutionEvidence(
                evidence_id=f"open-{account}",
                scope=scope,
                stream_id=_DURABLE_STREAM[0],
                stream_epoch=_DURABLE_STREAM[1],
                observed_at=NOW,
                fill=_fill(
                    trade_id=f"open-{account}",
                    order_id=f"open-{account}",
                    quantity=Decimal("10"),
                    account_label=account,
                    position_side="LONG",
                ),
            )
        )
        view = await book.read(scope)
        batch = view.batches[0]

        held_allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("3"),
            entry_price=batch.entry_price,
        )
        held_plan = _exit_plan(
            command_id="proven-held",
            symbol="BTCUSDT",
            quantity=Decimal("3"),
            allocation=held_allocation,
            view_token=view.projection_version,
        )
        held = await book.act(
            _exit_request(
                plan=held_plan,
                scope=scope,
                allocation=held_allocation,
                view_token=view.projection_version,
            )
        )
        assert isinstance(held, Accepted)

        remaining_view = await book.read(scope)
        remaining_allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("7"),
            entry_price=batch.entry_price,
        )
        remaining_plan = _exit_plan(
            command_id="proven-remaining",
            symbol="BTCUSDT",
            quantity=Decimal("7"),
            allocation=remaining_allocation,
            view_token=remaining_view.projection_version,
        )
        remaining = await book.act(
            _exit_request(
                plan=remaining_plan,
                scope=scope,
                allocation=remaining_allocation,
                view_token=remaining_view.projection_version,
            )
        )

        assert isinstance(remaining, Accepted)
        durable = await reservations.load_active_reservations(key)
        assert sum((item.active_quantity for item in durable), Decimal("0")) == Decimal(
            "10"
        )
        assert {item.command_id for item in durable} == {
            held_plan.client_order_id,
            remaining_plan.client_order_id,
        }
    finally:
        await engine.dispose()
