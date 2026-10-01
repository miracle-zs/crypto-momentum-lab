import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from crypto_momentum_lab.domain.execution.legacy_command_repository import (
    LegacyCommandRepositoryAdapter,
)
from crypto_momentum_lab.domain.execution.legacy_reservation_repository import (
    assemble_legacy_execution_book,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator as _RealOrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionKey,
    _KeyCommandScheduler,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)

NOW = datetime(2026, 8, 22, tzinfo=UTC)


class OrderExecutionCoordinator(_RealOrderExecutionCoordinator):
    def __init__(self, *args: Any, environment: str = "live", **kwargs: Any) -> None:
        super().__init__(*args, environment=environment, **kwargs)

    async def _ensure_reservation(self, plan: OrderExecutionPlan) -> None:
        if plan.projection_version is None and self._execution_book is not None:
            from crypto_momentum_lab.domain.execution.command_models import (
                ExecutionScope,
            )

            scope = ExecutionScope(
                environment=self._environment,
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            view = await self._execution_book.read(scope)
            object.__setattr__(plan, "projection_version", view.projection_version)
        if plan.strategy_name is None:
            object.__setattr__(plan, "strategy_name", "orderflow_impulse")
        if plan.strategy_version is None:
            object.__setattr__(plan, "strategy_version", "v1")
        await super()._ensure_reservation(plan)


def _register_execution_command_with_reservations(
    coordinator: OrderExecutionCoordinator,
    plan: OrderExecutionPlan,
    reservations: tuple[Any, ...] | list[Any],
) -> None:
    """Build the same explicit command-to-allocation link used by live flow."""
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol=plan.symbol,
        position_side=FuturesPositionSide(plan.position_side),
    )
    is_long = (plan.side.upper() == "BUY") != plan.reduce_only
    command = TradeCommand(
        command_id=plan.client_order_id,
        position_key=scope.to_position_key(),
        command_type=(
            TradeCommandType.EXIT if plan.reduce_only else TradeCommandType.ENTRY
        ),
        side=StrategySide.LONG if is_long else StrategySide.SHORT,
        order_type=EntryType(plan.order_type.lower()),
        requested_quantity=plan.quantity,
        limit_price=plan.price,
        reduce_only=plan.reduce_only,
        created_at=plan.created_at,
    )
    for reservation in reservations:
        coordinator.execution_book.coordinator.register_reservation(reservation)
    coordinator.execution_book.register_prepared_command(
        command,
        scope,
        [reservation.reservation_id for reservation in reservations],
    )


class BlockingBackend:
    def __init__(self) -> None:
        self.query_started = asyncio.Event()
        self.release_query = asyncio.Event()
        self.submit_started = asyncio.Event()
        self.calls: list[str] = []

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission=None,
    ):
        lane = "exit" if plan.reduce_only else "entry"
        self.calls.append(f"submit:{plan.symbol}:{lane}")
        self.submit_started.set()
        return _result(plan)

    async def reconcile_order(self, plan: OrderExecutionPlan):
        self.calls.append(f"reconcile:{plan.symbol}")
        self.query_started.set()
        await self.release_query.wait()
        return _result(plan)

    async def cancel_order(self, plan: OrderExecutionPlan):
        self.calls.append(f"cancel:{plan.symbol}")
        return _result(plan, ExchangeOrderState.CANCELED)


class BlockingSubmitBackend(BlockingBackend):
    def __init__(self) -> None:
        super().__init__()
        self.release_submit = asyncio.Event()

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission=None,
    ):
        result = await super().submit(
            plan,
            prepared_submission=prepared_submission,
        )
        await self.release_submit.wait()
        return result


async def test_slow_reconcile_does_not_block_other_symbol_submit() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )

    reconcile_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("ETHUSDT", reduce_only=False))
    )
    await backend.query_started.wait()
    submit_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=True))
    )

    await asyncio.wait_for(backend.submit_started.wait(), timeout=0.03)
    backend.release_query.set()
    await asyncio.gather(reconcile_task, submit_task)

    assert backend.calls[:2] == [
        "reconcile:ETHUSDT",
        "submit:BTCUSDT:exit",
    ]
    await coordinator.aclose()


async def test_same_position_is_serial_and_exit_has_priority_over_entry() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )

    reconcile_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.query_started.wait()
    entry_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    exit_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=True))
    )
    await asyncio.sleep(0)
    assert backend.calls == ["reconcile:BTCUSDT"]

    backend.release_query.set()
    await asyncio.gather(reconcile_task, entry_task, exit_task)

    assert backend.calls == [
        "reconcile:BTCUSDT",
        "submit:BTCUSDT:exit",
        "submit:BTCUSDT:entry",
    ]
    assert exit_task.result().state is ExchangeOrderState.ACKNOWLEDGED
    await coordinator.aclose()


async def test_scheduler_close_releases_queued_submitters() -> None:
    scheduler = _KeyCommandScheduler(
        OrderExecutionKey("primary", "BTCUSDT", FuturesPositionSide.BOTH)
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls: list[str] = []

    async def first_operation():
        calls.append("first")
        first_started.set()
        await release_first.wait()

    async def queued_operation():
        calls.append("queued")

    first_task = asyncio.create_task(
        scheduler.submit(priority=0, operation=first_operation)
    )
    await first_started.wait()
    queued_task = asyncio.create_task(
        scheduler.submit(priority=10, operation=queued_operation)
    )
    await asyncio.sleep(0)
    close_task = asyncio.create_task(scheduler.close())
    await asyncio.sleep(0)
    release_first.set()

    await first_task
    await close_task
    with pytest.raises(RuntimeError, match="scheduler is closed"):
        await queued_task
    assert calls == ["first"]


async def test_coordinator_close_waits_for_inflight_submit_after_caller_cancel() -> (
    None
):
    from unittest.mock import AsyncMock

    backend = BlockingSubmitBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )
    drain = AsyncMock()
    coordinator.execution_book.drain = drain
    submit_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.submit_started.wait()

    submit_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await submit_task

    close_task = asyncio.create_task(coordinator.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False
    drain.assert_not_awaited()
    backend.release_submit.set()
    await close_task
    assert backend.calls == ["submit:BTCUSDT:entry"]
    drain.assert_awaited_once_with()
    await coordinator.aclose()
    drain.assert_awaited_once_with()


async def test_entry_gate_drains_inflight_submit_and_rejects_new_entries() -> None:
    backend = BlockingSubmitBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )
    submit_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.submit_started.wait()

    coordinator.block_entry_submissions()
    drain_task = asyncio.create_task(coordinator.wait_for_entry_submissions_idle())
    await asyncio.sleep(0)
    assert drain_task.done() is False

    backend.release_submit.set()
    await drain_task
    await submit_task

    with pytest.raises(
        OrderPreSubmissionError,
        match="entry submissions are blocked",
    ):
        await coordinator.submit(_plan("ETHUSDT", reduce_only=False))

    await coordinator.aclose()


async def test_prepare_and_execute_serializes_reconcile_after_prepare() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )
    plan = _plan("BTCUSDT", reduce_only=False)
    prepare_started = asyncio.Event()
    release_prepare = asyncio.Event()

    async def prepare_submission():
        prepare_started.set()
        await release_prepare.wait()
        return _prepared(plan)

    submit_task = asyncio.create_task(
        coordinator.prepare_and_execute(
            plan,
            prepare_submission=prepare_submission,
        )
    )
    await prepare_started.wait()
    reconcile_task = asyncio.create_task(coordinator.reconcile_order(plan))
    await asyncio.sleep(0)
    assert backend.calls == []

    release_prepare.set()
    await backend.submit_started.wait()
    backend.release_query.set()
    await asyncio.gather(submit_task, reconcile_task)
    assert backend.calls == ["submit:BTCUSDT:entry", "reconcile:BTCUSDT"]
    await coordinator.aclose()


async def test_coordinator_rejects_commands_after_close() -> None:
    coordinator = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
    )
    await coordinator.aclose()

    with pytest.raises(RuntimeError, match="coordinator is closed"):
        await coordinator.submit(_plan("BTCUSDT", reduce_only=False))


def _plan(symbol: str, *, reduce_only: bool) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{symbol}-{reduce_only}",
        run_id="run-1",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        client_order_id=f"cml_{symbol}_{str(reduce_only).lower():0<20}",
        symbol=symbol,
        side="SELL" if reduce_only else "BUY",
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        reduce_only=reduce_only,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )


def _result(
    plan: OrderExecutionPlan,
    state: ExchangeOrderState = ExchangeOrderState.ACKNOWLEDGED,
):
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionResult,
    )

    return OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=state,
        exchange_order_id=f"exchange-{plan.symbol}",
    )


def _prepared(plan: OrderExecutionPlan):
    from crypto_momentum_lab.domain.execution.order_submission import (
        PreparedOrderSubmission,
    )

    event = ExchangeOrderEvent(
        event_id=f"event-{plan.client_order_id}",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.SUBMITTING,
        occurred_at=NOW,
        exchange_order_id=None,
        details={},
    )
    return PreparedOrderSubmission(plan=plan, submitting_event=event)


async def test_coordinator_queue_capacity_and_exit_headroom() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        max_queue_depth=4,
        exit_headroom=2,  # Entries can only fill up to 4 - 2 = 2 slots
    )

    # First command is picked up and blocks backend
    block_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.query_started.wait()

    # Now queue 2 entry commands (fills queue to entry_limit = 2)
    e1_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    e2_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    await asyncio.sleep(0.01)

    # 3rd entry command must be rejected immediately due to headroom preservation
    with pytest.raises(OrderPreSubmissionError, match="entry capacity exceeded"):
        await coordinator.submit(_plan("BTCUSDT", reduce_only=False))

    # An EXIT command (reduce_only=True) CAN still enter
    # because headroom is reserved for exits!
    exit_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=True))
    )
    await asyncio.sleep(0.01)

    # Unblock backend and let all tasks drain
    backend.release_query.set()
    await asyncio.gather(block_task, e1_task, e2_task, exit_task)
    await coordinator.aclose()


async def test_coordinator_queue_max_wait_timeout() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        max_queue_wait_seconds=0.05,  # Short timeout for testing
    )

    # Block the worker
    block_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.query_started.wait()

    # Queue an entry command
    entry_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )

    # Sleep longer than max_queue_wait_seconds
    await asyncio.sleep(0.08)

    # Unblock the worker; the entry command waited > 0.05s so it fails with timeout
    backend.release_query.set()
    await block_task

    with pytest.raises(
        OrderPreSubmissionError, match="waited .* in queue exceeding limit"
    ):
        await entry_task

    await coordinator.aclose()


async def test_coordinator_idle_worker_reclamation() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        idle_timeout_seconds=0.05,  # Short idle timeout for test
    )

    key = OrderExecutionKey("primary", "BTCUSDT", FuturesPositionSide.BOTH)

    # Submit one command
    res1 = await coordinator.submit(_plan("BTCUSDT", reduce_only=True))
    assert res1.state is ExchangeOrderState.ACKNOWLEDGED
    assert key in coordinator._schedulers

    # Wait for idle timeout
    await asyncio.sleep(0.1)

    # Scheduler should have been removed from coordinator after becoming idle
    assert key not in coordinator._schedulers or coordinator._schedulers[key].is_closed

    # Submitting another command should seamlessly spawn a fresh scheduler
    res2 = await coordinator.submit(_plan("BTCUSDT", reduce_only=True))
    assert res2.state is ExchangeOrderState.ACKNOWLEDGED
    assert key in coordinator._schedulers
    assert not coordinator._schedulers[key].is_closed

    await coordinator.aclose()


async def test_coordinator_caller_timeout_when_worker_is_hung() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        max_queue_wait_seconds=0.05,
    )

    # Worker starts and hangs on query without releasing
    block_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.query_started.wait()

    # Entry command submitted while worker is hung.
    # The caller must time out within max_queue_wait_seconds
    with pytest.raises(
        OrderPreSubmissionError, match="waited .* in queue exceeding limit"
    ):
        await coordinator.submit(_plan("BTCUSDT", reduce_only=False))

    backend.release_query.set()
    await block_task
    await coordinator.aclose()


async def test_cancel_order_succeeds_even_when_entry_queue_is_congested() -> None:
    backend = BlockingBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        max_queue_depth=4,
        exit_headroom=2,  # entry limit = 2
    )

    # Block worker with a reconcile
    block_task = asyncio.create_task(
        coordinator.reconcile_order(_plan("BTCUSDT", reduce_only=False))
    )
    await backend.query_started.wait()

    # Fill entry capacity (limit = 2)
    e1_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    e2_task = asyncio.create_task(
        coordinator.submit(_plan("BTCUSDT", reduce_only=False))
    )
    await asyncio.sleep(0.01)

    # Attempting another entry fails due to entry capacity
    with pytest.raises(OrderPreSubmissionError, match="entry capacity exceeded"):
        await coordinator.submit(_plan("BTCUSDT", reduce_only=False))

    # But cancel_order for entry order MUST still succeed using exit priority!
    non_reduce_only_plan = _plan("BTCUSDT", reduce_only=False)
    cancel_task = asyncio.create_task(coordinator.cancel_order(non_reduce_only_plan))
    await asyncio.sleep(0.01)

    backend.release_query.set()
    await asyncio.gather(block_task, e1_task, e2_task)
    cancel_res = await cancel_task
    assert cancel_res.state is ExchangeOrderState.CANCELED

    await coordinator.aclose()


async def test_cancel_order_releases_reservation_after_backend_success() -> None:
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    backend = BlockingBackend()
    reservation_repo = InMemoryPositionReservationRepository()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=reservation_repo,
    )
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    res = PositionReservation(
        reservation_id="res_test_123",
        command_id="order-exit-1",
        position_key=key,
        batch_id="batch_1",
        reserved_quantity=Decimal("1.5"),
    )
    reservation_repo.save_reservation(res)

    plan = OrderExecutionPlan(
        intent_id="intent-test-exit",
        run_id="run-1",
        client_order_id="order-exit-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.5"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )
    _register_execution_command_with_reservations(coordinator, plan, [res])

    cancel_res = await coordinator.cancel_order(plan)
    assert cancel_res.state is ExchangeOrderState.CANCELED

    # Reservation should be released
    active = reservation_repo.load_active_reservations(key)
    assert len(active) == 0
    saved = reservation_repo._reservations["res_test_123"]
    assert saved.released_quantity == Decimal("1.5")
    assert saved.active_quantity == Decimal("0")
    await coordinator.aclose()


async def test_cancel_order_does_not_release_reservation_if_state_not_canceled() -> (
    None
):
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class FilledCancelBackend(BlockingBackend):
        async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id="ex-1",
            )

    backend = FilledCancelBackend()
    reservation_repo = InMemoryPositionReservationRepository()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=reservation_repo,
    )
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    res = PositionReservation(
        reservation_id="res_test_filled",
        command_id="order-exit-filled",
        position_key=key,
        batch_id="batch_1",
        reserved_quantity=Decimal("1.5"),
    )
    reservation_repo.save_reservation(res)

    plan = OrderExecutionPlan(
        intent_id="intent-test-exit",
        run_id="run-1",
        client_order_id="order-exit-filled",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.5"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )

    cancel_res = await coordinator.cancel_order(plan)
    assert cancel_res.state is ExchangeOrderState.FILLED

    # Reservation MUST NOT be released since state was FILLED, not CANCELED!
    active = reservation_repo.load_active_reservations(key)
    assert len(active) == 1
    assert active[0].active_quantity == Decimal("1.5")

    await coordinator.aclose()


async def test_reservation_creation_failure_fails_closed() -> None:
    from decimal import Decimal

    backend = BlockingBackend()

    class BrokenReservationRepo:
        def load_active_reservations(self, key: Any) -> list[Any]:
            return []

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            raise RuntimeError("Database connection failure")

    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=BrokenReservationRepo(),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-test-fail-res",
        run_id="run-1",
        client_order_id="order-exit-fail-res",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.5"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )

    with pytest.raises(
        OrderPreSubmissionError, match="Failed to create position reservation"
    ):
        await coordinator.submit(plan)

    with pytest.raises(
        OrderPreSubmissionError, match="Failed to create position reservation"
    ):
        await coordinator.prepare_and_execute(
            plan,
            prepare_submission=lambda: asyncio.sleep(0, result=None),
        )

    await coordinator.aclose()


async def test_multi_batch_reservations_stay_active_on_ambiguous_backend_failure() -> (
    None
):
    backend = BlockingBackend()

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, Any] = {}

        def load_active_reservations(self, key: Any) -> list[Any]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            self.reservations[res.reservation_id] = res

    repo = InMemoryReservationRepo()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
    )

    class FailingBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission=None
        ):
            raise RuntimeError("Exchange API rejected order")

    coordinator._backend = FailingBackend()

    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation

    allocs = (
        ExitAllocation(batch_id="batch_1", allocated_quantity=Decimal("10.0")),
        ExitAllocation(batch_id="batch_2", allocated_quantity=Decimal("20.0")),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-multi-fail",
        run_id="run-1",
        client_order_id="order-multi-fail",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("30.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        allocations=allocs,
    )

    with pytest.raises(RuntimeError, match="Exchange API rejected order"):
        await coordinator.submit(plan)

    # A generic transport exception does not prove the exchange rejected the
    # order; both allocations stay reserved while the outbox is UNKNOWN.
    assert len(repo.reservations) == 2
    for r in repo.reservations.values():
        assert r.active_quantity == r.reserved_quantity
        assert r.released_quantity == Decimal("0")
    assert (
        coordinator.execution_book.get_outbox(plan.client_order_id).state.value
        == "unknown"
    )

    await coordinator.aclose()


async def test_multi_batch_reservation_consume_across_batches() -> None:
    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, Any] = {}

        def load_active_reservations(self, key: Any) -> list[Any]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            self.reservations[res.reservation_id] = res

    class FillBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission=None
        ):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id="exchange-1",
                executed_quantity=plan.quantity,
                average_price=Decimal("100.0"),
            )

    repo = InMemoryReservationRepo()
    coordinator = OrderExecutionCoordinator(
        backend=FillBackend(),
        account_label="primary",
        reservation_repository=repo,
    )

    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation

    allocs = (
        ExitAllocation(batch_id="batch_1", allocated_quantity=Decimal("10.0")),
        ExitAllocation(batch_id="batch_2", allocated_quantity=Decimal("15.0")),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-multi-consume",
        run_id="run-1",
        client_order_id="order-multi-consume",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("25.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        allocations=allocs,
    )

    # Execute fill of 25 (covering both batches: 10 from batch_1, 15 from batch_2)
    res = await coordinator.submit(plan)
    assert res.executed_quantity == Decimal("25.0")

    # Both reservations should be consumed to 0 active
    assert len(repo.reservations) == 2
    r0 = repo.reservations["res_order-multi-consume_0"]
    r1 = repo.reservations["res_order-multi-consume_1"]
    assert r0.consumed_quantity == Decimal("10.0")
    assert r0.active_quantity == Decimal("0")
    assert r1.consumed_quantity == Decimal("15.0")
    assert r1.active_quantity == Decimal("0")

    await coordinator.aclose()


async def test_partial_fill_consumes_batches_in_stable_order() -> None:
    """Partial fills must attribute quantity to batches in load order."""
    from dataclasses import dataclass, field

    @dataclass
    class OrderedRepo:
        reservations: dict[str, Any] = field(default_factory=dict)

        def load_active_reservations(self, key: Any) -> list[Any]:
            active = [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]
            # Simulate SQL ORDER BY created_at, reservation_id
            active.sort(key=lambda r: (r.created_at, r.reservation_id))
            return active

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            self.reservations[res.reservation_id] = res

        def load_reservation(self, reservation_id: str) -> Any:
            return self.reservations.get(reservation_id)

    class PartialFillBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission=None
        ):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.PARTIALLY_FILLED,
                exchange_order_id="exchange-partial",
                executed_quantity=Decimal("12.0"),
                average_price=Decimal("100.0"),
            )

    repo = OrderedRepo()
    coordinator = OrderExecutionCoordinator(
        backend=PartialFillBackend(),
        account_label="primary",
        reservation_repository=repo,
    )
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation

    allocs = (
        ExitAllocation(batch_id="batch_1", allocated_quantity=Decimal("10.0")),
        ExitAllocation(batch_id="batch_2", allocated_quantity=Decimal("20.0")),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-partial",
        run_id="run-1",
        client_order_id="order-partial",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("30.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        allocations=allocs,
    )
    res = await coordinator.submit(plan)
    assert res.executed_quantity == Decimal("12.0")

    r0 = repo.reservations["res_order-partial_0"]
    r1 = repo.reservations["res_order-partial_1"]
    # First batch (created earlier / lower id) absorbs the fill first.
    assert r0.consumed_quantity == Decimal("10.0")
    assert r0.active_quantity == Decimal("0")
    assert r1.consumed_quantity == Decimal("2.0")
    assert r1.active_quantity == Decimal("18.0")
    await coordinator.aclose()


async def test_reservation_save_receives_projection_version() -> None:
    captured: dict[str, Any] = {}

    class CaptureRepo:
        def load_active_reservations(self, key: Any) -> list[Any]:
            return []

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            captured["res"] = res
            captured["kwargs"] = kwargs

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            return None

        def load_reservation(self, reservation_id: str) -> Any:
            return captured.get("res")

    class FillBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission=None
        ):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id="e1",
                executed_quantity=plan.quantity,
                average_price=Decimal("1.0"),
            )

    coordinator = OrderExecutionCoordinator(
        backend=FillBackend(),
        account_label="primary",
        reservation_repository=CaptureRepo(),
    )
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope

    scope = ExecutionScope(
        environment=coordinator._environment,
        account_label=coordinator._account_label,
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = await coordinator.execution_book.read(scope)
    expected_pv = view.projection_version
    plan = OrderExecutionPlan(
        intent_id="intent-pv",
        run_id="run-1",
        client_order_id="order-pv",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        projection_version=expected_pv,
        batch_id="batch_1",
    )
    await coordinator.submit(plan)
    assert captured["kwargs"].get("expected_projection_version") == expected_pv
    await coordinator.aclose()


async def test_reservation_conflict_mismatch_is_rejected() -> None:
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        ReservationConflictError,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class ConflictingRepo:
        def __init__(self) -> None:
            self.existing = PositionReservation(
                reservation_id="res_order-conflict",
                command_id="order-conflict",
                position_key=PositionKey(
                    environment="live",
                    account_label="primary",
                    symbol="BTCUSDT",
                    position_side=FuturesPositionSide.BOTH,
                ),
                batch_id="batch_other",
                reserved_quantity=Decimal("99.0"),
            )

        def load_active_reservations(self, key: Any) -> list[Any]:
            return [self.existing]

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            raise ReservationConflictError("dup")

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            return None

        def load_reservation(self, reservation_id: str) -> Any:
            return self.existing

    coordinator = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        reservation_repository=ConflictingRepo(),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-conflict",
        run_id="run-1",
        client_order_id="order-conflict",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch_plan",
    )
    with pytest.raises(OrderPreSubmissionError, match="does not match|already exists"):
        await coordinator.submit(plan)
    await coordinator.aclose()


async def test_unallocated_exit_order_fails_closed_without_inventing_batch() -> None:
    class DummyRepo:
        def load_active_reservations(self, key: Any) -> list[Any]:
            return []

    coordinator = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        reservation_repository=DummyRepo(),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-unallocated",
        run_id="run-1",
        client_order_id="order-unallocated",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )
    with pytest.raises(
        OrderPreSubmissionError, match="has no allocated batches or batch_id"
    ):
        await coordinator.submit(plan)
    await coordinator.aclose()


async def test_reconcile_order_consumes_filled_reservation() -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, PositionReservation] = {}

        def load_active_reservations(self, key: Any) -> list[PositionReservation]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: PositionReservation, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(
            self, res: PositionReservation, release_reason: str = ""
        ) -> None:
            self.reservations[res.reservation_id] = res

    class ReconcileFillBackend(BlockingBackend):
        async def reconcile_order(self, plan: OrderExecutionPlan):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id="e-rec-1",
                executed_quantity=plan.quantity,
                average_price=Decimal("100.0"),
            )

    repo = InMemoryReservationRepo()
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    res_id = "res_order-reconcile-fill_0"
    repo.reservations[res_id] = PositionReservation(
        reservation_id=res_id,
        command_id="order-reconcile-fill",
        position_key=key,
        batch_id="batch_1",
        reserved_quantity=Decimal("5.0"),
    )

    coordinator = OrderExecutionCoordinator(
        backend=ReconcileFillBackend(),
        account_label="primary",
        reservation_repository=repo,
    )
    plan = OrderExecutionPlan(
        intent_id="intent-rec",
        run_id="run-1",
        client_order_id="order-reconcile-fill",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("5.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch_1",
    )
    _register_execution_command_with_reservations(
        coordinator, plan, [repo.reservations[res_id]]
    )

    result = await coordinator.reconcile_order(plan)
    assert result.state == ExchangeOrderState.FILLED
    r = repo.reservations[res_id]
    assert r.consumed_quantity == Decimal("5.0")
    assert r.active_quantity == Decimal("0")
    await coordinator.aclose()


async def test_apply_observed_snapshot_consumes_filled_reservation() -> None:
    from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderSnapshot
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, PositionReservation] = {}

        def load_active_reservations(self, key: Any) -> list[PositionReservation]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: PositionReservation, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(
            self, res: PositionReservation, release_reason: str = ""
        ) -> None:
            self.reservations[res.reservation_id] = res

    class SnapshotBackend(BlockingBackend):
        async def apply_observed_snapshot(
            self, plan: OrderExecutionPlan, snapshot: ExchangeOrderSnapshot
        ):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=snapshot.state,
                exchange_order_id=snapshot.exchange_order_id,
                executed_quantity=snapshot.executed_quantity,
                average_price=snapshot.average_price,
            )

    repo = InMemoryReservationRepo()
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    res_id = "res_order-snap-fill_0"
    repo.reservations[res_id] = PositionReservation(
        reservation_id=res_id,
        command_id="order-snap-fill",
        position_key=key,
        batch_id="batch_1",
        reserved_quantity=Decimal("3.0"),
    )

    coordinator = OrderExecutionCoordinator(
        backend=SnapshotBackend(),
        account_label="primary",
        reservation_repository=repo,
    )
    plan = OrderExecutionPlan(
        intent_id="intent-snap",
        run_id="run-1",
        client_order_id="order-snap-fill",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("3.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch_1",
    )
    snapshot = ExchangeOrderSnapshot(
        client_order_id="order-snap-fill",
        exchange_order_id="e-snap-1",
        state=ExchangeOrderState.FILLED,
        executed_quantity=Decimal("3.0"),
        average_price=Decimal("100.0"),
        observed_at=NOW,
    )
    _register_execution_command_with_reservations(
        coordinator, plan, [repo.reservations[res_id]]
    )

    result = await coordinator.apply_observed_snapshot(plan, snapshot)
    assert result.state == ExchangeOrderState.FILLED
    r = repo.reservations[res_id]
    assert r.consumed_quantity == Decimal("3.0")
    assert r.active_quantity == Decimal("0")
    await coordinator.aclose()


async def test_cancel_order_releases_all_allocations_for_command() -> None:
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, PositionReservation] = {}
            self.release_reasons: dict[str, str] = {}

        def load_active_reservations(self, key: Any) -> list[PositionReservation]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: PositionReservation, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(
            self, res: PositionReservation, release_reason: str = ""
        ) -> None:
            self.reservations[res.reservation_id] = res
            if release_reason:
                self.release_reasons[res.reservation_id] = release_reason

    repo = InMemoryReservationRepo()
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    r0 = PositionReservation(
        reservation_id="res_multi_cancel_0",
        command_id="order-multi-cancel",
        position_key=key,
        batch_id="batch_1",
        reserved_quantity=Decimal("10.0"),
    )
    r1 = PositionReservation(
        reservation_id="res_multi_cancel_1",
        command_id="order-multi-cancel",
        position_key=key,
        batch_id="batch_2",
        reserved_quantity=Decimal("15.0"),
    )
    repo.reservations[r0.reservation_id] = r0
    repo.reservations[r1.reservation_id] = r1

    class CancelBackend(BlockingBackend):
        async def cancel_order(self, plan: OrderExecutionPlan):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.CANCELED,
                exchange_order_id="e-cancel-1",
            )

    coordinator = OrderExecutionCoordinator(
        backend=CancelBackend(),
        account_label="primary",
        reservation_repository=repo,
    )
    allocs = (
        ExitAllocation(batch_id="batch_1", allocated_quantity=Decimal("10.0")),
        ExitAllocation(batch_id="batch_2", allocated_quantity=Decimal("15.0")),
    )
    plan = OrderExecutionPlan(
        intent_id="intent-cancel",
        run_id="run-1",
        client_order_id="order-multi-cancel",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("25.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        allocations=allocs,
    )
    _register_execution_command_with_reservations(coordinator, plan, [r0, r1])

    result = await coordinator.cancel_order(plan)
    assert result.state == ExchangeOrderState.CANCELED
    assert repo.reservations[r0.reservation_id].active_quantity == Decimal("0")
    assert repo.reservations[r0.reservation_id].released_quantity == Decimal("10.0")
    assert repo.reservations[r1.reservation_id].active_quantity == Decimal("0")
    assert repo.reservations[r1.reservation_id].released_quantity == Decimal("15.0")
    await coordinator.aclose()


async def test_terminal_order_with_zero_fill_releases_active_reservations() -> None:
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, PositionReservation] = {}
            self.release_reasons: dict[str, str] = {}

        def load_active_reservations(self, key: Any) -> list[PositionReservation]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: PositionReservation, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(
            self, res: PositionReservation, release_reason: str = ""
        ) -> None:
            self.reservations[res.reservation_id] = res
            if release_reason:
                self.release_reasons[res.reservation_id] = release_reason

    class RejectedBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission=None
        ):
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.REJECTED,
                exchange_order_id="e-rej-1",
                executed_quantity=Decimal("0"),
            )

    repo = InMemoryReservationRepo()
    coordinator = OrderExecutionCoordinator(
        backend=RejectedBackend(),
        account_label="primary",
        reservation_repository=repo,
    )
    plan = OrderExecutionPlan(
        intent_id="intent-reject",
        run_id="run-1",
        client_order_id="order-reject",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("5.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch_1",
    )

    result = await coordinator.submit(plan)
    assert result.state == ExchangeOrderState.REJECTED
    res = repo.reservations["res_order-reject"]
    assert res.active_quantity == Decimal("0")
    assert res.released_quantity == Decimal("5.0")
    assert repo.release_reasons.get(res.reservation_id) == "order_finished_rejected"
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_coordinator_execution_book_integration() -> None:
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    backend = BlockingBackend()
    repo = InMemoryPositionReservationRepository()
    custom_book = assemble_legacy_execution_book(reservation_repository=repo)

    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
        execution_book=custom_book,
    )

    assert coordinator.execution_book is custom_book

    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    view = await coordinator.execution_book.read(scope)
    assert view.projection_version.startswith("pv_")
    assert view.unallocated_quantity == Decimal("0")

    await coordinator.aclose()


@pytest.mark.asyncio
async def test_account_4_gray_cutover_activation() -> None:
    backend = BlockingBackend()
    coord_primary = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )
    coord_4 = OrderExecutionCoordinator(
        backend=backend,
        account_label="account-4",
    )
    # ExecutionBook is unconditionally authoritative across all accounts
    assert coord_primary.is_execution_book_enabled
    assert coord_4.is_execution_book_enabled

    await coord_primary.aclose()
    await coord_4.aclose()


@pytest.mark.asyncio
async def test_account_4_prohibits_synthetic_batches() -> None:
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    backend = BlockingBackend()
    repo = InMemoryPositionReservationRepository()

    synth_plan = OrderExecutionPlan(
        intent_id="intent-synth",
        run_id="run-1",
        client_order_id="order-synth-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch_BTCUSDT_BOTH",
    )

    coord_all = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
    )
    with pytest.raises(
        OrderPreSubmissionError,
        match="prohibited",
    ):
        await coord_all.submit(synth_plan)
    await coord_all.aclose()

    coord_4 = OrderExecutionCoordinator(
        backend=backend,
        account_label="account-4",
        reservation_repository=repo,
    )
    with pytest.raises(
        OrderPreSubmissionError,
        match="prohibited",
    ):
        await coord_4.submit(synth_plan)
    await coord_4.aclose()


@pytest.mark.asyncio
async def test_account_4_authoritative_reservation_and_outbox_lifecycle() -> None:
    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    backend = BlockingBackend()
    repo = InMemoryPositionReservationRepository()
    coord_4 = OrderExecutionCoordinator(
        backend=backend,
        account_label="account-4",
        reservation_repository=repo,
    )

    plan = OrderExecutionPlan(
        intent_id="intent-auth-1",
        run_id="run-1",
        client_order_id="order-auth-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("2.5"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="lot_20260927_001",
    )

    res = await coord_4.submit(plan)
    assert res.state == ExchangeOrderState.ACKNOWLEDGED

    # Outbox tracking in ExecutionBook
    book = coord_4.execution_book
    outbox = book.get_outbox(plan.client_order_id)
    assert outbox is not None
    assert outbox.state == DispatchState.ACKNOWLEDGED
    assert outbox.command.requested_quantity == Decimal("2.5")

    # Authoritative reservation recorded
    res = repo._reservations[f"res_{plan.client_order_id}"]
    assert res.batch_id == "lot_20260927_001"
    assert res.active_quantity == Decimal("2.5")

    # Command reservations registered
    assert book._command_reservations[plan.client_order_id] == [
        f"res_{plan.client_order_id}"
    ]

    await coord_4.aclose()


@pytest.mark.asyncio
async def test_account_4_outbox_marks_rejected_on_submission_failure() -> None:
    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    class FailingBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, **kwargs: Any
        ) -> Any:
            raise RuntimeError("Exchange API timeout")

    repo = InMemoryPositionReservationRepository()
    coord_4 = OrderExecutionCoordinator(
        backend=FailingBackend(),
        account_label="account-4",
        reservation_repository=repo,
    )

    plan = OrderExecutionPlan(
        intent_id="intent-fail-1",
        run_id="run-1",
        client_order_id="order-fail-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="lot_20260927_002",
    )

    with pytest.raises(RuntimeError, match="Exchange API timeout"):
        await coord_4.submit(plan)

    book = coord_4.execution_book
    outbox = book.get_outbox(plan.client_order_id)
    assert outbox is not None
    assert outbox.state == DispatchState.UNKNOWN
    assert "Exchange API timeout" in (outbox.last_error or "")

    # A transport failure does not prove the exchange rejected the order.
    res = repo._reservations[f"res_{plan.client_order_id}"]
    assert res.active_quantity == Decimal("1.0")

    await coord_4.aclose()


@pytest.mark.asyncio
async def test_dispatch_persistence_failure_prevents_exchange_post() -> None:
    from crypto_momentum_lab.domain.account import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    class DispatchFailingRepository:
        async def upsert_execution_command(self, **kwargs: Any) -> None:
            if kwargs["status"] == "dispatching":
                raise RuntimeError("dispatch write failed")

    backend = BlockingBackend()
    book = ExecutionBook(
        command_repository=LegacyCommandRepositoryAdapter(DispatchFailingRepository())
    )
    reservation_repo = InMemoryPositionReservationRepository()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=reservation_repo,
        execution_book=book,
    )
    plan = _plan("BTCUSDT", reduce_only=False)
    plan = OrderExecutionPlan(
        intent_id=plan.intent_id,
        run_id=plan.run_id,
        client_order_id=plan.client_order_id,
        symbol=plan.symbol,
        side=plan.side,
        order_type=plan.order_type,
        quantity=plan.quantity,
        price=plan.price,
        reduce_only=False,
        position_side=plan.position_side,
        created_at=plan.created_at,
        quantized=True,
    )
    await coordinator.observe_account_snapshot(
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("50000"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=None,
            margin_type=None,
            observed_at=NOW,
            raw_payload={},
        )
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        FactCoverageInterval,
        FactCoverageStatus,
        PositionKey,
    )

    journal = book._ensure_journal(
        PositionKey(
            environment=coordinator._environment,
            account_label=coordinator._account_label,
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.BOTH,
        )
    )
    journal.set_coverage(
        FactCoverageInterval(
            start_at=NOW,
            end_at=NOW,
            status=FactCoverageStatus.CONFIRMED,
        )
    )

    try:
        with pytest.raises(RuntimeError, match="dispatch write failed"):
            await asyncio.wait_for(coordinator.submit(plan), timeout=1)

        assert backend.calls == []
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_observation_failure_after_post_keeps_unknown_reservation() -> None:
    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    class FailAcknowledgementOnce:
        def __init__(self) -> None:
            self.failed = False

        async def upsert_execution_command(self, **kwargs: Any) -> None:
            if kwargs["status"] == "acknowledged" and not self.failed:
                self.failed = True
                raise RuntimeError("acknowledgement write failed")

    class AcceptedBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, **kwargs: Any
        ):
            self.calls.append(f"submit:{plan.symbol}:exit")
            return _result(plan, ExchangeOrderState.ACKNOWLEDGED)

    repo = InMemoryPositionReservationRepository()
    book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(FailAcknowledgementOnce()),
        reservation_repository=repo,
    )
    backend = AcceptedBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
        execution_book=book,
    )
    plan = OrderExecutionPlan(
        intent_id="intent-observe-persist-fail",
        run_id="run-1",
        client_order_id="order-observe-persist-fail",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch-explicit",
    )

    try:
        with pytest.raises(RuntimeError, match="acknowledgement write failed"):
            await asyncio.wait_for(coordinator.submit(plan), timeout=1)

        assert backend.calls == ["submit:BTCUSDT:exit"]
        assert book.get_outbox(plan.client_order_id).state is DispatchState.UNKNOWN
        reservation = repo._reservations[f"res_{plan.client_order_id}"]
        assert reservation.active_quantity == Decimal("1.0")
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_unknown_write_failure_seals_local_outbox_after_post() -> None:
    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )

    class FailAcknowledgementAndUnknown:
        async def upsert_execution_command(self, **kwargs: Any) -> None:
            if kwargs["status"] in ("acknowledged", "unknown"):
                raise RuntimeError(f"{kwargs['status']} write failed")

    class AcceptedBackend(BlockingBackend):
        async def submit(
            self, plan: OrderExecutionPlan, **kwargs: Any
        ):
            self.calls.append(f"submit:{plan.symbol}:exit")
            return _result(plan, ExchangeOrderState.ACKNOWLEDGED)

    reservation_repo = InMemoryPositionReservationRepository()
    book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(
            FailAcknowledgementAndUnknown()
        ),
        reservation_repository=reservation_repo,
    )
    backend = AcceptedBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=reservation_repo,
        execution_book=book,
    )
    plan = OrderExecutionPlan(
        intent_id="intent-unknown-write-fail",
        run_id="run-1",
        client_order_id="order-unknown-write-fail",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        batch_id="batch-unknown-write-fail",
    )

    try:
        with pytest.raises(RuntimeError, match="unknown write failed"):
            await asyncio.wait_for(coordinator.submit(plan), timeout=1)

        outbox = book.get_outbox(plan.client_order_id)
        assert outbox is not None
        assert outbox.state is DispatchState.UNKNOWN
        assert book._persistence_failed is True
        assert plan.client_order_id in (book._dispatch_reconciliation_required_commands)
        reservation = reservation_repo._reservations[f"res_{plan.client_order_id}"]
        assert reservation.active_quantity == Decimal("1.0")

        with pytest.raises(OrderPreSubmissionError):
            await asyncio.wait_for(coordinator.submit(plan), timeout=1)
        assert backend.calls == ["submit:BTCUSDT:exit"]
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_multi_batch_allocations_preserve_batch_quantities() -> None:
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class SpyRepo(InMemoryPositionReservationRepository):
        def __init__(self) -> None:
            super().__init__()
            self.saved_batch_quantities: dict[str, Decimal] | None = None

        def save_reservations(
            self,
            reservations: tuple[PositionReservation, ...],
            expected_projection_version: str | None = None,
            expires_at: datetime | None = None,
            batch_quantities: dict[str, Decimal] | None = None,
        ) -> None:
            self.saved_batch_quantities = batch_quantities
            super().save_reservations(
                reservations,
                expected_projection_version=expected_projection_version,
                expires_at=expires_at,
                batch_quantities=batch_quantities,
            )

    backend = BlockingBackend()
    repo = SpyRepo()
    coord = OrderExecutionCoordinator(
        backend=backend,
        account_label="account-4",
        reservation_repository=repo,
    )

    plan = OrderExecutionPlan(
        intent_id="intent-multi",
        run_id="run-1",
        client_order_id="order-multi-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("5.0"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        allocations=(
            ExitAllocation("lot_1", Decimal("2.0")),
            ExitAllocation("lot_2", Decimal("3.0")),
        ),
    )

    await coord.submit(plan)
    assert repo.saved_batch_quantities == {
        "lot_1": Decimal("2.0"),
        "lot_2": Decimal("3.0"),
    }
    await coord.aclose()


async def test_cumulative_executed_quantity_settlement_watermark() -> None:
    """Verifies §7.2: cumulative fill reports 3 -> 3 -> 5 only consume 5, not 11."""
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    res = PositionReservation(
        reservation_id="res-1",
        command_id="order-exit-1",
        position_key=key,
        batch_id="batch-1",
        reserved_quantity=Decimal("10.0"),
    )

    class InMemoryRepo:
        def __init__(self, initial: PositionReservation) -> None:
            self.res = initial

        def load_active_reservations(
            self, k: PositionKey | None = None
        ) -> list[PositionReservation]:
            return [self.res] if self.res.active_quantity > 0 else []

        def update_reservation(
            self, updated: PositionReservation, **kwargs: Any
        ) -> None:
            self.res = updated

    repo = InMemoryRepo(res)
    backend = BlockingBackend()
    coord = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
        initial_reservations=[res],
    )

    plan = OrderExecutionPlan(
        intent_id="intent-exit",
        run_id="run-1",
        client_order_id="order-exit-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("10.0"),
        price=Decimal("65000"),
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
    )
    _register_execution_command_with_reservations(coord, plan, [res])

    # 1st report: partial fill cum_qty = 3
    result_1 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        executed_quantity=Decimal("3.0"),
        average_price=Decimal("65000"),
        exchange_order_id="ex-1",
    )
    await coord._consume_reservation_if_filled(plan, result_1)
    assert repo.res.consumed_quantity == Decimal("3.0")
    assert repo.res.active_quantity == Decimal("7.0")

    # 2nd report: unchanged poll with same cum_qty = 3
    result_2 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        executed_quantity=Decimal("3.0"),
        average_price=Decimal("65000"),
        exchange_order_id="ex-1",
    )
    await coord._consume_reservation_if_filled(plan, result_2)
    assert repo.res.consumed_quantity == Decimal("3.0")
    assert repo.res.active_quantity == Decimal("7.0")

    # 3rd report: further fill cum_qty = 5
    result_3 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        executed_quantity=Decimal("5.0"),
        average_price=Decimal("65000"),
        exchange_order_id="ex-1",
    )
    await coord._consume_reservation_if_filled(plan, result_3)
    assert repo.res.consumed_quantity == Decimal("5.0")
    assert repo.res.active_quantity == Decimal("5.0")

    await coord.aclose()


@pytest.mark.asyncio
async def test_first_live_entry_reservation_on_cold_start() -> None:
    """Verifies F1: first live entry on empty ExecutionBook succeeds and is admitted."""
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        ExecutionCoordinator,
    )

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, Any] = {}

        def load_active_reservations(self, key: Any = None) -> list[Any]:
            return [
                r
                for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: Any, **kwargs: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            self.reservations[res.reservation_id] = res

    backend = BlockingBackend()
    repo = InMemoryReservationRepo()
    domain_coord = ExecutionCoordinator()
    book = assemble_legacy_execution_book(
        coordinator=domain_coord, reservation_repository=repo
    )
    coord = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
        domain_coordinator=domain_coord,
        execution_book=book,
    )
    snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=Decimal("10"),
        margin_type="cross",
        observed_at=NOW,
        raw_payload={},
    )
    await coord.observe_account_snapshot(snap)
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope

    scope = ExecutionScope(
        environment=coord._environment,
        account_label=coord._account_label,
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = await coord.execution_book.read(scope)
    plan = OrderExecutionPlan(
        intent_id="intent-entry-1",
        run_id="run-1",
        client_order_id="order-entry-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.01"),
        price=Decimal("65000"),
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        projection_version=view.projection_version,
    )
    # Must succeed without OrderPreSubmissionError: PositionView is not ready for trade
    await coord._ensure_reservation(plan)

    # Outbox should have the command prepared
    outbox = book.get_outbox("order-entry-1")
    assert outbox is not None
    assert outbox.command.requested_quantity == Decimal("0.01")
    assert not outbox.command.reduce_only

    await coord.aclose()


@pytest.mark.asyncio
async def test_snapshot_ingestion_requires_typed_facts_without_inferred_flat() -> None:
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook

    book = ExecutionBook()
    coord = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        execution_book=book,
    )
    flat = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=None,
        margin_type=None,
        observed_at=NOW,
        raw_payload={},
    )

    await coord.observe_account_snapshot(flat, symbols=("ETHUSDT",))

    btc_view = await book.read(
        ExecutionScope(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.BOTH,
        )
    )
    eth_view = await book.read(
        ExecutionScope(
            environment="live",
            account_label="primary",
            symbol="ETHUSDT",
            position_side=FuturesPositionSide.BOTH,
        )
    )
    assert btc_view.zero_position_snapshot_confirmed is True
    assert btc_view.is_ready_for_trade is True
    assert eth_view.zero_position_snapshot_confirmed is False
    assert eth_view.is_ready_for_trade is False

    with pytest.raises(ValueError, match="account_label"):
        await coord.observe_account_snapshot(
            AccountPositionSnapshot(
                environment="live",
                account_label="another-account",
                symbol="BTCUSDT",
                position_side="BOTH",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("65000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=None,
                margin_type=None,
                observed_at=NOW,
                raw_payload={},
            )
        )
    with pytest.raises(TypeError, match="AccountPositionSnapshot"):
        await coord.observe_account_snapshot(object())

    await coord.aclose()


@pytest.mark.asyncio
async def test_repeated_flat_snapshot_is_ingested_once_per_stream() -> None:
    from dataclasses import replace
    from datetime import timedelta

    from crypto_momentum_lab.domain.account.models import (
        AccountFillEvent,
        AccountPositionSnapshot,
    )
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook

    class Book(ExecutionBook):
        has_execution_unit_of_work = True

        def __init__(self) -> None:
            super().__init__()
            self.evidence = []

        async def observe(self, evidence):
            self.evidence.append(evidence)
            return object()

    book = Book()
    coord = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        domain_coordinator=object(),
        execution_book=book,
    )
    flat = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=None,
        margin_type=None,
        observed_at=NOW,
        raw_payload={},
    )

    async def observe(snapshot, *, epoch="epoch-1", sequence=1, fills=()):
        await coord.observe_account_snapshot(
            snapshot,
            fills=fills,
            stream_id="account-stream",
            stream_epoch=epoch,
            sequence=sequence,
        )

    await observe(flat)
    assert book.get_active_stream("live", "primary") == ("account-stream", "epoch-1")
    await observe(replace(flat, observed_at=NOW + timedelta(minutes=1)), sequence=2)
    assert len(book.evidence) == 1

    await observe(flat, epoch="epoch-2", sequence=1)
    assert book.get_active_stream("live", "primary") == ("account-stream", "epoch-2")
    assert len(book.evidence) == 2

    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="new-fill",
        order_id="new-order",
        side="BUY",
        price=Decimal("65000"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW + timedelta(minutes=2),
        raw_payload={"positionSide": "LONG"},
    )
    await observe(flat, epoch="epoch-2", sequence=2, fills=(fill,))
    await observe(flat, epoch="epoch-2", sequence=3)
    assert len(book.evidence) == 4


@pytest.mark.asyncio
async def test_snapshot_evidence_identity_keeps_position_sides_distinct() -> None:
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook

    book = ExecutionBook()
    coord = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        execution_book=book,
    )
    for side in ("LONG", "SHORT"):
        await coord.observe_account_snapshot(
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side=side,
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("65000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=None,
                margin_type=None,
                observed_at=NOW,
                raw_payload={},
            )
        )

    for side in (FuturesPositionSide.LONG, FuturesPositionSide.SHORT):
        view = await book.read(
            ExecutionScope(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side=side,
            )
        )
        assert view.zero_position_snapshot_confirmed is True
        assert view.is_ready_for_trade is True

    await coord.aclose()


@pytest.mark.asyncio
async def test_cumulative_fill_reconciliation_exact_deltas() -> None:
    """Verify 3 -> 5 -> 10 cumulative reports settle only their deltas."""
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    res = PositionReservation(
        reservation_id="res-cum-1",
        command_id="order-exit-cum",
        batch_id="batch-1",
        position_key=PositionKey(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.BOTH,
        ),
        reserved_quantity=Decimal("10.0"),
        consumed_quantity=Decimal("0.0"),
        released_quantity=Decimal("0.0"),
        created_at=NOW,
    )

    class InMemoryRepo:
        def __init__(self, initial: PositionReservation) -> None:
            self.res = initial

        def load_active_reservations(
            self, k: PositionKey | None = None
        ) -> list[PositionReservation]:
            return [self.res] if self.res.active_quantity > 0 else []

        def update_reservation(
            self, updated: PositionReservation, **kwargs: Any
        ) -> None:
            self.res = updated

    backend = BlockingBackend()
    repo = InMemoryRepo(res)
    coord = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        reservation_repository=repo,
        initial_reservations=[res],
    )
    plan = OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="order-exit-cum",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("10.0"),
        price=Decimal("65000"),
        reduce_only=True,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        batch_id="batch-1",
    )
    _register_execution_command_with_reservations(coord, plan, [res])

    # 1. First report: 3 partial
    res1 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        executed_quantity=Decimal("3.0"),
        average_price=Decimal("100"),
        exchange_order_id="ex-cum-1",
    )
    await coord._consume_reservation_if_filled(plan, res1)
    assert repo.res.consumed_quantity == Decimal("3.0")
    assert repo.res.active_quantity == Decimal("7.0")

    # 2. Second report: 5 partial (cumulative 5)
    res2 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        executed_quantity=Decimal("5.0"),
        average_price=Decimal("140"),
        exchange_order_id="ex-cum-1",
    )
    await coord._consume_reservation_if_filled(plan, res2)
    assert repo.res.consumed_quantity == Decimal("5.0")
    assert repo.res.active_quantity == Decimal("5.0")

    # 3. Third report: 10 filled (cumulative 10)
    res3 = OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.FILLED,
        executed_quantity=Decimal("10.0"),
        average_price=Decimal("130"),
        exchange_order_id="ex-cum-1",
    )
    await coord._consume_reservation_if_filled(plan, res3)
    assert repo.res.consumed_quantity == Decimal("10.0")
    assert repo.res.active_quantity == Decimal("0.0")

    order_fills = [
        fill
        for fill in coord._execution_book._ensure_journal(
            PositionKey(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side=FuturesPositionSide.BOTH,
            )
        )
        .read_cut()
        .fills
        if fill.order_id == plan.client_order_id
    ]
    # Under RFC 2026-09-25 authoritative model, cumulative order reports settle
    # reservations but do not fabricate synthetic AccountFillEvent into the journal.
    assert order_fills == []

    await coord.aclose()


@pytest.mark.asyncio
async def test_execution_book_observes_monotonic_cumulative_fill_facts() -> None:
    from crypto_momentum_lab.domain.account.models import (
        AccountFillEvent,
        AccountPositionSnapshot,
    )
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
    from crypto_momentum_lab.domain.execution.execution_book import (
        ExecutionBook,
        ExecutionRequest,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        FactCoverageInterval,
        FactCoverageStatus,
    )
    from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType

    book = ExecutionBook()
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    seed_fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="seed-long-position",
        order_id="seed-entry-order",
        side="BUY",
        price=Decimal("65000"),
        quantity=Decimal("10"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"positionSide": "LONG"},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("10"),
        entry_price=Decimal("65000"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("650000"),
        leverage=5,
        margin_type="cross",
        observed_at=NOW,
        raw_payload={},
    )
    journal = book._ensure_journal(scope.to_position_key())
    journal.set_coverage(
        FactCoverageInterval(
            start_at=NOW,
            end_at=NOW,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="seed-execution-book-position",
            scope=scope,
            observed_at=NOW,
            fill=seed_fill,
            snapshot=snapshot,
        )
    )
    view = await book.read(scope)
    exit_request = ExecutionRequest(
        request_id="order-cumulative-10",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-cum",
        decision_ref="decision-cum",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("10"),
    )
    assert await book.act(exit_request)
    plan = OrderExecutionPlan(
        intent_id="intent-cum",
        run_id="run-cum",
        client_order_id=exit_request.request_id,
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("10"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
    )
    coord = OrderExecutionCoordinator(
        backend=BlockingBackend(),
        account_label="primary",
        execution_book=book,
    )

    async def observe(state: ExchangeOrderState, cumulative: str) -> None:
        await coord._observe_order_result_in_execution_book(
            plan,
            OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=state,
                executed_quantity=Decimal(cumulative),
                average_price=Decimal("65000"),
                exchange_order_id="exchange-cum-10",
            ),
        )

    await observe(ExchangeOrderState.PARTIALLY_FILLED, "3")
    await observe(ExchangeOrderState.PARTIALLY_FILLED, "3")
    await observe(ExchangeOrderState.PARTIALLY_FILLED, "5")
    await observe(ExchangeOrderState.FILLED, "10")
    await observe(ExchangeOrderState.PARTIALLY_FILLED, "7")

    facts = journal.read_cut()
    order_fills = [
        fill for fill in facts.fills if fill.order_id == plan.client_order_id
    ]
    # Under RFC 2026-09-25 authoritative model, cumulative order reports settle
    # reservations but do not fabricate synthetic AccountFillEvent into the journal.
    assert order_fills == []
    reservations = book.get_active_reservations(scope.to_position_key())
    assert reservations == ()

    await coord.aclose()


@pytest.mark.parametrize("serialize_commands", [False, True])
async def test_ws_fact_commits_while_same_position_rest_recovery_is_waiting(
    serialize_commands,
):
    from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderSnapshot
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionStateMachine,
        SubmitPolicy,
    )

    started = asyncio.Event()
    release = asyncio.Event()
    events = []
    plan = _plan("BTCUSDT", reduce_only=False)
    snapshot = ExchangeOrderSnapshot(
        client_order_id=plan.client_order_id,
        exchange_order_id="123",
        state=ExchangeOrderState.FILLED,
        observed_at=NOW,
        executed_quantity=plan.quantity,
        average_price=Decimal("100"),
    )

    class Exchange:
        async def query_order_by_client_id(self, _symbol, _client_order_id):
            started.set()
            await release.wait()
            return snapshot

    class Repository:
        async def append_order_event(self, event):
            if any(item.event_id == event.event_id for item in events):
                return False
            events.append(event)
            return True

    backend = OrderExecutionStateMachine(
        exchange=Exchange(),
        repository=object(),
        event_repository=Repository(),
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        serialize_commands=serialize_commands,
    )
    coordinator = OrderExecutionCoordinator(backend=backend, account_label="primary")
    recovery = asyncio.create_task(coordinator.reconcile_order(plan))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        observed = await asyncio.wait_for(
            coordinator.apply_observed_snapshot(plan, snapshot),
            timeout=1,
        )
        assert not recovery.done()
        assert observed.state is ExchangeOrderState.FILLED
        assert len(events) == 1
        assert events[0].details["executed_quantity"] == str(plan.quantity)
    finally:
        release.set()
        await recovery
        await coordinator.aclose()
    # The later REST response is the same fact, so its replay does not append again.
    assert len(events) == 1


async def test_closed_coordinator_rejects_independent_fact():
    coordinator = OrderExecutionCoordinator(backend=object(), account_label="primary")
    await coordinator.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await coordinator.apply_observed_snapshot(
            _plan("BTCUSDT", reduce_only=False), object()
        )


async def test_incomplete_ws_update_durably_uses_existing_uncertainty_gate_without_rest():
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionStateMachine,
        SubmitPolicy,
    )
    from crypto_momentum_lab.live_rollout.gates import order_state_is_uncertain

    events = []
    plan = _plan("BTCUSDT", reduce_only=False)

    class Repository:
        async def append_order_event(self, event):
            events.append(event)
            return True

    backend = OrderExecutionStateMachine(
        exchange=object(),
        repository=object(),
        event_repository=Repository(),
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )
    coordinator = OrderExecutionCoordinator(backend=backend, account_label="primary")
    try:
        result = await coordinator.mark_reconciliation_pending(plan)
        assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
        assert len(events) == 1
        assert events[0].occurred_at == NOW
        assert order_state_is_uncertain(events[0].state)
        assert events[0].details["reason"] == "incomplete_ws_order_update"
    finally:
        await coordinator.aclose()
