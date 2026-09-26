import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionKey,
    _KeyCommandScheduler,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    OrderPreSubmissionError,
)

NOW = datetime(2026, 8, 22, tzinfo=UTC)


class BlockingBackend:
    def __init__(self) -> None:
        self.query_started = asyncio.Event()
        self.release_query = asyncio.Event()
        self.submit_started = asyncio.Event()
        self.calls: list[str] = []

    async def execute_approved_intent(
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

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission=None,
    ):
        result = await super().execute_approved_intent(
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
        coordinator.execute_approved_intent(_plan("BTCUSDT", reduce_only=True))
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
        coordinator.execute_approved_intent(_plan("BTCUSDT", reduce_only=False))
    )
    exit_task = asyncio.create_task(
        coordinator.execute_approved_intent(_plan("BTCUSDT", reduce_only=True))
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


async def test_coordinator_close_waits_for_inflight_submit_after_caller_cancel(
) -> None:
    backend = BlockingSubmitBackend()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
    )
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
    backend.release_submit.set()
    await close_task
    assert backend.calls == ["submit:BTCUSDT:entry"]


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
    drain_task = asyncio.create_task(
        coordinator.wait_for_entry_submissions_idle()
    )
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
    from crypto_momentum_lab.execution_account.orders.state_machine import (
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

    # But an EXIT command (reduce_only=True) CAN still enter because headroom is reserved for exits!
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

    # Unblock the worker; the entry command waited > 0.05s so it should fail with timeout
    backend.release_query.set()
    await block_task

    with pytest.raises(OrderPreSubmissionError, match="waited .* in queue exceeding limit"):
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
    # The caller must time out within max_queue_wait_seconds without waiting forever for worker
    with pytest.raises(OrderPreSubmissionError, match="waited .* in queue exceeding limit"):
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
    e1_task = asyncio.create_task(coordinator.submit(_plan("BTCUSDT", reduce_only=False)))
    e2_task = asyncio.create_task(coordinator.submit(_plan("BTCUSDT", reduce_only=False)))
    await asyncio.sleep(0.01)

    # Attempting another entry fails due to entry capacity
    with pytest.raises(OrderPreSubmissionError, match="entry capacity exceeded"):
        await coordinator.submit(_plan("BTCUSDT", reduce_only=False))

    # But cancel_order for a non-reduce_only entry order MUST still succeed using exit priority!
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

    cancel_res = await coordinator.cancel_order(plan)
    assert cancel_res.state is ExchangeOrderState.CANCELED

    # Reservation should be released
    active = reservation_repo.load_active_reservations(key)
    assert len(active) == 0
    saved = reservation_repo._reservations["res_test_123"]
    assert saved.released_quantity == Decimal("1.5")
    assert saved.active_quantity == Decimal("0")
    await coordinator.aclose()


async def test_cancel_order_does_not_release_reservation_if_state_not_canceled() -> None:
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

        def save_reservation(self, res: Any) -> None:
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


async def test_multi_batch_reservation_release_all_on_failure() -> None:
    backend = BlockingBackend()

    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, Any] = {}

        def load_active_reservations(self, key: Any) -> list[Any]:
            return [
                r for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: Any) -> None:
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
        async def execute_approved_intent(
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

    # Both batch_1 and batch_2 reservations must be released (active_quantity == 0)
    assert len(repo.reservations) == 2
    for r in repo.reservations.values():
        assert r.active_quantity == Decimal("0")
        assert r.released_quantity > Decimal("0")

    await coordinator.aclose()


async def test_multi_batch_reservation_consume_across_batches() -> None:
    class InMemoryReservationRepo:
        def __init__(self) -> None:
            self.reservations: dict[str, Any] = {}

        def load_active_reservations(self, key: Any) -> list[Any]:
            return [
                r for r in self.reservations.values()
                if r.active_quantity > Decimal("0")
            ]

        def save_reservation(self, res: Any) -> None:
            self.reservations[res.reservation_id] = res

        def update_reservation(self, res: Any, release_reason: str = "") -> None:
            self.reservations[res.reservation_id] = res

    class FillBackend(BlockingBackend):
        async def execute_approved_intent(
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
