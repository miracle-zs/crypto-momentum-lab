import asyncio
from datetime import UTC, datetime
from decimal import Decimal

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
