import asyncio
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)

NOW = datetime(2026, 8, 22, tzinfo=UTC)


class BlockingBackend:
    def __init__(self) -> None:
        self.query_started = asyncio.Event()
        self.release_query = asyncio.Event()
        self.submit_started = asyncio.Event()
        self.calls: list[str] = []

    async def execute_approved_intent(self, plan: OrderExecutionPlan):
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
