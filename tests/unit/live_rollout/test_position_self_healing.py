"""Unit tests for automated unmanaged position self-healing."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)

NOW = datetime(2026, 9, 29, 6, 7, 1, tzinfo=UTC)


class FakeBackend(OrderExecutionPort):
    def __init__(self) -> None:
        self.submitted_plans: list[OrderExecutionPlan] = []

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission=None,
    ) -> OrderExecutionResult:
        self.submitted_plans.append(plan)
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="ex-12345",
            executed_quantity=plan.quantity,
            average_price=Decimal("0.7248"),
            plan=plan,
        )

    async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-12345",
            plan=plan,
        )

    async def reconcile_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="ex-12345",
            executed_quantity=plan.quantity,
            average_price=Decimal("0.7248"),
            plan=plan,
        )

    async def apply_observed_snapshot(self, plan, snapshot):
        raise NotImplementedError

    async def mark_absent_reconciled(self, plan, *, details):
        raise NotImplementedError


def _plan(symbol: str = "GRASSUSDT") -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{symbol}",
        run_id="live-run-1",
        client_order_id=f"cml_{symbol.lower()}_123",
        symbol=symbol,
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("137.6"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        quantized=True,
    )


@pytest.mark.asyncio
async def test_coordinator_resolves_active_stream_on_cold_start() -> None:
    backend = FakeBackend()
    book = ExecutionBook(execution_unit_of_work=AsyncMock())
    book._persistence_failed = False
    book.observe = AsyncMock(
        return_value=Applied(evidence_id="ev-mock", updated_view_token="token-1")
    )
    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="test-epoch-1234",
    )

    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        environment="live",
        execution_book=book,
    )

    plan = _plan("GRASSUSDT")
    # Submitting an order for a new symbol with no prior restored stream identity
    # must resolve from the active stream rather than raising RuntimeError
    result = await coordinator.submit(plan)
    assert result.state is ExchangeOrderState.FILLED
    assert result.executed_quantity == Decimal("137.6")
    book.observe.assert_called_once()
    evidence = book.observe.call_args[0][0]
    assert evidence.stream_id == "account_event_hub"
    assert evidence.stream_epoch == "test-epoch-1234"
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_execution_book_get_active_stream() -> None:
    book = ExecutionBook()
    book.register_active_stream(
        environment="live",
        account_label="account-2",
        stream_id="account_event_hub",
        stream_epoch="epoch-abc",
    )

    active = book.get_active_stream("live", "account-2")
    assert active == ("account_event_hub", "epoch-abc")

    missing = book.get_active_stream("live", "nonexistent")
    assert missing is None


@pytest.mark.parametrize("stale", [False, True])
async def test_repair_worker_coalesces_latest_context_and_rejects_stale_requests(monkeypatch, stale):
    from dataclasses import replace
    from types import SimpleNamespace

    from crypto_momentum_lab.live_rollout.position_self_healing import (
        LiveUnmanagedPositionRepair,
    )
    from tests.unit.live_rollout.test_postgres_runtime import (
        _position,
        _runtime_context,
    )

    calls = []
    wakes = []
    invalidations = []
    current = [True]

    async def repair(**kwargs):
        calls.append(kwargs["request"])
        assert kwargs["is_current"]()
        return True

    monkeypatch.setattr("crypto_momentum_lab.live_rollout.position_self_healing.auto_heal_unmanaged_position", repair)
    runtime = LiveUnmanagedPositionRepair(account_label="primary", run_id="run-1",
        book=SimpleNamespace(get_active_stream=lambda *args: ("hub", "epoch")), uow=object(),
        context_is_current=lambda context: current[0],
        invalidate_context=lambda: invalidations.append(1),
        request_recovery=lambda: wakes.append(1))
    old = replace(_runtime_context(), unmanaged_position_symbols=frozenset({"BTCUSDT"}),
        account_snapshot=SimpleNamespace(positions=(_position(position_amt=Decimal("1")),)))
    latest = replace(old, account_snapshot=SimpleNamespace(positions=(
        _position(position_amt=Decimal("2")),)))
    runtime.request(old)
    runtime.request(latest)
    assert not calls
    current[0] = not stale
    await runtime.repair_pending()
    assert len(calls) == (0 if stale else 1)
    if calls:
        assert calls[0].expected_quantity == Decimal("2")
    assert len(invalidations) == (0 if stale else 1)
    await runtime.repair_pending()
    assert len(calls) == (0 if stale else 1)


async def test_repair_context_advanced_during_fact_read_cannot_persist():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.position_repair_models import (
        PositionRepairRequest,
    )
    from crypto_momentum_lab.live_rollout.position_self_healing import (
        auto_heal_unmanaged_position,
    )

    current = [True]
    persisted = AsyncMock()
    async def load(request):
        current[0] = False
        return object()

    class Uow:
        @asynccontextmanager
        async def transaction(self, key):
            yield SimpleNamespace(load_repair_facts=load, persist_repair=persisted)

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    request = PositionRepairRequest(key=key, run_id="run-1",
        scope=AccountFactStreamScope.for_position_key(key, stream_id="hub", stream_epoch="epoch"),
        expected_quantity=Decimal("1"), observed_at=NOW)
    book = SimpleNamespace(reload_position=AsyncMock())
    assert not await auto_heal_unmanaged_position(request=request, uow=Uow(), book=book,
                                                 is_current=lambda: current[0])
    persisted.assert_not_awaited()
    book.reload_position.assert_not_awaited()


async def test_context_reads_continue_while_existing_worker_repairs(monkeypatch):
    import asyncio
    from dataclasses import replace
    from types import SimpleNamespace

    from crypto_momentum_lab.live_rollout.order_reconciliation import (
        LiveOrderReconciliation,
    )
    from crypto_momentum_lab.live_rollout.position_self_healing import (
        LiveUnmanagedPositionRepair,
    )
    from crypto_momentum_lab.live_rollout.postgres_runtime import (
        PostgresLiveContextProvider,
    )
    from tests.unit.live_rollout.test_postgres_runtime import (
        _position,
        _runtime_context,
    )

    started, release = asyncio.Event(), asyncio.Event()
    async def repair(**kwargs):
        started.set()
        await release.wait()
        return True
    monkeypatch.setattr("crypto_momentum_lab.live_rollout.position_self_healing.auto_heal_unmanaged_position", repair)
    book = SimpleNamespace(list_position_views=AsyncMock(return_value=()),
                           get_active_stream=lambda *args: ("hub", "epoch"))
    provider = object.__new__(PostgresLiveContextProvider)
    provider._account_label = "primary"
    provider._execution_book = book
    provider._observe_book_drift = AsyncMock()
    worker = LiveOrderReconciliation(order_repository=SimpleNamespace(
        load_unresolved_orders=AsyncMock(return_value=())), state_machine=object(), run_id="run-1")
    runtime = LiveUnmanagedPositionRepair(account_label="primary", run_id="run-1",
        book=book, uow=object(), context_is_current=lambda context: True,
        invalidate_context=lambda: None, request_recovery=worker.request_recovery)
    provider._request_position_repair = runtime.request
    worker.repair_positions = runtime.repair_pending
    context = replace(_runtime_context(), open_position_symbols=frozenset({"BTCUSDT"}),
        account_snapshot=SimpleNamespace(positions=(_position(),)))
    task = asyncio.create_task(worker.run_periodically())
    try:
        await provider._with_execution_book(context, SimpleNamespace(bucket_end=NOW))
        await asyncio.wait_for(started.wait(), 1)
        # Force another projection read while the actual repair callback remains blocked.
        provider._cached_book_result = None
        result = await asyncio.wait_for(provider._with_execution_book(context,
            SimpleNamespace(bucket_end=NOW)), 0.5)
        assert result.unmanaged_position_symbols == frozenset({"BTCUSDT"})
        assert not release.is_set()
    finally:
        task.cancel()
        release.set()
        await asyncio.gather(task, return_exceptions=True)
