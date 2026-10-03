"""Account facts must resume existing recovery, not create new repair work."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.account_channel import LiveAccountEventRuntime
from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
    recover_restored_commands,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.live_rollout.position_self_healing import (
    LiveUnmanagedPositionRepair,
)
from tests.unit.execution.test_terminal_settlement import NOW, SCOPE
from tests.unit.live_rollout.test_order_reconciliation import _ws_order_case
from tests.unit.live_rollout.test_postgres_runtime import _position, _runtime_context


@pytest.mark.asyncio
async def test_normal_account_events_do_not_start_order_or_exit_recovery() -> None:
    orders = AsyncMock(return_value=())
    exits = AsyncMock(return_value=False)
    recovery = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(),
        "run-1",
        recover_exits=exits,
    )
    cache = SimpleNamespace(for_symbols=lambda _symbols: ())
    channel = LiveAccountEventRuntime(
        daemon=SimpleNamespace(),
        latest_market_states=cache,
        latest_market_quotes=cache,
        order_reconciliation=recovery,
        is_transient_error=lambda _error: False,
        # Match the runtime's account-fact publication callback.
        on_account_snapshot=lambda _event: recovery.notify_account_facts_changed(),
    )
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=False,
        symbols=(),
    )
    task = asyncio.create_task(recovery.run_requested())
    try:
        for _ in range(20):
            assert await channel._process_event(event) is None
            await asyncio.sleep(0)
        orders.assert_not_awaited()
        exits.assert_not_awaited()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup_failure", [False, True])
async def test_restored_command_and_unresolved_order_share_one_lookup_per_pass(
    lookup_failure: bool,
) -> None:
    order, _ = _ws_order_case()
    order = replace(order, state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION)
    book = ExecutionBook()
    command = TradeCommand(
        order.plan.client_order_id,
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        order.plan.quantity,
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    await book.mark_unknown(command.command_id, "response lost")
    orders = SimpleNamespace(
        load_order=AsyncMock(return_value=order),
        load_unresolved_orders=AsyncMock(return_value=(order,)),
    )
    coordinator = SimpleNamespace(reconcile_order=AsyncMock())
    if lookup_failure:
        coordinator.reconcile_order.side_effect = [
            ConnectionError("offline"),
            None,
            None,
        ]
    recovery = LiveOrderReconciliation(
        orders,
        coordinator,
        order.plan.run_id,
        recover_commands=lambda reconcile_order: recover_restored_commands(
            book=book,
            coordinator=coordinator,
            orders=orders,
            reconcile_order=reconcile_order,
        ),
    )

    if lookup_failure:
        with pytest.raises(ConnectionError, match="offline"):
            await recovery.reconcile_all()
        assert coordinator.reconcile_order.await_count == 1
    assert await recovery.reconcile_all()
    assert coordinator.reconcile_order.await_count == 1 + int(lookup_failure)
    assert coordinator.reconcile_order.await_args.args == (order.plan,)
    # Deduplication must expire: uncertainty still needs a later retry.
    assert await recovery.reconcile_all()
    assert coordinator.reconcile_order.await_count == 2 + int(lookup_failure)


@pytest.mark.asyncio
async def test_new_facts_resume_pending_exit_then_idle_updates_stop_scanning() -> None:
    first = asyncio.Event()
    second = asyncio.Event()
    calls = 0

    async def exits() -> bool:
        nonlocal calls
        calls += 1
        (first if calls == 1 else second).set()
        return calls == 1

    orders = AsyncMock(return_value=())
    recovery = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(),
        "run",
        interval_seconds=3600,
        recover_exits=exits,
    )
    task = asyncio.create_task(recovery.run_requested())
    try:
        recovery.request_recovery()
        await asyncio.wait_for(first.wait(), 1)
        recovery.notify_account_facts_changed()
        await asyncio.wait_for(second.wait(), 1)
        for _ in range(20):
            recovery.notify_account_facts_changed()
            await asyncio.sleep(0)
        assert orders.await_count == 1
        assert calls == 2
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_facts_arriving_during_recovery_retain_one_followup_pass() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    followup = asyncio.Event()
    calls = 0

    async def exits() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
        else:
            followup.set()
        return False

    recovery = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=AsyncMock(return_value=())),
        SimpleNamespace(),
        "run",
        interval_seconds=3600,
        recover_exits=exits,
    )
    task = asyncio.create_task(recovery.run_requested())
    try:
        recovery.request_recovery()
        await asyncio.wait_for(started.wait(), 1)
        for _ in range(20):
            recovery.notify_account_facts_changed()
        assert calls == 1
        release.set()
        await asyncio.wait_for(followup.wait(), 1)
        await asyncio.sleep(0)
        assert calls == 2
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unfinished_position_repair_retries_without_more_account_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repaired = asyncio.Event()
    attempts = 0

    async def repair(**kwargs: object) -> bool:
        nonlocal attempts
        attempts += 1
        return attempts >= 2

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.position_self_healing"
        ".auto_heal_unmanaged_position",
        repair,
    )
    recovery = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=AsyncMock(return_value=())),
        SimpleNamespace(),
        "run",
        interval_seconds=0.01,
    )
    positions = LiveUnmanagedPositionRepair(
        account_label="primary",
        run_id="run",
        book=SimpleNamespace(get_active_stream=lambda *_args: ("hub", "epoch")),
        uow=SimpleNamespace(),
        context_is_current=lambda _context: True,
        invalidate_context=repaired.set,
        request_recovery=recovery.request_recovery,
    )
    recovery.repair_positions = positions.repair_pending
    context = replace(
        _runtime_context(),
        unmanaged_position_symbols=frozenset({"BTCUSDT"}),
        account_snapshot=SimpleNamespace(positions=(_position(),)),
    )
    task = asyncio.create_task(recovery.run_requested())
    try:
        positions.request(context)
        await asyncio.wait_for(repaired.wait(), 0.5)
        await asyncio.sleep(0.03)
        assert attempts == 2
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
