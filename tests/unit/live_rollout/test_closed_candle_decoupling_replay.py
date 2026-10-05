"""Replay real candle decisions through Submission, Coordinator and exchange port."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderProjectionConflictError,
)
from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ManagedLivePosition,
)
from tests.unit.live_rollout.test_daemon import (
    NOW,
    FakeLiveRepository,
    PlanAwareExchange,
    _daemon,
    _runtime_context,
)


def _event():
    end = NOW - timedelta(seconds=20)
    return ClosedCandle15mEvent(
        candle=ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=end - timedelta(minutes=15),
            candle_end=end,
            open_price=Decimal("31000"),
            close_price=Decimal("30000"),
        ),
        exchange_event_at=end,
        received_at=NOW,
    )


def _position():
    return ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("31000"),
        opened_at=NOW - timedelta(minutes=45),
        batch_id="batch-1",
        projection_version="pv1",
    )


def _manager(ttl=60):
    return LiveExitManager(
        config=LiveExitConfig(account_label="primary", candle_grace_decision_profit_pct=Decimal("0"),
            run_id="run-1",
            strategy_name="compression_breakout",
            strategy_version="v0",
            strategy_config_hash="a" * 64,
            policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
            candidate_ttl_seconds=ttl,
        )
    )


@pytest.fixture
def pending_signal(monkeypatch):
    from crypto_momentum_lab.live_rollout import exit_channels

    pending = asyncio.Event()
    original = exit_channels.log.warning

    def warning(event, **details):
        original(event, **details)
        if event == "live_closed_candle_position_sync_pending":
            pending.set()

    monkeypatch.setattr(exit_channels.log, "warning", warning)
    return pending


@pytest.mark.parametrize(
    "ttl, delay, expected_posts", [(60, 30, 1), (60, 224, 0), (300, 224, 1)]
)
async def test_original_candle_ttl_survives_late_and_duplicate_delivery(
    ttl, delay, expected_posts
):
    exchange = PlanAwareExchange()
    now = NOW
    context = replace(
        _runtime_context(),
        pending_position_symbols=frozenset({"BTCUSDT"}),
        managed_positions=(_position(),),
        open_position_symbols=frozenset({"BTCUSDT"}),
    )

    async def provider(state):
        return context

    daemon = _daemon(
        exchange=exchange,
        context_provider=provider,
        exit_manager=_manager(ttl),
        clock=lambda: now,
    )
    failures = []
    runtime = LiveExitChannelRuntime(
        daemon=daemon,
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
        closed_candle_expires_at=daemon.closed_candle_expires_at,
        clock=lambda: now,
        on_exit_failure=lambda symbol, reason: failures.append(reason),
    )

    async def source():
        yield _event()
        yield _event()

    now += timedelta(seconds=delay)
    await asyncio.wait_for(runtime.run_closed_candle_channel(source=source()), 1)
    assert len(exchange.plans) == expected_posts
    if expected_posts:
        plan = exchange.plans[0]
        assert plan.reduce_only
        assert plan.quantity == Decimal("0.001")
    else:
        assert "closed_candle_evaluation_expired:BTCUSDT" in failures
    # Reconnection repeats the same official event; it must not POST twice.
    await asyncio.wait_for(runtime.run_closed_candle_channel(source=source()), 1)
    assert len(exchange.plans) == expected_posts


async def test_projection_conflict_rebuilds_real_allocation_before_post(pending_signal):
    attempted = []

    class Repository(FakeLiveRepository):
        async def prepare_submission_in_session(self, session, **kwargs):
            plan = kwargs["plan"]
            attempted.append(plan)
            if plan.projection_version == "pv1":
                raise OrderProjectionConflictError("projection advanced")
            return await super().prepare_submission_in_session(session, **kwargs)

    context = replace(
        _runtime_context(),
        managed_positions=(_position(),),
        open_position_symbols=frozenset({"BTCUSDT"}),
    )

    async def provider(state):
        return context

    exchange = PlanAwareExchange()
    daemon = _daemon(
        exchange=exchange,
        context_provider=provider,
        repository=Repository(),
        exit_manager=_manager(),
        clock=lambda: NOW,
    )
    runtime = LiveExitChannelRuntime(
        daemon=daemon,
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
        closed_candle_expires_at=daemon.closed_candle_expires_at,
        clock=lambda: NOW,
    )

    async def source():
        yield _event()

    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(pending_signal.wait(), 1)
        assert attempted and exchange.plans == []
        old_id = attempted[0].client_order_id
        assert len({plan.client_order_id for plan in attempted}) == 1
        context = replace(
            context,
            managed_positions=(
                replace(
                    _position(), projection_version="pv2", quantity=Decimal("0.002")
                ),
            ),
        )
        runtime.note_account_facts_changed(("BTCUSDT",))
        await asyncio.wait_for(task, 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(exchange.plans) == 1
    plan = exchange.plans[0]
    assert plan.projection_version == "pv2"
    assert plan.quantity == Decimal("0.002")
    assert sum(
        allocation.allocated_quantity for allocation in plan.allocations
    ) == Decimal("0.002")
    assert plan.client_order_id != old_id


async def test_one_failed_candle_produces_one_error_event(monkeypatch):
    from crypto_momentum_lab.live_rollout import exit_channels

    errors = []
    monkeypatch.setattr(
        exit_channels.log, "error", lambda event, **details: errors.append(event)
    )

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            return "exit_submission_not_executed"

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
    )
    event = _event()
    key = (event.candle.symbol, event.candle.candle_start)
    runtime._pending_candles[key] = event
    await runtime._evaluate_candle(key)
    assert errors == ["live_closed_candle_exit_degraded"]
