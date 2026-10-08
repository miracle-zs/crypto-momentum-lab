import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
    ScheduledRiskWindowControllerConfig,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)


def _idle_controller(clock) -> ScheduledRiskWindowController:
    return ScheduledRiskWindowController(
        config=ScheduledRiskWindowControllerConfig(
            run_id="test-run", scheduled_risk_window=ScheduledRiskWindowConfig()
        ),
        exit_manager=None,
        state_machine=None,
        context_provider=None,
        apply_context=None,
        invalidate_context_cache=lambda: None,
        process_exit_requests=None,
        set_entry_blocked=lambda blocked, **kwargs: None,
        pending_entry_plans=lambda: (),
        cancel_unfilled_entry_orders=None,
        fetch_exchange_positions=None,
        clock=clock,
        wait_for_entry_submissions_idle=AsyncMock(),
    )


@pytest.mark.parametrize(
    ("hour", "minute", "second", "verified", "expected"),
    [
        (23, 40, 0, False, 60.0),  # 07:40 local: bounded wall-clock recheck
        (23, 44, 59, False, 1.0),  # wake exactly at the entry stop
        (23, 45, 0, False, 1.0),  # flattening starts now, not tomorrow
        (23, 55, 0, False, 1.0),  # deadline retry cadence unchanged
        (23, 58, 0, False, 1.0),  # verification cadence unchanged
        (0, 59, 59, True, 1.0),  # do not defer reopening
        (1, 0, 0, False, 1.0),  # overdue positions still retry
        (1, 0, 0, True, 60.0),  # verified and reopened: idle
        (15, 59, 59, True, 1.0),  # wake at local midnight for daily reset
    ],
)
def test_poll_delay_respects_boundaries_and_unresolved_positions(
    hour, minute, second, verified, expected
) -> None:
    now = datetime(2026, 7, 4, hour, minute, second, tzinfo=UTC)
    controller = _idle_controller(lambda: now)
    schedule = controller._config.scheduled_risk_window
    controller._scheduled_window_day = schedule.localize(now).date()
    controller._scheduled_positions_verified = verified
    assert controller._next_poll_delay(now) == expected


def test_new_day_does_not_sleep_on_previous_verification() -> None:
    now = datetime(2026, 7, 4, 1, 0, tzinfo=UTC)
    controller = _idle_controller(lambda: now)
    controller._scheduled_window_day = now.date() - timedelta(days=1)
    controller._scheduled_positions_verified = True
    assert controller._next_poll_delay(now) == 1.0


@pytest.mark.asyncio
async def test_late_start_after_reopen_idles_without_flattening() -> None:
    now = datetime(2026, 7, 4, 2, 0, tzinfo=UTC)
    controller = _idle_controller(lambda: now)
    assert await controller.process() is None
    assert controller._scheduled_positions_verified
    assert controller._next_poll_delay(now) == 60.0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cancel_failed", OSError("unavailable")])
async def test_run_processes_before_sleep_and_keeps_error_retry_cadence(
    monkeypatch, failure
) -> None:
    now = datetime(2026, 7, 4, 23, 40, tzinfo=UTC)
    controller = _idle_controller(lambda: now)
    process = AsyncMock(
        return_value=failure if not isinstance(failure, Exception) else None,
        side_effect=failure if isinstance(failure, Exception) else None,
    )
    monkeypatch.setattr(controller, "process", process)

    async def sleep(delay):
        process.assert_awaited_once()
        assert delay == (60.0 if failure is None else 1.0)
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await controller.run()


@pytest.mark.asyncio
async def test_run_rechecks_clock_after_slow_action(monkeypatch) -> None:
    before = datetime(2026, 7, 4, 23, 44, 30, tzinfo=UTC)
    after = before + timedelta(seconds=29, microseconds=500_000)
    clock = Mock(side_effect=[before, after])
    controller = _idle_controller(clock)
    monkeypatch.setattr(controller, "process", AsyncMock(return_value=None))

    async def sleep(delay):
        assert delay == 0.5
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await controller.run()


@pytest.mark.asyncio
async def test_clock_jump_into_window_is_processed_on_next_idle_recheck(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 4, 23, 40, tzinfo=UTC)
    controller = _idle_controller(lambda: now)
    gate = Mock()
    controller._set_entry_blocked = gate
    controller._exit_manager = Mock()
    positions = AsyncMock(return_value=())
    controller._fetch_exchange_positions = positions
    delays = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        if len(delays) == 1:
            assert delay == 60.0
            # A forward wall-clock correction skips the exact boundary.
            now = datetime(2026, 7, 4, 23, 46, tzinfo=UTC)
            return
        assert delay == 1.0
        gate.assert_called_with(True, reason="scheduled_risk_window")
        positions.assert_awaited_once()
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await controller.run()
    assert delays == [60.0, 1.0]


def _market_state(
    *,
    symbol: str = "BTCUSDT",
    now: datetime,
) -> MarketState15s:
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=symbol,
        bucket_start=now - timedelta(seconds=15),
        bucket_end=now,
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("500"),
        aggressive_sell_notional=Decimal("500"),
        last_bid_price=Decimal("99.9"),
        last_ask_price=Decimal("100.1"),
        spread=Decimal("0.2"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=now,
        last_received_at=now,
    )


@pytest.mark.asyncio
async def test_flat_exchange_needs_no_market_state_or_stale_context() -> None:
    # 23:45 UTC is FLATTENING phase under default Asia/Shanghai schedule
    current_time = datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC)

    async def fetch_positions() -> tuple:
        return ()

    async def dummy_context(_state):
        pytest.fail("a flat exchange must not rebuild exits from stale local context")

    def dummy_publish(_context):
        pass

    class DummyExitManager:
        async def requests_for_scheduled_flatten(self, positions, *, state, context):
            return ()

    controller = ScheduledRiskWindowController(
        config=ScheduledRiskWindowControllerConfig(
            run_id="test-run",
            scheduled_risk_window=ScheduledRiskWindowConfig(),
        ),
        exit_manager=cast(object, DummyExitManager()),
        state_machine=None,
        context_provider=dummy_context,
        apply_context=dummy_publish,
        invalidate_context_cache=lambda: None,
        process_exit_requests=cast(object, None),
        set_entry_blocked=lambda _b, **_kw: None,
        pending_entry_plans=lambda: (),
        cancel_unfilled_entry_orders=cast(object, None),
        fetch_exchange_positions=fetch_positions,
        clock=lambda: current_time,
        startup_market_timeout_seconds=30.0,
        wait_for_entry_submissions_idle=AsyncMock(),
    )

    # No exchange exposure: startup market readiness is irrelevant to flattening.
    status = await controller.process()
    assert status is None

    # A later pass also completes without consulting stale local positions.
    current_time += timedelta(seconds=10)
    status = await controller.process()
    assert status is None


@pytest.mark.parametrize("mode", ["success", "failure"])
async def test_entry_drain_uses_explicit_waiter_before_plan_reads(mode: str) -> None:
    calls = []

    class Canceller:
        async def wait_for_entry_submissions_idle(self):
            pytest.fail("controller must not discover waiter methods")

    async def wait():
        calls.append("wait")
        if mode == "failure":
            raise OSError("unavailable")

    def pending():
        calls.append("pending")
        return ()

    controller = ScheduledRiskWindowController(
        config=ScheduledRiskWindowControllerConfig(
            run_id="test-run", scheduled_risk_window=None
        ),
        exit_manager=None,
        state_machine=Canceller(),
        context_provider=None,
        apply_context=None,
        invalidate_context_cache=lambda: None,
        process_exit_requests=None,
        set_entry_blocked=lambda blocked, **kwargs: None,
        pending_entry_plans=pending,
        cancel_unfilled_entry_orders=None,
        fetch_exchange_positions=None,
        clock=lambda: datetime.now(tz=UTC),
        wait_for_entry_submissions_idle=wait,
    )
    result = await controller._cancel_scheduled_entry_orders()
    assert calls == (["wait"] if mode == "failure" else ["wait", "pending"])
    assert result == (
        "scheduled_entry_submission_drain_failed:OSError" if mode == "failure" else None
    )
