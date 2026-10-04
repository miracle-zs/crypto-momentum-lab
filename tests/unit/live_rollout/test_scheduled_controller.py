from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
    ScheduledRiskWindowControllerConfig,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)


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
    assert calls == (
        ["wait"]
        if mode == "failure"
        else ["wait", "pending"]
    )
    assert result == (
        "scheduled_entry_submission_drain_failed:OSError" if mode == "failure" else None
    )
