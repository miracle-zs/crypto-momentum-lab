from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
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
async def test_scheduled_controller_reports_pending_during_startup_market_wait(
) -> None:
    # 23:45 UTC is FLATTENING phase under default Asia/Shanghai schedule
    current_time = datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC)

    async def fetch_positions() -> tuple:
        return ()

    async def dummy_context(_state):
        return cast(
            LiveDaemonRuntimeContext,
            SimpleNamespace(
                pending_position_symbols=frozenset(),
                managed_positions=(),
                unmanaged_position_symbols=frozenset(),
            ),
        )

    async def dummy_publish(_context):
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
        sync_pending_entry_plans=lambda _: None,
        publish_managed_position_symbols=dummy_publish,
        invalidate_context_cache=lambda: None,
        process_exit_requests=cast(object, None),
        set_entry_blocked=lambda _b, **_kw: None,
        pending_entry_plans=lambda: (),
        cancel_unfilled_entry_orders=cast(object, None),
        fetch_exchange_positions=fetch_positions,
        clock=lambda: current_time,
        startup_market_timeout_seconds=30.0,
    )

    # Within startup period (t = 0), market states have not arrived yet
    status = await controller.process()
    assert status == "scheduled_flatten_market_state_pending"

    # Within startup period (t = 10s), still waiting for market states
    current_time += timedelta(seconds=10)
    status = await controller.process()
    assert status == "scheduled_flatten_market_state_pending"


@pytest.mark.asyncio
async def test_scheduled_controller_reports_error_when_market_wait_times_out() -> None:
    current_time = datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC)

    async def fetch_positions() -> tuple:
        return ()

    async def dummy_context(_state):
        return cast(
            LiveDaemonRuntimeContext,
            SimpleNamespace(
                pending_position_symbols=frozenset(),
                managed_positions=(),
                unmanaged_position_symbols=frozenset(),
            ),
        )

    async def dummy_publish(_context):
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
        sync_pending_entry_plans=lambda _: None,
        publish_managed_position_symbols=dummy_publish,
        invalidate_context_cache=lambda: None,
        process_exit_requests=cast(object, None),
        set_entry_blocked=lambda _b, **_kw: None,
        pending_entry_plans=lambda: (),
        cancel_unfilled_entry_orders=cast(object, None),
        fetch_exchange_positions=fetch_positions,
        clock=lambda: current_time,
        startup_market_timeout_seconds=30.0,
    )

    # Advance time past the 30.0s startup grace window without observing market state
    current_time += timedelta(seconds=35)
    status = await controller.process()
    assert status == "scheduled_flatten_market_state_unavailable"


@pytest.mark.asyncio
async def test_scheduled_controller_completes_flatten_after_market_state_arrives(
) -> None:
    current_time = datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC)

    async def fetch_positions() -> tuple:
        return ()

    async def dummy_context(_state):
        return cast(
            LiveDaemonRuntimeContext,
            SimpleNamespace(
                pending_position_symbols=frozenset(),
                managed_positions=(),
                unmanaged_position_symbols=frozenset(),
            ),
        )

    async def dummy_publish(_context):
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
        sync_pending_entry_plans=lambda _: None,
        publish_managed_position_symbols=dummy_publish,
        invalidate_context_cache=lambda: None,
        process_exit_requests=cast(object, None),
        set_entry_blocked=lambda _b, **_kw: None,
        pending_entry_plans=lambda: (),
        cancel_unfilled_entry_orders=cast(object, None),
        fetch_exchange_positions=fetch_positions,
        clock=lambda: current_time,
        startup_market_timeout_seconds=30.0,
    )

    # Initial tick is pending
    status = await controller.process()
    assert status == "scheduled_flatten_market_state_pending"

    # Market state arrives at t = 5s
    current_time += timedelta(seconds=5)
    state = _market_state(symbol="BTCUSDT", now=current_time)
    controller.observe_state(state)

    # Now flatten should process and complete cleanly
    status = await controller.process()
    assert status is None
