from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_event_coordinator import (
    LiveExitEventCoordinator,
)
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from crypto_momentum_lab.live_rollout.exit_processor import LiveExitProcessor
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m
from tests.unit.shadow_operation.test_service import _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


class _Processor:
    def __init__(self, outcome: ExitLaneOutcome | None = None) -> None:
        self.outcome = outcome or ExitLaneOutcome()
        self.calls: list[tuple[object, ...]] = []

    async def process_state(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        self.calls.append(("state", state, context))
        return self.outcome

    async def process_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        self.calls.append(("quote", quote, state, context))
        return self.outcome

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        self.calls.append(
            ("candle", event, state, context, latest_quote),
        )
        return self.outcome

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        now: datetime,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        self.calls.append(
            ("grace", state, now, context, latest_quote),
        )
        return self.outcome


class _Lane:
    def __init__(self, outcome: ExitLaneOutcome | None = None) -> None:
        self.outcome = outcome or ExitLaneOutcome()
        self.start_calls = 0
        self.account_calls: list[tuple[object, object]] = []
        self.quote_calls: list[tuple[object, object, object]] = []

    async def start(self) -> None:
        self.start_calls += 1

    async def submit_account(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        self.account_calls.append((state, context))
        return self.outcome

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        self.quote_calls.append((quote, state, context))


def _coordinator(
    *,
    processor: _Processor,
    lane: _Lane,
    run_active: bool = False,
    context: LiveDaemonRuntimeContext | None = None,
) -> tuple[LiveExitEventCoordinator, dict[str, list[object]]]:
    events: dict[str, list[object]] = {
        "provider": [],
        "sync": [],
        "publish": [],
        "invalidate": [],
    }
    runtime_context = context or cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            pending_position_symbols=frozenset(),
            unmanaged_position_symbols=frozenset(),
        ),
    )

    async def provider(state: MarketState15s) -> LiveDaemonRuntimeContext:
        events["provider"].append(state)
        return runtime_context

    async def publish(context_value: LiveDaemonRuntimeContext) -> None:
        events["publish"].append(context_value)

    coordinator = LiveExitEventCoordinator(
        run_id="run-1",
        exit_manager=cast(LiveExitManager, object()),
        exit_enabled=lambda: True,
        run_active=lambda: run_active,
        context_provider=cast(LiveContextProvider, provider),
        sync_pending_entry_plans=lambda context_value: events["sync"].append(
            context_value
        ),
        publish_managed_position_symbols=publish,
        invalidate_context_cache=lambda: events["invalidate"].append(True),
        exit_processor=cast(LiveExitProcessor, processor),
        exit_lane=cast(ExitExecutionLane, lane),
    )
    return coordinator, events


def _quote(*, symbol: str = "BTCUSDT") -> RealtimeMarketQuote:
    return RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="live",
        symbol=symbol,
        event_at=NOW,
        received_at=NOW,
        bid_price=Decimal("29999"),
        ask_price=Decimal("30001"),
    )


@pytest.mark.asyncio
async def test_account_event_refreshes_context_and_uses_account_lane() -> None:
    processor = _Processor()
    lane = _Lane()
    coordinator, events = _coordinator(
        processor=processor,
        lane=lane,
        run_active=True,
    )
    state = cast(MarketState15s, _state())

    result = await coordinator.process_account_event(state)

    assert result is None
    assert len(events["provider"]) == 1
    assert len(events["sync"]) == 1
    assert len(events["publish"]) == 1
    assert len(events["invalidate"]) == 2
    assert lane.start_calls == 1
    assert len(lane.account_calls) == 1
    assert processor.calls == []


@pytest.mark.asyncio
async def test_quote_event_rejects_symbol_mismatch_before_loading_context() -> None:
    processor = _Processor()
    coordinator, events = _coordinator(
        processor=processor,
        lane=_Lane(),
    )

    result = await coordinator.process_market_quote(
        _quote(symbol="ETHUSDT"),
        cast(MarketState15s, _state()),
    )

    assert result is None
    assert events["provider"] == []
    assert processor.calls == []


@pytest.mark.asyncio
async def test_closed_candle_event_builds_synthetic_state_and_returns_failure() -> None:
    processor = _Processor(ExitLaneOutcome(failure="candle_failed"))
    coordinator, events = _coordinator(
        processor=processor,
        lane=_Lane(),
    )
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=NOW - timedelta(minutes=15),
        candle_end=NOW,
        open_price=Decimal("29900"),
        close_price=Decimal("30000"),
    )
    event = ClosedCandle15mEvent(
        candle=candle,
        exchange_event_at=NOW,
        received_at=NOW,
    )

    result = await coordinator.process_closed_candle(
        event,
        latest_quote=_quote(),
    )

    assert result == "candle_failed"
    assert len(events["provider"]) == 1
    kind, received_event, state_value, _context, latest_quote = (
        processor.calls[0]
    )
    state = cast(MarketState15s, state_value)
    assert kind == "candle"
    assert received_event == event
    assert state.symbol == "BTCUSDT"
    assert state.bucket_start == NOW - timedelta(seconds=15)
    assert latest_quote == _quote()


@pytest.mark.asyncio
async def test_grace_timeout_uses_processor_directly_when_lane_is_idle() -> None:
    processor = _Processor()
    coordinator, _events = _coordinator(
        processor=processor,
        lane=_Lane(),
    )
    state = replace(cast(MarketState15s, _state()), symbol="BTCUSDT")

    result = await coordinator.process_grace_timeout(
        state,
        now=NOW + timedelta(seconds=5),
        latest_quote=_quote(),
    )

    assert result is None
    kind, received_state, now, _context, latest_quote = processor.calls[0]
    assert kind == "grace"
    assert received_state == state
    assert now == NOW + timedelta(seconds=5)
    assert latest_quote == _quote()
