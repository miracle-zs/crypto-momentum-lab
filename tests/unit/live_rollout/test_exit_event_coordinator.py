import asyncio
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
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m
from tests.unit.live_rollout.test_postgres_runtime import _runtime_context
from tests.unit.shadow_operation.test_service import _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


@pytest.mark.parametrize("trigger", ["market", "quote"])
async def test_inflight_account_updates_defer_exit_without_poisoning_lane(trigger):
    provider = PostgresLiveContextProvider(
        session_factory=object(), account_label="primary", run_id="run",
        strategy_name="strategy", strategy_config_hash="config",
        git_commit_hash="commit", migration_revision="migration",
        lease_owner="owner", approval_id="approval",
    )
    changing = True
    reads = []

    async def load_once(state):
        context = replace(_runtime_context(), context_epoch=provider._cache_epoch)
        reads.append(context)
        if changing:
            provider.invalidate_account_snapshot()
        return context

    provider._load_context_once = load_once
    processor = _Processor()
    coordinator, events = _coordinator(processor=processor, lane=_Lane())
    coordinator._context_provider = provider._load_context
    outcomes = []
    lane = ExitExecutionLane(coordinator.process_market_work, coordinator.process_quote_work,
                             on_outcome=lambda symbol, outcome: outcomes.append(outcome))
    await lane.start()
    try:
        state = _state()
        if trigger == "quote":
            await lane.submit_quote(_quote(symbol=state.symbol), state)
        else:
            await lane.submit_market(state)
        await asyncio.wait_for(lane._idle.wait(), 1)
        assert len(reads) == 3
        assert processor.calls == []
        assert events["publish"] == []
        assert lane.failure is None
        assert not outcomes[-1].fatal_failure
        assert outcomes[-1].failure == f"pending_live_context:{state.symbol}"
        changing = False
        await lane.submit_market(state)
        await asyncio.wait_for(lane._idle.wait(), 1)
        assert len(processor.calls) == 1
        assert provider.is_current(processor.calls[0][-1])
        assert outcomes[-1].failure is None
    finally:
        await lane.stop()


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
        self.market_calls: list[tuple[object, object]] = []
        self.quote_calls: list[tuple[object, object, object]] = []

    async def start(self) -> None:
        self.start_calls += 1

    async def submit_market(
        self,
        state: MarketState15s,
    ) -> None:
        self.market_calls.append(state)

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> None:
        self.quote_calls.append((quote, state))


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

    def publish(context_value: LiveDaemonRuntimeContext) -> None:
        events["sync"].append(context_value)
        events["publish"].append(context_value)

    coordinator = LiveExitEventCoordinator(
        run_id="run-1",
        exit_enabled=lambda: True,
        run_active=lambda: run_active,
        context_provider=cast(LiveContextProvider, provider),
        apply_context=publish,
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
async def test_account_event_defers_context_to_existing_market_lane() -> None:
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
    assert events["provider"] == []
    assert events["sync"] == []
    assert events["publish"] == []
    assert len(events["invalidate"]) == 1
    assert lane.start_calls == 1
    assert len(lane.market_calls) == 1
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
    kind, received_event, state_value, _context, latest_quote = processor.calls[0]
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


@pytest.mark.parametrize("trigger", ["account", "quote", "candle", "grace"])
async def test_disabled_exit_coordinator_skips_all_collaborators(trigger: str) -> None:
    coordinator = LiveExitEventCoordinator(
        run_id="run-1",
        exit_enabled=lambda: False,
        run_active=lambda: pytest.fail("must not read run state"),
        context_provider=cast(LiveContextProvider, object()),
        apply_context=object(),
        invalidate_context_cache=lambda: pytest.fail("must not invalidate"),
        exit_processor=cast(LiveExitProcessor, object()),
        exit_lane=cast(ExitExecutionLane, object()),
    )
    if trigger == "account":
        result = await coordinator.process_account_event(_state())
    elif trigger == "quote":
        result = await coordinator.process_market_quote(_quote(), _state())
    elif trigger == "candle":
        result = await coordinator.process_closed_candle(
            cast(ClosedCandle15mEvent, object())
        )
    else:
        result = await coordinator.process_grace_timeout(_state(), now=NOW)
    assert result is None


@pytest.mark.asyncio
async def test_account_quote_uses_existing_quote_lane_without_inline_processing():
    processor = _Processor()
    lane = _Lane()
    coordinator, events = _coordinator(processor=processor, lane=lane, run_active=True)
    state = cast(MarketState15s, _state())
    quote = _quote()
    assert await coordinator.process_account_event(state, quote=quote) is None
    assert len(lane.quote_calls) == 1
    assert processor.calls == []
    assert len(events["invalidate"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("with_quote", [False, True])
async def test_account_consumer_publishes_next_snapshot_while_exit_work_is_waiting(
    with_quote,
):
    from crypto_momentum_lab.execution_account.hub import AccountEvent
    from crypto_momentum_lab.live_rollout.account_channel import LiveAccountEventRuntime

    started = asyncio.Event()
    release = asyncio.Event()
    snapshots = []
    outcomes = []
    state = cast(MarketState15s, _state())
    quote = _quote()
    context = cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            pending_position_symbols=frozenset(),
            unmanaged_position_symbols=frozenset(),
        ),
    )

    class Processor(_Processor):
        async def process_state(self, state, context):
            started.set()
            await release.wait()
            return ExitLaneOutcome(failure="exit_recovery_failed")

        async def process_quote(self, quote, state, context):
            return await self.process_state(state, context)

    processor = Processor()
    lane = ExitExecutionLane(
        lambda state: coordinator.process_market_work(state),
        lambda quote, state: coordinator.process_quote_work(quote, state),
        on_outcome=lambda symbol, outcome: outcomes.append((symbol, outcome.failure)),
    )
    coordinator, _events = _coordinator(
        processor=processor, lane=lane, run_active=True, context=context
    )
    first = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ACCOUNT_UPDATE",
        event_id="first",
        event_at=NOW,
        received_at=NOW,
        symbols=(state.symbol,),
    )

    class Source:
        def __aiter__(self):
            async def events():
                yield first
                # The first exit is actually waiting, rather than merely queued.
                await asyncio.wait_for(started.wait(), timeout=1)
                yield replace(first, event_id="second")

            return events()

    runtime = LiveAccountEventRuntime(
        daemon=coordinator,
        latest_market_states=SimpleNamespace(for_symbols=lambda _symbols: (state,)),
        latest_market_quotes=SimpleNamespace(
            for_symbols=lambda _symbols: (quote,) if with_quote else ()
        ),
        is_transient_error=lambda _error: False,
        on_account_snapshot=lambda event: snapshots.append(event.event_id),
        on_exit_failure=lambda _symbol, _failure: pytest.fail(
            "queue admission is not an exit outcome"
        ),
    )
    try:
        await asyncio.wait_for(runtime.run(Source()), timeout=1)
        assert snapshots == ["first", "second"]
        assert outcomes == []
    finally:
        release.set()
        await lane.stop()
    assert outcomes
    assert all(failure == "exit_recovery_failed" for _symbol, failure in outcomes)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_quote", [False, True])
async def test_context_preparation_does_not_block_account_facts(
    with_quote
):
    from crypto_momentum_lab.execution_account.hub import AccountEvent
    from crypto_momentum_lab.live_rollout.account_channel import LiveAccountEventRuntime

    started = asyncio.Event()
    release = asyncio.Event()
    snapshots = []
    current = SimpleNamespace(
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
        version=0,
    )
    processor = _Processor()
    state = cast(MarketState15s, _state())
    quote = _quote()

    async def provider(_state):
        started.set()
        await release.wait()
        return current

    def publish(_context):
        pass

    lane = ExitExecutionLane(
        lambda state: coordinator.process_market_work(state),
        lambda quote, state: coordinator.process_quote_work(quote, state),
    )
    coordinator = LiveExitEventCoordinator(
        run_id="run-1",
        exit_enabled=lambda: True,
        run_active=lambda: True,
        context_provider=provider,
        apply_context=publish,
        invalidate_context_cache=lambda: None,
        exit_processor=processor,
        exit_lane=lane,
    )
    first = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ACCOUNT_UPDATE",
        event_id="first",
        event_at=NOW,
        received_at=NOW,
        symbols=(state.symbol,),
    )

    class Source:
        def __aiter__(self):
            async def events():
                yield first
                await asyncio.wait_for(started.wait(), timeout=1)
                yield replace(first, event_id="second")

            return events()

    def snapshot(event):
        nonlocal current
        snapshots.append(event.event_id)
        current = SimpleNamespace(
            pending_position_symbols=frozenset(),
            unmanaged_position_symbols=frozenset(),
            version=len(snapshots),
        )

    runtime = LiveAccountEventRuntime(
        daemon=coordinator,
        latest_market_states=SimpleNamespace(for_symbols=lambda _symbols: (state,)),
        latest_market_quotes=SimpleNamespace(
            for_symbols=lambda _symbols: (quote,) if with_quote else ()
        ),
        is_transient_error=lambda _error: False,
        on_account_snapshot=snapshot,
    )
    try:
        await asyncio.wait_for(runtime.run(Source()), timeout=1)
        assert snapshots == ["first", "second"]
        assert processor.calls == []
    finally:
        release.set()
        await asyncio.wait_for(lane.stop(), timeout=1)
    # Resolve the context after the wait, rather than retaining the view at enqueue.
    assert all(call[-1].version == 2 for call in processor.calls)


@pytest.mark.asyncio
async def test_pending_position_is_published_and_blocks_background_exit_decision():
    processor = _Processor()
    pending = cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            pending_position_symbols=frozenset({"BTCUSDT"}),
            unmanaged_position_symbols=frozenset(),
        ),
    )
    coordinator, events = _coordinator(
        processor=processor, lane=_Lane(), run_active=True, context=pending
    )
    state = cast(MarketState15s, _state())
    assert await coordinator.process_account_event(state) is None
    assert events["provider"] == []
    result = await coordinator.process_market_work(state)
    assert result.failure == "pending_live_positions:BTCUSDT"
    assert events["publish"] == [pending]
    assert processor.calls == []


async def test_unrelated_context_runtime_error_remains_fatal():
    coordinator, events = _coordinator(processor=_Processor(), lane=_Lane())

    async def load(state):
        raise RuntimeError("database failure")

    coordinator._context_provider = load
    lane = ExitExecutionLane(coordinator.process_market_work, coordinator.process_quote_work)
    await lane.start()
    await lane.submit_market(_state())
    outcome = await asyncio.wait_for(lane.stop(), 1)
    assert outcome.fatal_failure
    assert outcome.failure == "exit_execution_failed:RuntimeError"
    assert lane.failure == outcome.failure
    assert events["publish"] == []
