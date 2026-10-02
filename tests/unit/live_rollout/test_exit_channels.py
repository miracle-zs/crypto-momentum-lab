import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout import exit_channels
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime


@pytest.mark.asyncio
async def test_quote_channel_updates_cache_and_reports_success() -> None:
    quote = SimpleNamespace(symbol="BTCUSDT")
    state = SimpleNamespace(symbol="BTCUSDT")
    observed: list[object] = []
    processed: list[tuple[object, object]] = []
    outcomes: list[tuple[str, str | None]] = []

    class QuoteCache:
        def observe(self, value) -> None:
            observed.append(value)

        def for_symbols(self, _symbols):
            return ()

    class StateCache:
        def for_symbols(self, _symbols):
            return (state,)

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_market_quote(self, value, market_state):
            processed.append((value, market_state))
            return None

    class Source:
        def __aiter__(self):
            async def stream():
                yield quote

            return stream()

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),  # type: ignore[arg-type]
        latest_market_quotes=QuoteCache(),  # type: ignore[arg-type]
        latest_market_states=StateCache(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
        on_exit_failure=lambda symbol, failure: outcomes.append((symbol, failure)),
    )

    await runtime.run_quote_channel(source=Source())  # type: ignore[arg-type]

    assert observed == [quote]
    assert processed == [(quote, state)]
    assert outcomes == [("BTCUSDT", None)]


async def test_pending_candles_wait_for_facts_without_blocking_other_symbols(monkeypatch):
    events = [SimpleNamespace(candle=SimpleNamespace(
        symbol=symbol, candle_start=datetime(2026, 8, 4, tzinfo=UTC) + timedelta(minutes=15*i)
    )) for i, symbol in enumerate(("BTCUSDT", "BTCUSDT", "ETHUSDT"))]
    calls = []
    outcomes = []
    ready = False
    consumed = asyncio.Event()
    quote = SimpleNamespace(price=100)

    async def forbidden_sleep(delay):
        pytest.fail("pending positions must wait for facts, not sleep")

    monkeypatch.setattr(exit_channels.asyncio, "sleep", forbidden_sleep)

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            calls.append((event, latest_quote))
            if event.candle.symbol == "ETHUSDT":
                consumed.set()
                return None
            return None if ready else "pending_live_positions:BTCUSDT"

    async def source():
        for event in events:
            yield event
        yield events[0]  # A duplicate pending event must not trigger reevaluation.
        yield events[2]  # A duplicate confirmed event must not submit another exit.

    cache = SimpleNamespace(for_symbols=lambda symbols: (quote,))
    runtime = LiveExitChannelRuntime(
        daemon=Daemon(), latest_market_quotes=cache, latest_market_states=cache,
        is_transient_error=lambda error: False,
        on_exit_failure=lambda symbol, failure: outcomes.append((symbol, failure)),
    )
    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(consumed.wait(), 1)
        assert [e for e, q in calls] == events
        assert outcomes == [("ETHUSDT", None)]
        assert len(runtime._pending_candles) == 2
        ready = True
        quote = SimpleNamespace(price=101)
        runtime.note_account_facts_changed()
        runtime.note_account_facts_changed()
        await asyncio.wait_for(task, 1)
        assert [e for e, q in calls] == events + events[:2]
        assert all(q.price == 101 for e, q in calls[3:])
        assert not runtime._pending_candles
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("pending_reason", ["pending_live_positions:BTCUSDT", "pending_exit_order_recovery:BTCUSDT", "pending_live_context:BTCUSDT"])
async def test_candle_notification_during_evaluation_is_retained(pending_reason):
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    event = SimpleNamespace(candle=SimpleNamespace(
        symbol="BTCUSDT", candle_start=datetime(2026, 8, 4, tzinfo=UTC)))

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
                return pending_reason
            return None

    async def source():
        yield event

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(), latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(), is_transient_error=lambda error: False,
    )
    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(started.wait(), 1)
        runtime.note_account_facts_changed()
        release.set()
        await asyncio.wait_for(task, 1)
        assert calls == 2
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_pending_candle_survives_cancellation_and_restarts():
    event = SimpleNamespace(candle=SimpleNamespace(
        symbol="BTCUSDT", candle_start=datetime(2026, 8, 4, tzinfo=UTC)))
    seen = asyncio.Event()
    ready = False

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            seen.set()
            return None if ready else "pending_live_positions:BTCUSDT"

    async def source():
        yield event

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(), latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(), is_transient_error=lambda error: False,
    )
    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    await asyncio.wait_for(seen.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(runtime._pending_candles) == 1
    ready = True
    runtime.note_account_facts_changed()
    await asyncio.wait_for(runtime.run_closed_candle_channel(source=source()), 1)
    assert not runtime._pending_candles


@pytest.mark.parametrize("notify", [False, True])
async def test_quote_conflict_uses_only_injected_notifier(notify: bool) -> None:
    quote = SimpleNamespace(symbol="BTCUSDT")
    state = SimpleNamespace(symbol="BTCUSDT")
    calls = []

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_market_quote(self, quote, state):
            raise ValueError("identity conflict")

        def note_order_identity_conflict(self, symbol):
            pytest.fail("runtime must not discover the daemon notification method")

    class Source:
        def __aiter__(self):
            async def stream():
                yield quote

            return stream()

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(observe=lambda q: None),
        latest_market_states=SimpleNamespace(for_symbols=lambda symbols: (state,)),
        is_transient_error=lambda error: False,
        is_order_identity_conflict=lambda error: True,
        on_order_identity_conflict=(
            (lambda symbol: calls.append(("identity", symbol))) if notify else None
        ),
        on_exit_failure=lambda symbol, failure: calls.append(("failure", symbol)),
    )
    await runtime.run_quote_channel(source=Source())
    assert calls == (
        [("identity", "BTCUSDT"), ("failure", "BTCUSDT")]
        if notify
        else [("failure", "BTCUSDT")]
    )


async def test_quote_channel_requires_managed_position_symbols() -> None:
    class Source:
        def __aiter__(self):
            async def stream():
                yield SimpleNamespace(symbol="BTCUSDT")

            return stream()

    runtime = LiveExitChannelRuntime(
        daemon=SimpleNamespace(),
        latest_market_quotes=SimpleNamespace(
            observe=lambda quote: pytest.fail("must reject before cache mutation")
        ),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
    )
    with pytest.raises(AttributeError, match="managed_position_symbols"):
        await runtime.run_quote_channel(source=Source())


async def test_candle_conflict_stays_pending_until_success():
    event = SimpleNamespace(candle=SimpleNamespace(
        symbol="BTCUSDT", candle_start=datetime(2026, 8, 4, tzinfo=UTC)))
    calls = 0
    failures = []
    reported = asyncio.Event()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("identity conflict")
            return None

    def failure(symbol, reason):
        failures.append(reason)
        reported.set()

    async def source():
        yield event

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(), latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(), is_transient_error=lambda error: False,
        is_order_identity_conflict=lambda error: True, on_exit_failure=failure,
    )
    task = asyncio.create_task(runtime.run_closed_candle_channel(source=source()))
    try:
        await asyncio.wait_for(reported.wait(), 1)
        assert failures == ["order_identity_conflict"]
        assert len(runtime._pending_candles) == 1
        runtime.note_account_facts_changed()
        await asyncio.wait_for(task, 1)
        assert failures == ["order_identity_conflict", None]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("conflicts", [1, 5])
async def test_stale_context_retry_is_bounded_and_retains_original_candle(
    conflicts,
):
    event = SimpleNamespace(
        candle=SimpleNamespace(
            symbol="BTCUSDT", candle_start=datetime(2026, 8, 4, tzinfo=UTC)
        )
    )
    calls = 0

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            nonlocal calls
            calls += 1
            return (
                "pending_live_context:BTCUSDT" if calls <= conflicts else None
            )

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
    )
    key = (event.candle.symbol, event.candle.candle_start)
    runtime._pending_candles[key] = event
    await runtime._evaluate_candle(key)
    assert calls == min(conflicts + 1, 3)
    assert (key in runtime._pending_candles) is (conflicts >= 3)
    if conflicts >= 3:
        assert runtime._pending_candles[key] is event
        await runtime._evaluate_candle(key)
    assert key not in runtime._pending_candles


@pytest.mark.parametrize("channel", ["quote", "grace"])
async def test_context_refresh_wait_does_not_fault_or_back_off(channel):
    state = SimpleNamespace(symbol="BTCUSDT")
    quote = SimpleNamespace(symbol="BTCUSDT")
    calls = []
    outcomes = []
    finished = asyncio.Event()

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_market_quote(self, quote, state):
            return await self.evaluate()

        async def process_grace_timeout(self, state, **kwargs):
            return await self.evaluate()

        async def evaluate(self):
            calls.append(1)
            return "pending_live_context:BTCUSDT" if len(calls) == 1 else None

    def publish(symbol, failure):
        outcomes.append((symbol, failure))
        finished.set()

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(observe=lambda quote: None,
                                            for_symbols=lambda symbols: (quote,)),
        latest_market_states=SimpleNamespace(for_symbols=lambda symbols: (state,)),
        is_transient_error=lambda error: False, on_exit_failure=publish,
    )

    async def source():
        yield quote
        assert outcomes == []  # A pending read neither sets nor clears a real fault.
        yield quote

    if channel == "quote":
        await runtime.run_quote_channel(source=source())
    else:
        task = asyncio.create_task(runtime.run_grace_timeout_channel(interval_seconds=0.001))
        try:
            await asyncio.wait_for(finished.wait(), 0.5)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert len(calls) == 2
    assert outcomes == [("BTCUSDT", None)]


async def test_quotes_keep_cache_current_during_exit_backoff():
    quotes = [SimpleNamespace(symbol="BTCUSDT", price=price) for price in (100, 101, 102)]
    observed = []
    evaluated = []
    failures = []

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_market_quote(self, quote, state):
            evaluated.append(quote)
            return "exit_submission_failed"

    async def source():
        for quote in quotes:
            yield quote

    runtime = LiveExitChannelRuntime(daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(observe=observed.append),
        latest_market_states=SimpleNamespace(for_symbols=lambda symbols: (SimpleNamespace(symbol="BTCUSDT"),)),
        is_transient_error=lambda error: False,
        on_exit_failure=lambda symbol, failure: failures.append((symbol, failure)))
    await runtime.run_quote_channel(source=source())
    assert observed == quotes
    assert evaluated == quotes[:1]
    assert failures == [("BTCUSDT", "exit_submission_failed")]
