from datetime import UTC, datetime
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


@pytest.mark.asyncio
async def test_closed_candle_channel_retries_pending_position_sync(
    monkeypatch,
) -> None:
    candle = SimpleNamespace(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 8, 4, tzinfo=UTC),
    )
    event = SimpleNamespace(candle=candle)
    failures: list[tuple[str, str | None]] = []
    sleeps: list[float] = []

    async def controlled_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(exit_channels.asyncio, "sleep", controlled_sleep)

    class QuoteCache:
        def for_symbols(self, _symbols):
            return ()

    class StateCache:
        def for_symbols(self, _symbols):
            return ()

    class Daemon:
        def __init__(self) -> None:
            self.calls = 0

        async def process_closed_candle(self, _event, *, latest_quote):
            del latest_quote
            self.calls += 1
            if self.calls == 1:
                return "pending_live_positions:BTCUSDT"
            return None

    class Source:
        def __aiter__(self):
            async def stream():
                yield event

            return stream()

    daemon = Daemon()
    runtime = LiveExitChannelRuntime(
        daemon=daemon,  # type: ignore[arg-type]
        latest_market_quotes=QuoteCache(),  # type: ignore[arg-type]
        latest_market_states=StateCache(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
        on_exit_failure=lambda symbol, failure: failures.append((symbol, failure)),
        pending_position_retry_delays=(0.25,),
    )

    await runtime.run_closed_candle_channel(source=Source())  # type: ignore[arg-type]

    assert daemon.calls == 2
    assert sleeps == [0.25]
    assert failures == []


def test_pending_position_failure_is_promoted_after_retries() -> None:
    assert exit_channels.is_pending_position_sync_failure(
        "pending_live_positions:BTCUSDT"
    )
    assert exit_channels.promote_pending_position_failure(
        "pending_live_positions:BTCUSDT"
    ) == "unmanaged_live_positions:BTCUSDT"
