from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout import exit_channels, exit_failure_policy
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
        managed_position_symbols = frozenset({"BTCUSDT"})

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
    assert exit_failure_policy.is_pending_position_sync_failure(
        "pending_live_positions:BTCUSDT"
    )
    assert (
        exit_failure_policy.promote_pending_position_failure("pending_live_positions:BTCUSDT")
        == "unmanaged_live_positions:BTCUSDT"
    )


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
