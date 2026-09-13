import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.account_channel import (
    LiveAccountEventRuntime,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_runtime_reconciles_order_before_publishing_snapshot() -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=False,
        received_at=NOW,
        symbols=(),
    )
    ordering: list[str] = []

    class Reconciliation:
        run_id = "run-1"

        async def reconcile_account_event(self, _event: object) -> None:
            ordering.append("reconcile")

    class Source:
        def __aiter__(self) -> AsyncIterator[object]:
            async def stream() -> AsyncIterator[object]:
                yield event

            return stream()

    class EmptyCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return ()

    runtime = LiveAccountEventRuntime(
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        order_reconciliation=Reconciliation(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
        on_account_snapshot=lambda _event: ordering.append("snapshot"),
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert ordering == ["reconcile", "snapshot"]


@pytest.mark.asyncio
async def test_runtime_retries_pending_position_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=False,
        received_at=NOW,
        symbols=("BTCUSDT",),
    )
    state = SimpleNamespace(symbol="BTCUSDT")
    delays: list[float] = []
    failures: list[tuple[str, str | None]] = []

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

    class Daemon:
        def __init__(self) -> None:
            self.calls = 0

        async def process_account_event(
            self,
            _state: object,
            *,
            quote: object,
        ) -> str | None:
            del quote
            self.calls += 1
            if self.calls == 1:
                return "pending_live_positions:BTCUSDT"
            return None

    class Source:
        def __aiter__(self) -> AsyncIterator[object]:
            async def stream() -> AsyncIterator[object]:
                yield event

            return stream()

    class StateCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return (state,)

    class QuoteCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return ()

    daemon = Daemon()
    runtime = LiveAccountEventRuntime(
        daemon=daemon,  # type: ignore[arg-type]
        latest_market_states=StateCache(),  # type: ignore[arg-type]
        latest_market_quotes=QuoteCache(),  # type: ignore[arg-type]
        run_id="run-1",
        is_transient_error=lambda _error: False,
        on_exit_failure=lambda symbol, failure: failures.append(
            (symbol, failure)
        ),
        pending_position_retry_delays=(0.25,),
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert daemon.calls == 2
    assert delays == [0.25]
    assert failures == [("BTCUSDT", None)]


@pytest.mark.asyncio
async def test_runtime_invalidates_account_snapshot_before_non_transient_crash(
) -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=False,
        received_at=NOW,
        symbols=("BTCUSDT",),
    )
    recovery_reasons: list[str] = []

    class Reconciliation:
        run_id = "run-1"

        async def reconcile_account_event(self, _event: object) -> None:
            raise RuntimeError("order journal unavailable")

    class Source:
        def __aiter__(self) -> AsyncIterator[object]:
            async def stream() -> AsyncIterator[object]:
                yield event

            return stream()

    class EmptyCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return ()

    runtime = LiveAccountEventRuntime(
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        order_reconciliation=Reconciliation(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
        on_account_snapshot_recovery=recovery_reasons.append,
    )

    with pytest.raises(RuntimeError, match="order journal unavailable"):
        await runtime.run(Source())  # type: ignore[arg-type]

    assert recovery_reasons == [
        "account_event_processing_failed:RuntimeError",
    ]


@pytest.mark.asyncio
async def test_runtime_deduplicates_fill_telemetry_after_account_stream_replay(
) -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=True,
        trade_id="trade-1",
        symbol="BTCUSDT",
        received_at=NOW,
        symbols=(),
    )
    fill_events: list[object] = []

    class Telemetry:
        async def account_fill(self, received_event: object, *, occurred_at) -> None:
            del occurred_at
            fill_events.append(received_event)

    class Source:
        def __aiter__(self) -> AsyncIterator[object]:
            async def stream() -> AsyncIterator[object]:
                yield event
                yield event

            return stream()

    class EmptyCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return ()

    runtime = LiveAccountEventRuntime(
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        telemetry=Telemetry(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert fill_events == [event]
