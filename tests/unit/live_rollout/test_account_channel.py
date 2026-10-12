import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.live_rollout.account_channel import (
    LiveAccountEventRuntime,
)

pytestmark = pytest.mark.transition

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_telemetry_outage_cannot_discard_account_trade_facts():
    ordering = []
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=True,
        trade_id="trade-1",
        symbol="BTCUSDT",
        received_at=NOW,
        symbols=(),
    )

    class Telemetry:
        async def account_fill(self, event, *, occurred_at):
            ordering.append("telemetry")
            raise ConnectionError("telemetry unavailable")

    class Cache:
        def for_symbols(self, symbols):
            return ()

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=object(),
        latest_market_states=Cache(),
        latest_market_quotes=Cache(),
        telemetry=Telemetry(),
        on_account_snapshot=AsyncMock(
            side_effect=lambda event: ordering.append("durable-facts")
        ),
        is_transient_error=lambda error: isinstance(error, ConnectionError),
    )
    await runtime._process_event(event)
    assert ordering == ["durable-facts", "telemetry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failures", [2, 3])
async def test_runtime_retains_event_until_order_and_book_facts_are_applied(
    failures: int,
) -> None:
    events = tuple(
        SimpleNamespace(
            event_type="ORDER_TRADE_UPDATE",
            client_order_id=f"entry-{i}",
            has_fill=False,
            received_at=NOW,
            symbols=(),
        )
        for i in (1, 2)
    )
    ordering: list[tuple[str, str]] = []
    calls = 0

    class Reconciliation:
        run_id = "run-1"

        async def reconcile_account_event(self, event: object) -> None:
            nonlocal calls
            calls += 1
            ordering.append(("reconcile", event.client_order_id))
            if calls <= failures:
                raise ConnectionError("temporary persistence failure")

    class Source:
        index = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.index == len(events):
                raise StopAsyncIteration
            event = events[self.index]
            self.index += 1
            return event

    class EmptyCache:
        def for_symbols(self, _symbols):
            return ()

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=object(),
        latest_market_states=EmptyCache(),
        latest_market_quotes=EmptyCache(),
        order_reconciliation=Reconciliation(),
        is_transient_error=lambda error: isinstance(error, ConnectionError),
        on_account_snapshot=AsyncMock(side_effect=lambda event: ordering.append(
            ("book", event.client_order_id)
        )),
    )
    if failures == 3:
        with pytest.raises(ConnectionError, match="temporary persistence failure"):
            await runtime.run(Source())
        assert ordering == [("reconcile", "entry-1")] * 3
        return
    await runtime.run(Source())
    assert ordering == [
        ("reconcile", "entry-1"),
        ("reconcile", "entry-1"),
        ("reconcile", "entry-1"),
        ("book", "entry-1"),
        ("reconcile", "entry-2"),
        ("book", "entry-2"),
    ]


@pytest.mark.asyncio
async def test_transient_order_failure_requests_account_fact_recovery() -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=False,
        received_at=NOW,
        symbols=(),
    )
    recoveries: list[str] = []
    published: list[object] = []

    class Reconciliation:
        run_id = "run-1"

        async def reconcile_account_event(self, _event: object) -> None:
            raise ConnectionError("order persistence temporarily unavailable")

    class EmptyCache:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return ()

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        order_reconciliation=Reconciliation(),  # type: ignore[arg-type]
        is_transient_error=lambda error: isinstance(error, ConnectionError),
        on_account_snapshot=AsyncMock(side_effect=published.append),
        on_account_snapshot_recovery=recoveries.append,
    )

    await runtime._process_event(event)  # type: ignore[arg-type]

    assert published == []
    assert recoveries == ["account_event_processing_failed:ConnectionError"]


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
        run_id="run-1",
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        order_reconciliation=Reconciliation(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
        on_account_snapshot=AsyncMock(
            side_effect=lambda _event: ordering.append("snapshot")
        ),
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert ordering == ["reconcile", "snapshot"]


@pytest.mark.asyncio
async def test_pending_position_does_not_delay_next_account_snapshot(
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
    snapshots: list[int] = []
    failures: list[tuple[str, str | None]] = []

    async def controlled_sleep(delay: float) -> None:
        pytest.fail(f"account consumer slept for {delay}")

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
            assert len(snapshots) == self.calls + 1
            self.calls += 1
            if self.calls == 1:
                return "pending_live_positions:BTCUSDT"
            return None

    class Source:
        def __aiter__(self) -> AsyncIterator[object]:
            async def stream() -> AsyncIterator[object]:
                yield event
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
        on_exit_failure=lambda symbol, failure: failures.append((symbol, failure)),
        on_account_snapshot=AsyncMock(
            side_effect=lambda _event: snapshots.append(len(snapshots))
        ),
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert daemon.calls == 2
    assert snapshots == [0, 1]
    assert failures == []


@pytest.mark.asyncio
async def test_runtime_invalidates_account_snapshot_before_non_transient_crash() -> (
    None
):
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
        run_id="run-1",
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
async def test_runtime_deduplicates_fill_telemetry_after_account_stream_replay() -> (
    None
):
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
        run_id="run-1",
        daemon=object(),  # type: ignore[arg-type]
        latest_market_states=EmptyCache(),  # type: ignore[arg-type]
        latest_market_quotes=EmptyCache(),  # type: ignore[arg-type]
        telemetry=Telemetry(),  # type: ignore[arg-type]
        is_transient_error=lambda _error: False,
    )

    await runtime.run(Source())  # type: ignore[arg-type]

    assert fill_events == [event]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fail_snapshot,transient", [(False, False), (True, False), (True, True)]
)
async def test_async_snapshot_is_applied_before_account_decision(
    fail_snapshot: bool,
    transient: bool,
) -> None:
    ordering: list[str] = []
    recoveries: list[str] = []
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=False,
        symbols=("BTCUSDT",),
    )
    state = SimpleNamespace(symbol="BTCUSDT")

    class Cache:
        def for_symbols(self, symbols):
            return (state,) if symbols else ()

    class Quotes:
        def for_symbols(self, symbols):
            return ()

    class Daemon:
        async def process_account_event(self, state, *, quote):
            ordering.append("decision")
            return None

    async def snapshot(event):
        await asyncio.sleep(0)
        ordering.append("snapshot")
        if fail_snapshot:
            raise ValueError("snapshot invalid")

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=Daemon(),
        latest_market_states=Cache(),
        latest_market_quotes=Quotes(),
        is_transient_error=lambda error: transient,
        on_account_snapshot=AsyncMock(side_effect=snapshot),
        on_account_snapshot_recovery=recoveries.append,
    )
    if fail_snapshot:
        if transient:
            await runtime._process_event(event)
        else:
            with pytest.raises(ValueError, match="snapshot invalid"):
                await runtime._process_event(event)
        assert ordering == ["snapshot"]
        assert recoveries == ["account_event_processing_failed:ValueError"]
    else:
        await runtime._process_event(event)
        assert ordering == ["snapshot", "decision"]


@pytest.mark.asyncio
async def test_blocked_exit_repair_does_not_block_account_publication():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.live_rollout.order_reconciliation import (
        LiveOrderReconciliation,
    )

    started = asyncio.Event()
    published: list[int] = []

    async def recover():
        started.set()
        await asyncio.Event().wait()

    repair = LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=())
        ),
        state_machine=SimpleNamespace(),
        run_id="run",
        interval_seconds=3600,
        recover_exits=recover,
    )

    class Cache:
        def for_symbols(self, symbols):
            return ()

    async def publish(event):
        # Mirrors the production callback after its durable fact commit.
        published.append(event.sequence)
        repair.request_recovery()

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=object(),
        latest_market_states=Cache(),
        latest_market_quotes=Cache(),
        order_reconciliation=repair,
        is_transient_error=lambda error: False,
        on_account_snapshot=AsyncMock(side_effect=publish),
    )
    worker = asyncio.create_task(repair.run_requested())
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=False,
        symbols=(),
        sequence=1,
    )
    try:
        await runtime._process_event(event)
        await asyncio.wait_for(started.wait(), 1)
        event.sequence = 2
        await asyncio.wait_for(runtime._process_event(event), 1)
        assert published == [1, 2]
        assert not worker.done()
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_a4c_account_runtime_hedge_close_and_fill_processing():
    """A4c: Real account runtime processes A4a order update and trade facts,
    then zero position arrives; no identity conflict recovery, fills recorded.
    """
    from decimal import Decimal

    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderState,
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.execution_account.binance.user_data_parser import (
        order_snapshot_from_update,
    )
    from crypto_momentum_lab.execution_account.hub import AccountEvent
    from crypto_momentum_lab.live_rollout.order_identity_errors import (
        is_runtime_order_identity_conflict,
    )

    plan = OrderExecutionPlan(
        intent_id="exit-long-1",
        run_id="run-1",
        client_order_id="exit-long-1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("0.002"),
        price=Decimal("50000"),
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
    )

    reconciled_snapshots = []

    class Reconciler:
        run_id = "run-1"

        async def reconcile_account_event(self, event: AccountEvent) -> None:
            if event.order_update is not None:
                snapshot = order_snapshot_from_update(event.order_update, plan)
                reconciled_snapshots.append(snapshot)

    telemetry_fills = []

    class TelemetrySink:
        async def account_fill(self, fill_event, *, occurred_at):
            telemetry_fills.append(fill_event)

    recovery_reasons = []
    exit_failures = []
    snapshots_received = []

    class Cache:
        def for_symbols(self, symbols):
            return ()

    runtime = LiveAccountEventRuntime(
        run_id="run-1",
        daemon=object(),
        latest_market_states=Cache(),
        latest_market_quotes=Cache(),
        order_reconciliation=Reconciler(),
        telemetry=TelemetrySink(),
        is_transient_error=lambda err: False,
        is_order_identity_conflict=is_runtime_order_identity_conflict,
        on_exit_failure=lambda sym, fail: exit_failures.append((sym, fail)),
        on_account_snapshot=AsyncMock(
            side_effect=lambda ev: snapshots_received.append(ev)
        ),
        on_account_snapshot_recovery=recovery_reasons.append,
    )

    ws_order = {
        "c": "exit-long-1",
        "s": "BTCUSDT",
        "S": "SELL",
        "o": "LIMIT",
        "q": "0.002",
        "p": "50000",
        "R": False,
        "ps": "LONG",
        "i": 2001,
        "X": "FILLED",
        "z": "0.002",
        "ap": "50000",
        "T": int(NOW.timestamp() * 1000),
    }
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="trade-999",
        order_id="2001",
        side="SELL",
        price=Decimal("50000"),
        quantity=Decimal("0.002"),
        realized_pnl=Decimal("10.0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={},
    )
    order_trade_event = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ORDER_TRADE_UPDATE",
        event_id="event-1",
        event_at=NOW,
        received_at=NOW,
        symbols=("BTCUSDT",),
        symbol="BTCUSDT",
        client_order_id="exit-long-1",
        has_fill=True,
        trade_id="trade-999",
        fills=(fill,),
        order_update=ws_order,
    )

    # 1. Process order trade update
    await runtime._process_event(order_trade_event)

    assert len(reconciled_snapshots) == 1
    assert reconciled_snapshots[0].state is ExchangeOrderState.FILLED
    assert len(telemetry_fills) == 1
    assert telemetry_fills[0].trade_id == "trade-999"
    assert recovery_reasons == []
    assert exit_failures == []

    # 2. Replay the same trade ID -> deduplicated, not recorded again
    await runtime._process_event(order_trade_event)
    assert len(telemetry_fills) == 1

    # 3. Subsequent ACCOUNT_UPDATE with position reduced to zero
    zero_pos_event = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ACCOUNT_UPDATE",
        event_id="event-2",
        event_at=NOW,
        received_at=NOW,
        symbols=("BTCUSDT",),
        has_fill=False,
    )
    await runtime._process_event(zero_pos_event)
    assert recovery_reasons == []
    assert exit_failures == []
    assert len(snapshots_received) == 3
