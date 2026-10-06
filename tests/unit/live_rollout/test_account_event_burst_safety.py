import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.live_rollout.account_channel import (
    LiveAccountEventRuntime,
    _coalesce_account_event_burst,
)
from crypto_momentum_lab.live_rollout.market_loop import (
    _orphan_reduce_only_limit_plans,
)
from tests.unit.execution_account.test_hub import _event, _snapshot


def test_account_event_burst_capacity_and_metrics_are_configurable() -> None:
    from crypto_momentum_lab.execution_account.hub import (
        AccountEventHubConfig,
        WebSocketAccountEventSource,
    )

    source = WebSocketAccountEventSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="burst-test",
        config=AccountEventHubConfig(client_receive_queue_size=256),
    )
    queue = asyncio.Queue(maxsize=256)
    for sequence in range(1, 18):
        source._enqueue_account_event(
            queue,
            replace(_event(), event_id=f"event-{sequence}", sequence=sequence),
        )

    metrics = source.metrics
    assert metrics.queue_overflow_count == 0
    assert metrics.queue_capacity == 256
    assert metrics.queue_high_watermark == 17
    assert metrics.order_trade_update_count == 17


def test_consecutive_account_updates_reduce_to_latest_full_snapshot() -> None:
    first = replace(
        _event(),
        event_type="ACCOUNT_UPDATE",
        event_id="account-1",
        symbols=("BTCUSDT",),
        snapshot_kind="full",
        account_snapshot=_snapshot(),
    )
    second = replace(
        first,
        event_id="account-2",
        symbols=("ETHUSDT",),
        snapshot_kind="delta",
    )

    reduced = _coalesce_account_event_burst([first, second])

    assert len(reduced) == 1
    assert reduced[0].event_id == "account-2"
    assert reduced[0].symbols == ("BTCUSDT", "ETHUSDT")
    assert reduced[0].snapshot_kind == "full"
    assert reduced[0].account_delta is None


def test_order_updates_separated_by_account_update_are_not_coalesced() -> None:
    order = _event()
    account = replace(
        order,
        event_type="ACCOUNT_UPDATE",
        event_id="account-1",
        account_snapshot=_snapshot(),
        snapshot_kind="full",
    )

    assert _coalesce_account_event_burst([order, account, order]) == (
        order,
        account,
        order,
    )


def test_consecutive_order_updates_coalesce_but_retain_every_fill() -> None:
    first = replace(
        _event(),
        event_id="trade-1",
        has_fill=True,
        trade_id="trade-1",
        fills=(_fill("trade-1"),),
        snapshot_kind="full",
        account_snapshot=_snapshot(),
    )
    second = replace(
        first,
        event_id="trade-2",
        trade_id="trade-2",
        fills=(_fill("trade-2"),),
        snapshot_kind="delta",
    )

    reduced = _coalesce_account_event_burst([first, second])

    assert len(reduced) == 1
    assert reduced[0].event_id == "trade-2"
    assert tuple(fill.trade_id for fill in reduced[0].fills) == ("trade-1", "trade-2")
    assert reduced[0].snapshot_kind == "full"
    assert reduced[0].account_delta is None


@pytest.mark.asyncio
async def test_coalesced_order_update_emits_telemetry_for_every_fill() -> None:
    first = replace(
        _event(),
        event_id="trade-1",
        has_fill=True,
        trade_id="trade-1",
        fills=(_fill("trade-1"),),
    )
    second = replace(
        first,
        event_id="trade-2",
        trade_id="trade-2",
        fills=(_fill("trade-2"),),
    )
    event = _coalesce_account_event_burst([first, second])[0]
    observed_trade_ids: list[str | None] = []

    class Cache:
        def for_symbols(self, _symbols):
            return ()

    class Telemetry:
        async def account_fill(self, received_event, *, occurred_at) -> None:
            del occurred_at
            observed_trade_ids.append(received_event.trade_id)

    runtime = LiveAccountEventRuntime(
        run_id="burst-test",
        daemon=object(),
        latest_market_states=Cache(),
        latest_market_quotes=Cache(),
        telemetry=Telemetry(),
        is_transient_error=lambda _error: False,
    )

    await runtime._process_event(event)

    assert observed_trade_ids == ["trade-1", "trade-2"]


def test_unknown_account_projection_never_marks_reduce_only_limit_as_orphan() -> None:
    plan = SimpleNamespace(symbol="BTCUSDT", reduce_only=True, order_type="LIMIT")
    context = SimpleNamespace(
        open_position_symbols=None,
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
        unresolved_orders=(SimpleNamespace(plan=plan),),
    )

    assert _orphan_reduce_only_limit_plans(context) == ()


def test_confirmed_flat_account_marks_reduce_only_limit_as_orphan() -> None:
    plan = SimpleNamespace(symbol="BTCUSDT", reduce_only=True, order_type="LIMIT")
    context = SimpleNamespace(
        open_position_symbols=frozenset(),
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
        unresolved_orders=(SimpleNamespace(plan=plan),),
    )

    assert _orphan_reduce_only_limit_plans(context) == (plan,)


@pytest.mark.asyncio
async def test_runtime_ingress_queue_accepts_a_normal_partial_fill_burst() -> None:
    events = tuple(
        replace(_event(), event_id=f"event-{sequence}", sequence=sequence)
        for sequence in range(1, 18)
    )
    applied: list[str] = []

    class Cache:
        def for_symbols(self, _symbols):
            return ()

    source = iter(events)

    class FiniteSource:
        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(source)
            except StopIteration as error:
                raise StopAsyncIteration from error

    runtime = LiveAccountEventRuntime(
        run_id="burst-test",
        daemon=object(),
        latest_market_states=Cache(),
        latest_market_quotes=Cache(),
        is_transient_error=lambda _error: False,
        on_account_snapshot=lambda event: _record(applied, event.event_id),
    )

    await runtime.run(FiniteSource())

    assert applied == ["event-17"]


async def _record(target: list[str], event_id: str) -> None:
    target.append(event_id)


def _fill(trade_id: str) -> AccountFillEvent:
    event = _event()
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=trade_id,
        order_id="order-1",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("0.1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=event.event_at,
        raw_payload={},
    )
