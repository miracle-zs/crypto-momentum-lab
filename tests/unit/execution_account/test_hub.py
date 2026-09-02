import asyncio
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import crypto_momentum_lab.execution_account.hub as hub_module
from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    AccountEventHub,
    AccountEventHubConfig,
    AccountEventHubSequenceGap,
    WebSocketAccountEventSource,
    decode_account_event,
    encode_account_event,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    diff_account_snapshots,
)


def _event() -> AccountEvent:
    observed_at = datetime(2026, 8, 21, 9, 0, tzinfo=UTC)
    return AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ORDER_TRADE_UPDATE",
        event_id="event-1",
        event_at=observed_at,
        received_at=observed_at,
        symbols=("BTCUSDT",),
        symbol="BTCUSDT",
        client_order_id="live-entry-1",
        order_status="FILLED",
        reason="ready_readonly",
    )


def test_account_event_round_trips() -> None:
    event = _event()

    decoded = decode_account_event(
        encode_account_event(event, sequence=1),
        expected_environment="live",
        expected_account_label="primary",
    )

    assert decoded == event


def test_account_event_round_trips_fill_identity() -> None:
    event = replace(_event(), has_fill=True, trade_id="trade-1")

    decoded = decode_account_event(
        encode_account_event(event, sequence=1),
        expected_environment="live",
        expected_account_label="primary",
    )

    assert decoded.has_fill is True
    assert decoded.trade_id == "trade-1"


def test_account_event_round_trips_versioned_full_snapshot() -> None:
    snapshot = _snapshot()
    event = replace(
        _event(),
        account_state=ExecutionAccountStatus.READY_READONLY,
        account_snapshot=snapshot,
    )

    decoded = decode_account_event(
        encode_account_event(event, sequence=7),
        expected_environment="live",
        expected_account_label="primary",
    )

    assert decoded.sequence == 7
    assert decoded.account_state is ExecutionAccountStatus.READY_READONLY
    assert decoded.account_snapshot == snapshot


def test_account_event_round_trips_delta_without_serializing_full_snapshot() -> None:
    previous = _snapshot()
    current = replace(
        previous,
        balances=(replace(previous.balances[0], wallet_balance=Decimal("120")),),
    )
    event = replace(
        _event(),
        snapshot_kind="delta",
        account_snapshot=current,
        account_delta=diff_account_snapshots(previous, current),
    )

    encoded = encode_account_event(event, sequence=8)
    decoded = decode_account_event(
        encoded,
        expected_environment="live",
        expected_account_label="primary",
    )

    assert '"account_snapshot":null' in encoded
    assert decoded.snapshot_kind == "delta"
    assert decoded.account_snapshot is None
    assert decoded.account_delta is not None
    assert decoded.account_delta.balances[0].wallet_balance == Decimal("120")


def test_account_event_source_materializes_full_then_delta() -> None:
    previous = _snapshot()
    current = replace(
        previous,
        balances=(replace(previous.balances[0], wallet_balance=Decimal("120")),),
    )
    source = WebSocketAccountEventSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="test-live-exit",
    )
    full = replace(
        _event(),
        sequence=1,
        snapshot_kind="full",
        account_snapshot=previous,
    )
    delta = replace(
        _event(),
        event_id="event-2",
        sequence=2,
        snapshot_kind="delta",
        account_delta=diff_account_snapshots(previous, current),
    )

    first = source._materialize_event(full)
    second = source._materialize_event(delta)

    assert first is not None
    assert first.account_snapshot == previous
    assert second is not None
    assert second.account_snapshot == current


def test_account_event_source_requests_full_snapshot_after_sequence_gap() -> None:
    previous = _snapshot()
    source = WebSocketAccountEventSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="test-live-exit",
    )
    full = replace(
        _event(),
        sequence=1,
        snapshot_kind="full",
        account_snapshot=previous,
    )
    gap = replace(
        _event(),
        event_id="event-3",
        sequence=3,
        snapshot_kind="delta",
        account_delta=diff_account_snapshots(previous, previous),
    )

    source._materialize_event(full)
    with pytest.raises(AccountEventHubSequenceGap):
        source._materialize_event(gap)

    assert source._require_full_snapshot is True
    assert source._last_sequence is None
    assert source._account_snapshot is None


def test_account_event_hub_bootstraps_latest_state_and_replays_delta() -> None:
    previous = _snapshot()
    current = replace(
        previous,
        balances=(replace(previous.balances[0], wallet_balance=Decimal("120")),),
    )
    hub = AccountEventHub(AccountEventHubConfig(replay_event_count=2))
    hub.publish(
        replace(
            _event(),
            snapshot_kind="full",
            account_snapshot=previous,
            account_state=ExecutionAccountStatus.READY_READONLY,
        )
    )
    hub.publish(
        replace(
            _event(),
            event_id="event-2",
            snapshot_kind="delta",
            account_snapshot=current,
            account_delta=diff_account_snapshots(previous, current),
        )
    )

    bootstrap_messages, used_full, stream_reset = hub._subscription_messages(
        ("live", "primary"),
        requested_epoch=None,
        last_sequence=None,
        require_full_snapshot=True,
    )
    replay_messages, replay_full, replay_reset = hub._subscription_messages(
        ("live", "primary"),
        requested_epoch=hub._stream_epoch,
        last_sequence=1,
        require_full_snapshot=False,
    )

    bootstrap = decode_account_event(
        bootstrap_messages[0],
        expected_environment="live",
        expected_account_label="primary",
    )
    replay = decode_account_event(
        replay_messages[0],
        expected_environment="live",
        expected_account_label="primary",
    )
    assert used_full is True
    assert stream_reset is False
    assert bootstrap.snapshot_kind == "full"
    assert bootstrap.account_snapshot == current
    assert replay_full is False
    assert replay_reset is False
    assert replay.snapshot_kind == "delta"
    assert replay.sequence == 2


def _snapshot() -> AccountSnapshot:
    observed_at = datetime(2026, 8, 21, 9, 0, tzinfo=UTC)
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            can_trade=True,
            fee_tier=0,
            observed_at=observed_at,
            raw_payload={},
        ),
        balances=(
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("0"),
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        positions=(
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="LONG",
                position_amt=Decimal("0.5"),
                entry_price=Decimal("100"),
                mark_price=Decimal("101"),
                unrealized_pnl=Decimal("0.5"),
                notional=Decimal("50.5"),
                leverage=5,
                margin_type="CROSSED",
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        open_orders=(
            AccountOpenOrderSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                order_id="100",
                client_order_id="live-entry-1",
                side="BUY",
                order_type="LIMIT",
                status="NEW",
                price=Decimal("99"),
                original_quantity=Decimal("0.5"),
                executed_quantity=Decimal("0"),
                reduce_only=False,
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
    )


@pytest.mark.skipif(
    os.environ.get("CML_RUN_HUB_NETWORK_TESTS") != "1",
    reason="requires local loopback socket permission",
)
async def test_account_event_hub_fans_out_latest_event() -> None:
    hub = AccountEventHub(
        AccountEventHubConfig(
            host="127.0.0.1",
            port=0,
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        )
    )
    await hub.start()
    source = WebSocketAccountEventSource(
        url=hub.url,
        environment="live",
        account_label="primary",
        consumer_id="test-live-exit",
        config=AccountEventHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        ),
    )
    event = _event()
    hub.publish(event)
    iterator = source.__aiter__()
    next_event = asyncio.create_task(iterator.__anext__())
    try:
        for _ in range(100):
            if hub.subscriber_count == 1:
                break
            await asyncio.sleep(0.01)
        assert hub.subscriber_count == 1

        assert await asyncio.wait_for(next_event, timeout=1) == event
    finally:
        source.stop()
        if not next_event.done():
            next_event.cancel()
        await hub.stop()
        await asyncio.gather(next_event, return_exceptions=True)


@pytest.mark.skipif(
    os.environ.get("CML_RUN_HUB_NETWORK_TESTS") != "1",
    reason="requires local loopback socket permission",
)
async def test_account_event_hub_bootstraps_and_applies_live_delta() -> None:
    previous = _snapshot()
    current = replace(
        previous,
        balances=(replace(previous.balances[0], wallet_balance=Decimal("120")),),
    )
    hub = AccountEventHub(
        AccountEventHubConfig(
            host="127.0.0.1",
            port=0,
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        )
    )
    await hub.start()
    hub.publish(
        replace(
            _event(),
            snapshot_kind="full",
            account_snapshot=previous,
            account_state=ExecutionAccountStatus.READY_READONLY,
        )
    )
    source = WebSocketAccountEventSource(
        url=hub.url,
        environment="live",
        account_label="primary",
        consumer_id="test-live-delta",
        config=AccountEventHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=1,
        ),
    )
    iterator = source.__aiter__()
    try:
        first = await asyncio.wait_for(iterator.__anext__(), timeout=1)
        assert first.snapshot_kind == "full"
        assert first.account_snapshot == previous
        hub.publish(
            replace(
                _event(),
                event_id="event-2",
                snapshot_kind="delta",
                account_snapshot=current,
                account_delta=diff_account_snapshots(previous, current),
            )
        )
        second = await asyncio.wait_for(iterator.__anext__(), timeout=1)
        assert second.snapshot_kind == "delta"
        assert second.account_snapshot == current
    finally:
        source.stop()
        await iterator.aclose()
        await hub.stop()


async def test_account_event_source_fails_closed_when_hub_is_unavailable() -> None:
    source = WebSocketAccountEventSource(
        url="ws://127.0.0.1:1",
        environment="live",
        account_label="primary",
        consumer_id="test-live-exit",
        config=AccountEventHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=0.05,
            handshake_timeout_seconds=0.01,
        ),
    )

    with pytest.raises(RuntimeError, match="account-event hub unavailable"):
        await anext(source.__aiter__())


async def test_account_event_source_reader_prefetches_while_consumer_is_busy(
    monkeypatch,
) -> None:
    first = _event()
    second = replace(first, event_id="event-2", client_order_id="live-exit-1")
    second_received = asyncio.Event()
    idle = asyncio.Event()

    class FakeConnection:
        def __init__(self) -> None:
            self._messages = [
                json.dumps(
                    {
                        "type": "account_event_hub_ready",
                        "schema_version": 1,
                        "environment": "live",
                        "account_label": "primary",
                    }
                ),
                encode_account_event(first, sequence=1),
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _message):
            return None

        async def recv(self):
            if self._messages:
                return self._messages.pop(0)
            if not second_received.is_set():
                second_received.set()
                return encode_account_event(second, sequence=2)
            await idle.wait()
            raise OSError("simulated disconnect")

    monkeypatch.setattr(
        hub_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(),
    )
    source = WebSocketAccountEventSource(
        url="ws://unused",
        environment="live",
        account_label="primary",
        consumer_id="test-live-exit",
        config=AccountEventHubConfig(
            reconnect_delays=(0,),
            unavailable_timeout_seconds=10,
        ),
    )
    iterator = source.__aiter__()
    try:
        assert await anext(iterator) == first
        await asyncio.wait_for(second_received.wait(), timeout=1)
        assert await anext(iterator) == second
    finally:
        source.stop()
        await iterator.aclose()
