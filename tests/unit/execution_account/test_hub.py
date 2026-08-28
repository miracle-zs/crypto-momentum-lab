import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    AccountEventHub,
    AccountEventHubConfig,
    WebSocketAccountEventSource,
    decode_account_event,
    encode_account_event,
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
    iterator = source.__aiter__()
    next_event = asyncio.create_task(iterator.__anext__())
    try:
        for _ in range(100):
            if hub.subscriber_count == 1:
                break
            await asyncio.sleep(0.01)
        assert hub.subscriber_count == 1

        event = _event()
        hub.publish(event)

        assert await asyncio.wait_for(next_event, timeout=1) == event
    finally:
        source.stop()
        if not next_event.done():
            next_event.cancel()
        await hub.stop()
        await asyncio.gather(next_event, return_exceptions=True)


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
