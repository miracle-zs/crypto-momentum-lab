import asyncio

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    ConnectionLifecycleEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.binance.websocket import (
    BinanceWebSocketConnection,
)


@pytest.mark.e2e
async def test_connection_reconnects_with_new_session_and_full_set(
    fake_binance_server,
) -> None:
    received: list[RawEnvelope] = []
    lifecycle: list[ConnectionLifecycleEvent] = []

    async def receive(envelope: RawEnvelope) -> None:
        received.append(envelope)

    async def observe(event: ConnectionLifecycleEvent) -> None:
        lifecycle.append(event)

    connection = BinanceWebSocketConnection(
        base_url=fake_binance_server.market_url,
        route=CaptureRoute.MARKET,
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=receive,
        on_lifecycle=observe,
        reconnect_delays=(0.0, 0.0),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    task = asyncio.create_task(connection.run())
    await fake_binance_server.wait_for_connections(2)
    await connection.stop()
    await task

    assert received
    assert len({event.session_id for event in lifecycle if event.opened}) == 2
    assert fake_binance_server.subscribe_requests == [
        ("btcusdt@aggTrade",),
        ("btcusdt@aggTrade",),
    ]
