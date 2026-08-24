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
from crypto_momentum_lab.market_data.capture.queue import CaptureQueueFull


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
    try:
        await fake_binance_server.wait_for_connections(2)
        await fake_binance_server.wait_for_subscription_requests(2)
        for _ in range(100):
            metrics = connection.metrics_snapshot()
            if metrics.reader_task_alive and metrics.dispatch_task_alive:
                break
            await asyncio.sleep(0.01)
        assert metrics.reader_task_alive is True
        assert metrics.dispatch_task_alive is True
        assert metrics.ingress_queue_max_events == 4096
        assert metrics.ingress_queue_high_watermark_events >= 1
    finally:
        await connection.stop()
        await task

    assert received
    assert len({event.session_id for event in lifecycle if event.opened}) == 2
    assert fake_binance_server.subscribe_requests == [
        ("btcusdt@aggTrade",),
        ("btcusdt@aggTrade",),
    ]


@pytest.mark.e2e
async def test_connection_rebuilds_when_envelope_dispatch_fails(
    fake_binance_server,
) -> None:
    fake_binance_server.close_first_connection = False
    lifecycle: list[ConnectionLifecycleEvent] = []

    async def reject(envelope: RawEnvelope) -> None:
        del envelope
        raise CaptureQueueFull("queue event limit reached")

    async def observe(event: ConnectionLifecycleEvent) -> None:
        lifecycle.append(event)

    connection = BinanceWebSocketConnection(
        base_url=fake_binance_server.market_url,
        route=CaptureRoute.MARKET,
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=reject,
        on_lifecycle=observe,
        reconnect_delays=(0.0,),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    run_task = asyncio.create_task(connection.run())
    try:
        await fake_binance_server.wait_for_connections(2)
    finally:
        await connection.stop()
        await run_task

    assert any(
        event.reason == "CaptureQueueFull" and not event.opened
        for event in lifecycle
    )


@pytest.mark.e2e
async def test_subscription_update_during_handshake_is_queued_for_actor(
    fake_binance_server,
) -> None:
    opened = asyncio.Event()
    release_opened = asyncio.Event()

    async def receive(envelope: RawEnvelope) -> None:
        del envelope

    async def observe(event: ConnectionLifecycleEvent) -> None:
        if event.opened:
            opened.set()
            await release_opened.wait()

    connection = BinanceWebSocketConnection(
        base_url=fake_binance_server.market_url,
        route=CaptureRoute.MARKET,
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=receive,
        on_lifecycle=observe,
        reconnect_delays=(0.0,),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    run_task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(opened.wait(), timeout=5)
        await asyncio.wait_for(
            connection.subscribe(("ethusdt@aggTrade",), generation=2),
            timeout=0.5,
        )
        release_opened.set()
        await fake_binance_server.wait_for_subscriptions(
            {"btcusdt@aggTrade", "ethusdt@aggTrade"}
        )
    finally:
        release_opened.set()
        await connection.stop()
        await run_task

    assert fake_binance_server.subscribe_requests


@pytest.mark.e2e
async def test_connection_rebuilds_when_control_ack_never_arrives(
    fake_binance_server,
) -> None:
    fake_binance_server.close_first_connection = False
    fake_binance_server.acknowledge_controls = False

    connection = BinanceWebSocketConnection(
        base_url=fake_binance_server.market_url,
        route=CaptureRoute.MARKET,
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=lambda envelope: asyncio.sleep(0),
        on_lifecycle=lambda event: asyncio.sleep(0),
        reconnect_delays=(0.0,),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
        control_ack_timeout_seconds=0.05,
    )

    run_task = asyncio.create_task(connection.run())
    try:
        await fake_binance_server.wait_for_connections(2)
        assert connection.metrics_snapshot().reconnect_count >= 1
    finally:
        await connection.stop()
        await run_task
