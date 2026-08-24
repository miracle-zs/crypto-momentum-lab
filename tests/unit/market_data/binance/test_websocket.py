import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.market_data.binance.websocket import (
    BinancePayloadError,
    BinanceWebSocketConnection,
    parse_binance_message,
    route_for,
)
from crypto_momentum_lab.market_data.capture.queue import CaptureQueueFull


@pytest.mark.parametrize(
    (
        "stream_name",
        "payload",
        "expected_stream",
        "expected_symbol",
    ),
    [
        (
            "btcusdt@aggTrade",
            {"e": "aggTrade", "E": 1781488800000, "s": "BTCUSDT", "a": 42},
            CaptureStream.AGG_TRADE,
            "BTCUSDT",
        ),
        (
            "btcusdt@bookTicker",
            {"e": "bookTicker", "E": 1781488800000, "s": "BTCUSDT", "u": 7},
            CaptureStream.BOOK_TICKER,
            "BTCUSDT",
        ),
        (
            "btcusdt@forceOrder",
            {
                "e": "forceOrder",
                "E": 1781488800000,
                "o": {"s": "BTCUSDT", "T": 1781488799000},
            },
            CaptureStream.FORCE_ORDER,
            "BTCUSDT",
        ),
        (
            "btcusdt@markPrice@1s",
            {"e": "markPriceUpdate", "E": 1781488800000, "s": "BTCUSDT"},
            CaptureStream.MARK_PRICE,
            "BTCUSDT",
        ),
        (
            "btcusdt@kline_1m",
            {
                "e": "kline",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "k": {"t": 1781488800000},
            },
            CaptureStream.KLINE_1M,
            "BTCUSDT",
        ),
        (
            "btcusdt@kline_15m",
            {
                "e": "kline",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "k": {"t": 1781488800000},
            },
            CaptureStream.KLINE_15M,
            "BTCUSDT",
        ),
    ],
)
def test_parses_combined_stream_payloads(
    stream_name: str,
    payload: dict[str, object],
    expected_stream: CaptureStream,
    expected_symbol: str,
) -> None:
    envelope = parse_binance_message(
        route=route_for(expected_stream),
        message=json.dumps({"stream": stream_name, "data": payload}),
        environment="test",
        connection_session_id=UUID(int=1),
        local_sequence=1,
        subscription_generation=3,
        received_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_monotonic_ns=10,
    )

    assert envelope.stream is expected_stream
    assert envelope.symbol == expected_symbol
    assert envelope.exchange_event_at is not None


def test_rejects_malformed_combined_stream_payload() -> None:
    with pytest.raises(BinancePayloadError):
        parse_binance_message(
            route=route_for(CaptureStream.AGG_TRADE),
            message=json.dumps({"stream": "btcusdt@aggTrade"}),
            environment="test",
            connection_session_id=UUID(int=1),
            local_sequence=1,
            subscription_generation=3,
            received_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
            received_monotonic_ns=10,
        )


def test_parses_raw_stream_payload_when_expected_stream_is_known() -> None:
    envelope = parse_binance_message(
        route=route_for(CaptureStream.AGG_TRADE),
        message=json.dumps(
            {
                "e": "aggTrade",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "a": 42,
            }
        ),
        environment="test",
        connection_session_id=UUID(int=1),
        local_sequence=1,
        subscription_generation=3,
        received_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_monotonic_ns=10,
        expected_stream=CaptureStream.AGG_TRADE,
    )

    assert envelope.stream is CaptureStream.AGG_TRADE
    assert envelope.symbol == "BTCUSDT"
    assert envelope.exchange_sequence == "42"


def test_parses_global_book_ticker_stream_payload() -> None:
    envelope = parse_binance_message(
        route=route_for(CaptureStream.BOOK_TICKER),
        message=json.dumps(
            {
                "stream": "!bookTicker",
                "data": {
                    "e": "bookTicker",
                    "E": 1781488800000,
                    "s": "BTCUSDT",
                    "u": 7,
                },
            }
        ),
        environment="test",
        connection_session_id=UUID(int=1),
        local_sequence=1,
        subscription_generation=3,
        received_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_monotonic_ns=10,
        expected_stream=CaptureStream.BOOK_TICKER,
    )

    assert envelope.stream is CaptureStream.BOOK_TICKER
    assert envelope.symbol == "BTCUSDT"


def test_fast_symbol_extracts_combined_and_nested_payloads() -> None:
    from crypto_momentum_lab.market_data.binance.websocket import _fast_symbol

    assert (
        _fast_symbol(
            {
                "stream": "!bookTicker",
                "data": {"s": "BTCUSDT"},
            }
        )
        == "BTCUSDT"
    )
    assert _fast_symbol({"o": {"s": "ETHUSDT"}}) == "ETHUSDT"


@pytest.mark.asyncio
async def test_connection_reconnects_after_capture_queue_backpressure(
    monkeypatch,
) -> None:
    attempts = 0
    lifecycle_reasons: list[str | None] = []

    async def observe_lifecycle(event) -> None:
        lifecycle_reasons.append(event.reason)

    connection = BinanceWebSocketConnection(
        base_url="wss://example.test/ws",
        route=route_for(CaptureStream.AGG_TRADE),
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=lambda envelope: asyncio.sleep(0),
        on_lifecycle=observe_lifecycle,
        reconnect_delays=(0.0,),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    async def fake_run_once(session_id) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CaptureQueueFull("queue event limit reached")
        connection._stopping = True
        return "stopped"

    monkeypatch.setattr(connection, "_run_once", fake_run_once)

    await connection.run()

    assert attempts == 2
    assert lifecycle_reasons == ["CaptureQueueFull", "stopped"]


@pytest.mark.asyncio
async def test_connection_reconnects_after_unexpected_session_error(
    monkeypatch,
) -> None:
    attempts = 0
    lifecycle_reasons: list[str | None] = []

    async def observe_lifecycle(event) -> None:
        lifecycle_reasons.append(event.reason)

    connection = BinanceWebSocketConnection(
        base_url="wss://example.test/ws",
        route=route_for(CaptureStream.AGG_TRADE),
        environment="test",
        desired_names=("btcusdt@aggTrade",),
        generation=1,
        on_envelope=lambda envelope: asyncio.sleep(0),
        on_lifecycle=observe_lifecycle,
        reconnect_delays=(0.0,),
        connection_lifetime_seconds=60,
        open_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        silence_timeout_seconds=2,
    )

    async def fake_run_once(session_id) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("unexpected test failure")
        connection._stopping = True
        return "stopped"

    monkeypatch.setattr(connection, "_run_once", fake_run_once)

    await connection.run()

    assert attempts == 2
    assert lifecycle_reasons == ["RuntimeError", "stopped"]


@pytest.mark.asyncio
async def test_subscription_updates_are_queued_without_direct_socket_access(
    monkeypatch,
) -> None:
    connection = BinanceWebSocketConnection(
        base_url="wss://example.test/ws",
        route=route_for(CaptureStream.AGG_TRADE),
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
    )
    connection._connection = object()

    async def forbidden_send(*args, **kwargs) -> None:
        del args, kwargs
        raise AssertionError("subscription updates must be sent by the actor")

    monkeypatch.setattr(connection, "_send_control", forbidden_send)

    await connection.subscribe(("ethusdt@aggTrade",), generation=2)
    await connection.unsubscribe(("btcusdt@aggTrade",), generation=3)

    assert connection._desired_names == ("ethusdt@aggTrade",)
    assert connection._generation == 3


@pytest.mark.parametrize(
    ("stream", "expected_timeout"),
    [
        (CaptureStream.AGG_TRADE, None),
        (CaptureStream.BOOK_TICKER, None),
        (CaptureStream.FORCE_ORDER, None),
    ],
)
def test_event_stream_silence_policy(
    stream: CaptureStream,
    expected_timeout: float | None,
) -> None:
    from crypto_momentum_lab.market_data.binance.websocket import (
        _silence_timeout_for_stream,
    )

    assert (
        _silence_timeout_for_stream(stream, configured_timeout=30.0)
        == expected_timeout
    )


@pytest.mark.parametrize(
    ("stream", "expected_timeout"),
    [
        (CaptureStream.AGG_TRADE, None),
        (CaptureStream.BOOK_TICKER, None),
        (CaptureStream.FORCE_ORDER, 10.0),
    ],
)
def test_ping_timeout_policy(
    stream: CaptureStream,
    expected_timeout: float | None,
) -> None:
    from crypto_momentum_lab.market_data.binance.websocket import (
        _ping_timeout_for_stream,
    )

    assert (
        _ping_timeout_for_stream(stream, configured_timeout=10.0)
        == expected_timeout
    )
