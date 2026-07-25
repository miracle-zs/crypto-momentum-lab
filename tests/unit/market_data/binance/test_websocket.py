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
