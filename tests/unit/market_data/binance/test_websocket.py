import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.market_data.binance.websocket import (
    BinancePayloadError,
    parse_binance_message,
    route_for,
)


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
