from datetime import UTC, datetime

from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUsdMUserDataStream,
    _fill_event_key,
    _is_fill_event,
    parse_user_data_event,
)


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeListenKeyClient:
    async def start_user_data_stream(self) -> str:
        return "listen-key"

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        return None

    async def close_user_data_stream(self, listen_key: str) -> None:
        return None


async def test_stream_exposes_event_counters_and_can_request_reconnect() -> None:
    stream = BinanceUsdMUserDataStream(
        listen_key_client=FakeListenKeyClient(),
        reconnect_delays=(0,),
    )
    connection = FakeConnection()
    stream._connection = connection

    metrics = stream.metrics
    await stream.request_reconnect("test_watchdog")

    assert connection.closed
    assert metrics.received_message_count == 0
    assert metrics.parsed_event_count == 0
    assert metrics.fill_event_count == 0
    assert metrics.last_event_received_at is None


def test_stream_classifies_only_nonzero_trade_updates_as_fill_events() -> None:
    event = parse_user_data_event(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1783209600000,
            "o": {"x": "TRADE", "l": "0.01"},
        },
        received_at=datetime(2026, 8, 28, tzinfo=UTC),
    )
    zero_event = parse_user_data_event(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1783209600000,
            "o": {"x": "TRADE", "l": "0"},
        },
        received_at=datetime(2026, 8, 28, tzinfo=UTC),
    )

    assert _is_fill_event(event)
    assert not _is_fill_event(zero_event)


def test_stream_extracts_unique_fill_identity() -> None:
    event = parse_user_data_event(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1783209600000,
            "o": {
                "s": "btcusdt",
                "t": 42,
                "x": "TRADE",
                "l": "0.01",
            },
        },
        received_at=datetime(2026, 8, 28, tzinfo=UTC),
    )

    assert _fill_event_key(event) == ("BTCUSDT", "42")
