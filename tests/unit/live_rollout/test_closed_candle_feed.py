from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
    ClosedCandle15mEvent,
    ClosedCandle15mFeedConfig,
    ClosedCandleFeedOverflow,
    decode_closed_candle_event,
)
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m

RECEIVED_AT = datetime(2026, 8, 24, 14, 45, 0, 100000, tzinfo=UTC)


def _envelope(*, closed: bool = True) -> RawEnvelope:
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.KLINE_15M,
        symbol="BTCUSDT",
        exchange_event_at=datetime(2026, 8, 24, 14, 45, tzinfo=UTC),
        received_at=RECEIVED_AT,
        received_monotonic_ns=100,
        connection_session_id=UUID(int=1),
        local_sequence=1,
        exchange_sequence="1781488800000",
        subscription_generation=1,
        raw_payload={
            "e": "kline",
            "E": 1787582700000,
            "s": "BTCUSDT",
            "k": {
                "t": 1787581800000,
                "T": 1787582699999,
                "s": "BTCUSDT",
                "i": "15m",
                "o": "100",
                "c": "101.5",
                "x": closed,
            },
        },
    )


def test_decode_closed_candle_ignores_open_updates() -> None:
    assert decode_closed_candle_event(_envelope(closed=False)) is None


def test_decode_closed_candle_requires_official_final_flag() -> None:
    event = decode_closed_candle_event(_envelope())

    assert event is not None
    assert event.candle.symbol == "BTCUSDT"
    assert event.candle.candle_end - event.candle.candle_start == timedelta(
        minutes=15
    )
    assert event.candle.open_price == Decimal("100")
    assert event.candle.close_price == Decimal("101.5")
    assert event.recovered is False


@pytest.mark.asyncio
async def test_feed_deduplicates_same_symbol_and_candle_start() -> None:
    feed = BinanceClosedCandle15mFeed(
        config=ClosedCandle15mFeedConfig(
            websocket_url="wss://example.test/market/ws",
        )
    )

    await feed._receive_envelope(_envelope())
    await feed._receive_envelope(_envelope())

    event = await feed._events.get()
    assert event.candle.symbol == "BTCUSDT"
    assert feed._events.empty()


@pytest.mark.asyncio
async def test_feed_recovery_publishes_only_missing_candles() -> None:
    class Backfill:
        def __init__(self) -> None:
            self.calls: list[tuple[str, datetime, datetime]] = []

        def load_closed_candles(
            self,
            *,
            symbol: str,
            start: datetime,
            end: datetime,
        ) -> tuple[ClosedCandle15m, ...]:
            self.calls.append((symbol, start, end))
            return (
                ClosedCandle15m(
                    symbol=symbol,
                    candle_start=start,
                    candle_end=start + timedelta(minutes=15),
                    open_price=Decimal("100"),
                    close_price=Decimal("99"),
                ),
            )

    backfill = Backfill()
    feed = BinanceClosedCandle15mFeed(
        config=ClosedCandle15mFeedConfig(
            websocket_url="wss://example.test/market/ws",
        ),
        backfill_source=backfill,
        clock=lambda: RECEIVED_AT,
    )
    through = datetime(2026, 8, 24, 15, 1, tzinfo=UTC)

    count = await feed.recover_missing(symbol="BTCUSDT", through=through)

    assert count == 1
    assert backfill.calls[0][0] == "BTCUSDT"
    event = await feed._events.get()
    assert event.recovered is True


@pytest.mark.asyncio
async def test_feed_overflow_does_not_ack_a_dropped_candle() -> None:
    feed = BinanceClosedCandle15mFeed(
        config=ClosedCandle15mFeedConfig(
            websocket_url="wss://example.test/market/ws",
            final_event_queue_size=1,
        )
    )
    first = ClosedCandle15mEvent(
        candle=ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=datetime(2026, 8, 24, 14, 30, tzinfo=UTC),
            candle_end=datetime(2026, 8, 24, 14, 45, tzinfo=UTC),
            open_price=Decimal("100"),
            close_price=Decimal("99"),
        ),
        exchange_event_at=RECEIVED_AT,
        received_at=RECEIVED_AT,
    )
    second = ClosedCandle15mEvent(
        candle=ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=datetime(2026, 8, 24, 14, 45, tzinfo=UTC),
            candle_end=datetime(2026, 8, 24, 15, 0, tzinfo=UTC),
            open_price=Decimal("99"),
            close_price=Decimal("98"),
        ),
        exchange_event_at=RECEIVED_AT,
        received_at=RECEIVED_AT,
    )

    assert await feed._publish(first) is True
    with pytest.raises(ClosedCandleFeedOverflow):
        await feed._publish(second)
    assert feed.last_seen_start["BTCUSDT"] == first.candle.candle_start

    await feed._events.get()
    assert await feed._publish(second) is True
