import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import crypto_momentum_lab.market_data.quote_hub as quote_hub_module
from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.market_data.quote_hub import (
    MarketQuoteHubConfig,
    WebSocketMarketQuoteSource,
    decode_market_quote,
    encode_market_quote,
)
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
)


class _Repository:
    def __init__(self) -> None:
        self.saved_states = []

    async def save_closed_states(
        self,
        states,
        *,
        source_watermark_at,
        sequence_range,
    ) -> None:
        self.saved_states.extend(states)

    async def mark_incomplete(self, gap) -> None:
        del gap


async def test_realtime_and_durable_clocks_are_independent() -> None:
    repository = _Repository()
    realtime_states = []

    async def realtime_sink(states) -> None:
        realtime_states.extend(states)

    publisher = ClosedMarketStatePublisher(
        repository=repository,
        realtime_state_sink=realtime_sink,
    )

    await publisher.observe(_trade(0, sequence=1, price="100"))
    await publisher.observe(_trade(16, sequence=2, price="101"))

    assert [state.bucket_start.second for state in realtime_states] == [0]
    assert repository.saved_states == []

    # This packet arrives after the 1s realtime close, but before the 3s
    # durable close, so it must still be incorporated into the audit state.
    await publisher.observe(_trade(0, sequence=3, price="99"))
    await publisher.observe(_trade(31, sequence=4, price="102"))

    durable_state = next(
        state for state in repository.saved_states if state.bucket_start.second == 0
    )
    assert durable_state.trade_count == 2
    assert publisher.metrics.late_event_count == 0


def test_quote_hub_round_trip() -> None:
    event_at = datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
    quote = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=20),
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
    )

    decoded = decode_market_quote(encode_market_quote(quote))

    assert decoded == quote


async def test_quote_source_reader_keeps_latest_quote_while_consumer_is_busy(
    monkeypatch,
) -> None:
    event_at = datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
    first = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=event_at,
        received_at=event_at,
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
    )
    second = replace_quote(first, bid_price=Decimal("102"), ask_price=Decimal("103"))
    second_received = asyncio.Event()
    idle = asyncio.Event()

    class FakeConnection:
        def __init__(self) -> None:
            self._messages = [
                json.dumps(
                    {
                        "type": "market_quote_hub_ready",
                        "schema_version": 1,
                        "environment": "research",
                    }
                ),
                encode_market_quote(first),
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
                return encode_market_quote(second)
            await idle.wait()
            raise OSError("simulated disconnect")

    monkeypatch.setattr(
        quote_hub_module,
        "connect",
        lambda *_args, **_kwargs: FakeConnection(),
    )
    source = WebSocketMarketQuoteSource(
        url="ws://unused",
        environment="research",
        consumer_id="test-live-quotes",
        config=MarketQuoteHubConfig(
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


def replace_quote(
    quote: RealtimeMarketQuote,
    *,
    bid_price: Decimal,
    ask_price: Decimal,
) -> RealtimeMarketQuote:
    return RealtimeMarketQuote(
        exchange=quote.exchange,
        environment=quote.environment,
        symbol=quote.symbol,
        event_at=quote.event_at,
        received_at=quote.received_at,
        bid_price=bid_price,
        ask_price=ask_price,
    )


def _trade(offset_seconds: int, *, sequence: int, price: str) -> RawEnvelope:
    event_at = datetime(2026, 8, 23, 0, 0, tzinfo=UTC) + timedelta(
        seconds=offset_seconds
    )
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        exchange_event_at=event_at,
        received_at=event_at,
        received_monotonic_ns=sequence,
        connection_session_id=UUID(int=1),
        local_sequence=sequence,
        exchange_sequence=str(sequence),
        subscription_generation=1,
        raw_payload={
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": sequence,
            "p": price,
            "q": "1",
            "T": int(event_at.timestamp() * 1000),
            "m": False,
        },
    )
