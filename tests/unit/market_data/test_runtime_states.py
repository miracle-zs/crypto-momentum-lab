from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
    ClosedMarketStatePublisherConfig,
)


class FakeRuntimeStateRepository:
    def __init__(self) -> None:
        self.saved_symbols: list[tuple[str, ...]] = []
        self.saved_states = []

    async def save_closed_states(
        self,
        states,
        *,
        source_watermark_at,
        sequence_range,
    ) -> None:
        self.saved_symbols.append(tuple(state.symbol for state in states))
        self.saved_states.extend(states)


async def test_publisher_closes_only_buckets_behind_watermark() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(1, price="101", sequence=2))
    await publisher.observe(fixture_trade(3, price="102", sequence=3))

    assert repository.saved_symbols == [("BTCUSDT", "BTCUSDT")]
    assert publisher.metrics.closed_state_count == 2


async def test_late_event_for_closed_bucket_is_rejected() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))
    await publisher.observe(fixture_trade(0, price="99", sequence=3))

    assert publisher.metrics.late_event_count == 1
    assert repository.saved_symbols == [("BTCUSDT",)]


async def test_late_event_for_previously_unseen_bucket_is_rejected() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))
    await publisher.observe(
        fixture_trade(0, price="99", sequence=3, symbol="ETHUSDT")
    )

    assert publisher.metrics.late_event_count == 1
    assert repository.saved_symbols == [("BTCUSDT",)]


async def test_publisher_carries_the_latest_book_quote_into_later_states() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_book_ticker(0, sequence=1))
    await publisher.observe(fixture_trade(1, price="101", sequence=2))
    await publisher.observe(fixture_trade(3, price="103", sequence=3))

    state = next(
        state
        for state in repository.saved_states
        if state.bucket_start == datetime(2026, 7, 3, 0, 0, 15, tzinfo=UTC)
    )
    assert state.last_bid_price == Decimal("99")
    assert state.last_ask_price == Decimal("101")
    assert state.midpoint == Decimal("100")
    assert state.spread == Decimal("2")


def fixture_trade(
    bucket_index: int,
    *,
    price: str,
    sequence: int,
    symbol: str = "BTCUSDT",
) -> RawEnvelope:
    event_at = datetime(2026, 7, 3, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol=symbol,
        exchange_event_at=event_at,
        received_at=event_at,
        received_monotonic_ns=sequence,
        connection_session_id=UUID(int=1),
        local_sequence=sequence,
        exchange_sequence=str(sequence),
        subscription_generation=1,
        raw_payload={
            "e": "aggTrade",
            "s": symbol,
            "a": sequence,
            "p": price,
            "q": "1",
            "T": int(event_at.timestamp() * 1000),
            "m": False,
        },
    )


def fixture_book_ticker(bucket_index: int, *, sequence: int) -> RawEnvelope:
    event_at = datetime(2026, 7, 3, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.PUBLIC,
        stream=CaptureStream.BOOK_TICKER,
        symbol="BTCUSDT",
        exchange_event_at=event_at,
        received_at=event_at,
        received_monotonic_ns=sequence,
        connection_session_id=UUID(int=1),
        local_sequence=sequence,
        exchange_sequence=str(sequence),
        subscription_generation=1,
        raw_payload={
            "e": "bookTicker",
            "E": int(event_at.timestamp() * 1000),
            "s": "BTCUSDT",
            "u": sequence,
            "b": "99",
            "B": "1",
            "a": "101",
            "A": "1",
        },
    )
