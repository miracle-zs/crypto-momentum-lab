import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
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
        self.incomplete_gaps = []

    async def save_closed_states(
        self,
        states,
        *,
        source_watermark_at,
        sequence_range,
    ) -> None:
        self.saved_symbols.append(tuple(state.symbol for state in states))
        self.saved_states.extend(states)

    async def mark_incomplete(self, gap) -> None:
        self.incomplete_gaps.append(gap)


class OrderedRuntimeStateRepository(FakeRuntimeStateRepository):
    def __init__(self) -> None:
        super().__init__()
        self.operations: list[str] = []

    async def save_closed_states(self, states, **kwargs) -> None:
        self.operations.append("states")
        await super().save_closed_states(states, **kwargs)

    async def mark_incomplete(self, gap) -> None:
        self.operations.append("gap")
        await super().mark_incomplete(gap)


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


async def test_late_recovered_trade_marks_durable_bucket_incomplete() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))
    await publisher.observe(
        replace(
            fixture_trade(0, price="99", sequence=3),
            recovered=True,
        )
    )

    assert publisher.metrics.late_event_count == 1
    assert len(repository.incomplete_gaps) == 1
    assert (
        repository.incomplete_gaps[0].reason
        == "late_recovery_after_durable_close"
    )


async def test_gap_persistence_is_ordered_after_pending_state_insert() -> None:
    repository = OrderedRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    await publisher.start()
    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))
    start = datetime(2026, 7, 3, tzinfo=UTC)
    await publisher.mark_incomplete(
        AggTradeGap(
            environment="research",
            symbol="BTCUSDT",
            previous_id=10,
            current_id=12,
            previous_event_at=start,
            current_event_at=start + timedelta(seconds=1),
            missing_count=1,
            reason="history_incomplete",
        )
    )

    await publisher.stop()

    assert repository.operations == ["states", "gap"]


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


async def test_publisher_keeps_only_latest_book_quote_per_state_bucket() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    first = fixture_book_ticker(0, sequence=1)
    latest = replace(
        first,
        local_sequence=2,
        received_monotonic_ns=2,
        exchange_sequence="2",
        raw_payload={
            "e": "bookTicker",
            "E": int(first.exchange_event_at.timestamp() * 1000),
            "s": "BTCUSDT",
            "u": 2,
            "b": "100",
            "B": "1",
            "a": "102",
            "A": "1",
        },
    )

    await publisher.observe(first)
    await publisher.observe(latest)
    await publisher.observe(fixture_trade(1, price="101", sequence=3))
    await publisher.observe(fixture_trade(3, price="103", sequence=4))

    state = next(
        state
        for state in repository.saved_states
        if state.bucket_start == datetime(2026, 7, 3, 0, 0, tzinfo=UTC)
    )
    assert state.last_bid_price == Decimal("100")
    assert state.last_ask_price == Decimal("102")


async def test_publisher_fanout_happens_before_durable_runtime_state_write() -> None:
    repository = FakeRuntimeStateRepository()
    events: list[str] = []

    async def realtime_sink(states) -> None:
        assert states
        events.append("realtime")

    original_save = repository.save_closed_states

    async def save_closed_states(*args, **kwargs) -> None:
        events.append("durable")
        await original_save(*args, **kwargs)

    repository.save_closed_states = save_closed_states
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        realtime_state_sink=realtime_sink,
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(1, price="101", sequence=2))
    await publisher.observe(fixture_trade(3, price="102", sequence=3))

    assert events == ["realtime", "durable"]
    assert publisher.metrics.realtime_batch_count == 1
    assert publisher.metrics.realtime_sink_failure_count == 0


async def test_publisher_durable_write_runs_behind_realtime_fanout() -> None:
    class BlockingRepository(FakeRuntimeStateRepository):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def save_closed_states(
            self,
            states,
            *,
            source_watermark_at,
            sequence_range,
        ) -> None:
            self.started.set()
            await self.release.wait()
            await super().save_closed_states(
                states,
                source_watermark_at=source_watermark_at,
                sequence_range=sequence_range,
            )

    repository = BlockingRepository()
    publisher = ClosedMarketStatePublisher(repository=repository)
    await publisher.start()
    try:
        await publisher.observe(fixture_trade(0, price="100", sequence=1))
        await publisher.observe(fixture_trade(1, price="101", sequence=2))
        await publisher.observe(fixture_trade(3, price="102", sequence=3))
        await asyncio.wait_for(repository.started.wait(), timeout=1)

        assert repository.saved_states == []
        assert publisher.metrics.durable_queue_size == 0
    finally:
        repository.release.set()
        await publisher.stop()

    assert len(repository.saved_states) == 2


async def test_publisher_reports_transport_lateness_and_close_thresholds() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(repository=repository)

    first = fixture_trade(0, price="100", sequence=1)
    await publisher.observe(
        replace(
            first,
            received_at=first.received_at + timedelta(milliseconds=750),
        )
    )
    second = fixture_trade(1, price="101", sequence=2)
    await publisher.observe(
        replace(
            second,
            exchange_event_at=second.exchange_event_at + timedelta(
                milliseconds=600
            ),
        )
    )
    await publisher.observe(fixture_trade(0, price="99", sequence=3))

    summary = publisher.lateness_metrics_snapshot()
    stream = summary["streams"][CaptureStream.AGG_TRADE.value]

    assert stream["raw_event_count"] == 3
    assert stream["timestamped_event_count"] == 3
    assert stream["received_over_threshold_count"]["0.5"] == 1
    assert stream["received_over_threshold_count"]["1"] == 0
    assert stream["simulated_close_drop_count"]["0.5"] == 1
    assert stream["simulated_close_drop_count"]["1"] == 0
    assert stream["simulated_close_drop_count"]["2"] == 0
    assert stream["simulated_close_drop_count"]["3"] == 0
    assert summary["aggregation"]["processing_count"] == 3
    assert summary["aggregation"]["processing_max_ms"] >= 0


async def test_publisher_marks_gap_buckets_incomplete() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    start = datetime(2026, 7, 3, 0, 0, 2, tzinfo=UTC)
    await publisher.mark_incomplete(
        AggTradeGap(
            environment="research",
            symbol="BTCUSDT",
            previous_id=10,
            current_id=13,
            previous_event_at=start,
            current_event_at=start + timedelta(seconds=2),
            missing_count=2,
            reason="history_incomplete",
        )
    )

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))

    state = repository.saved_states[0]
    assert state.data_complete is False
    assert state.missing_agg_trade_count == 2
    assert publisher.metrics.incomplete_gap_count == 1
    assert publisher.metrics.missing_agg_trade_count == 2


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
