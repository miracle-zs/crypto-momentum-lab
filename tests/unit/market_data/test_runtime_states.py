import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

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
        self.saved_sequence_ranges = []
        self.incomplete_gaps = []

    async def save_closed_states(
        self,
        states,
        *,
        source_watermark_at,
        sequence_range,
    ) -> None:
        self.saved_symbols.append(tuple(state.symbol for state in states))
        self.saved_sequence_ranges.append(sequence_range)
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


async def test_set_expected_symbols_drops_last_state_for_removed_symbols() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    publisher.set_expected_symbols(frozenset({"BTCUSDT", "ETHUSDT"}))

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(0, price="200", sequence=2, symbol="ETHUSDT"))
    await publisher.observe(fixture_trade(3, price="101", sequence=3))

    assert ("research", "ETHUSDT") in publisher._last_state_by_symbol

    publisher.set_expected_symbols(frozenset({"BTCUSDT"}))

    assert ("research", "ETHUSDT") not in publisher._last_state_by_symbol
    assert ("research", "BTCUSDT") in publisher._last_state_by_symbol
    assert ("research", "ETHUSDT") not in (
        publisher._last_materialized_bucket_by_symbol
    )


async def test_publisher_materializes_empty_bucket_for_expected_quiet_symbol() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    publisher.set_expected_symbols(frozenset({"BTCUSDT"}))

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="102", sequence=2))

    states_by_bucket = {state.bucket_start: state for state in repository.saved_states}
    empty_state = states_by_bucket[datetime(2026, 7, 3, 0, 0, 15, tzinfo=UTC)]
    assert empty_state.source_event_count == 0
    assert empty_state.trade_count == 0
    assert empty_state.close_price == Decimal("100")
    assert empty_state.data_complete is True
    assert [
        (item.minimum, item.maximum) for item in repository.saved_sequence_ranges
    ] == [(1, 1), (None, None)]


async def test_quiet_symbol_fills_when_global_watermark_advances() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    publisher.set_expected_symbols(frozenset({"BTCUSDT", "ETHUSDT"}))

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(3, price="200", sequence=2, symbol="ETHUSDT"))

    btc_bucket_starts = sorted(
        state.bucket_start
        for state in repository.saved_states
        if state.symbol == "BTCUSDT"
    )
    assert btc_bucket_starts == [
        datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 3, 0, 0, 15, tzinfo=UTC),
    ]


def test_expected_symbols_reports_real_entries_only() -> None:
    """Entering the dense set is reported once; the startup baseline is not."""

    publisher = ClosedMarketStatePublisher(
        repository=FakeRuntimeStateRepository(),
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )

    # The first call is the startup baseline: nothing "just entered".
    publisher.set_expected_symbols(frozenset({"BTCUSDT"}))
    assert publisher.consume_pending_entry_symbols() == frozenset()

    # An unchanged set reports nothing.
    publisher.set_expected_symbols(frozenset({"BTCUSDT"}))
    assert publisher.consume_pending_entry_symbols() == frozenset()

    # A symbol joining is reported exactly once, then cleared.
    publisher.set_expected_symbols(frozenset({"BTCUSDT", "ETHUSDT"}))
    assert publisher.consume_pending_entry_symbols() == frozenset({"ETHUSDT"})
    assert publisher.consume_pending_entry_symbols() == frozenset()

    # A symbol leaving, or re-appearing after a leave, is not an entry.
    publisher.set_expected_symbols(frozenset({"ETHUSDT"}))
    assert publisher.consume_pending_entry_symbols() == frozenset()

    # Re-entering after leaving IS an entry -- this is the case that used to
    # look like lost buckets downstream.
    publisher.set_expected_symbols(frozenset({"ETHUSDT", "BTCUSDT"}))
    assert publisher.consume_pending_entry_symbols() == frozenset({"BTCUSDT"})


async def test_realtime_batch_announces_pending_entry_symbols_once() -> None:
    """The entry handover reaches the sink, and only on the first batch after."""

    batches: list[tuple[object, frozenset[str]]] = []

    async def realtime_sink(states, entered_symbols=frozenset()) -> None:
        batches.append((states, entered_symbols))

    publisher = ClosedMarketStatePublisher(
        repository=FakeRuntimeStateRepository(),
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
        realtime_state_sink=realtime_sink,
    )

    # Baseline, then ETHUSDT joins the dense set.
    publisher.set_expected_symbols(frozenset({"BTCUSDT"}))
    publisher.set_expected_symbols(frozenset({"BTCUSDT", "ETHUSDT"}))

    await publisher.observe(fixture_trade(0, price="100", sequence=1))
    await publisher.observe(fixture_trade(16, price="102", sequence=2))
    await publisher.observe(
        fixture_trade(16, price="200", sequence=3, symbol="ETHUSDT")
    )

    assert batches, "expected at least one realtime batch"
    announced = [entered for _states, entered in batches if "ETHUSDT" in entered]
    # Announced exactly once: it is consumed, not repeated on every batch.
    assert len(announced) == 1
    assert announced[0] == frozenset({"ETHUSDT"})


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
    await publisher.observe(fixture_trade(0, price="99", sequence=3, symbol="ETHUSDT"))

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
    assert repository.incomplete_gaps[0].reason == "late_recovery_after_durable_close"


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

    async def realtime_sink(states, entered_symbols=frozenset()) -> None:
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


async def test_snapshot_build_yields_when_closing_many_symbols() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(repository=repository)
    marker_ran = asyncio.Event()

    async def marker() -> None:
        marker_ran.set()

    marker_task = asyncio.create_task(marker())
    try:
        for index in range(8):
            await publisher.observe(
                fixture_trade(
                    0,
                    price="100",
                    sequence=index + 1,
                    symbol=f"COIN{index}USDT",
                )
            )
        await publisher.observe(
            fixture_trade(2, price="102", sequence=9, symbol="BTCUSDT")
        )
        assert marker_ran.is_set()
    finally:
        if not marker_task.done():
            marker_task.cancel()
        await asyncio.gather(marker_task, return_exceptions=True)


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


async def test_publisher_fails_closed_after_permanent_durable_write_error() -> None:
    class FailingRepository(FakeRuntimeStateRepository):
        async def save_closed_states(self, states, **kwargs) -> None:
            del states, kwargs
            raise ValueError("runtime market state conflict")

    publisher = ClosedMarketStatePublisher(
        repository=FailingRepository(),
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=1),
    )
    await publisher.start()
    try:
        await publisher.observe(fixture_trade(0, price="100", sequence=1))
        await publisher.observe(fixture_trade(2, price="102", sequence=2))
        await asyncio.sleep(0)

        with pytest.raises(
            RuntimeError,
            match="durable market-state persistence worker failed",
        ):
            await publisher.observe(fixture_trade(3, price="103", sequence=3))
    finally:
        await publisher.stop()


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
            exchange_event_at=second.exchange_event_at + timedelta(milliseconds=600),
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


async def test_through_bucket_gating_and_invalidation() -> None:
    repository = FakeRuntimeStateRepository()
    publisher = ClosedMarketStatePublisher(
        repository=repository,
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    publisher.set_expected_symbols(["BTCUSDT", "ETHUSDT"])
    base = datetime(2026, 7, 3, 0, 0, tzinfo=UTC)

    # Observe initial events for BTCUSDT to establish baseline
    await publisher.observe(fixture_trade(0, price="100", sequence=1, symbol="BTCUSDT"))
    await publisher.observe(fixture_trade(1, price="101", sequence=2, symbol="BTCUSDT"))
    assert publisher._last_materialized_empty_buckets_through is not None

    # Save through-bucket and verify gate skips re-scan
    through = publisher._last_materialized_empty_buckets_through
    scan_count = 0
    original_materialize = publisher._materialize_buckets_until

    def count_materialize(*args, **kwargs):
        nonlocal scan_count
        scan_count += 1
        return original_materialize(*args, **kwargs)

    publisher._materialize_buckets_until = count_materialize

    # Call with same watermark -> gate prevents scanning
    publisher._materialize_empty_buckets_through(base + timedelta(seconds=1))
    assert scan_count == 0

    # Invalidate by changing expected_symbols
    publisher.set_expected_symbols(["BTCUSDT", "SOLUSDT"])
    assert publisher._last_materialized_empty_buckets_through is None

    # Call with watermark -> gate allows scanning because it was invalidated
    publisher._materialize_empty_buckets_through(base + timedelta(seconds=15))
    assert scan_count > 0
    assert publisher._last_materialized_empty_buckets_through is not None

    # Invalidate by observing a newly seen symbol
    publisher._last_materialized_empty_buckets_through = base + timedelta(seconds=15)
    await publisher.observe(fixture_trade(2, price="200", sequence=3, symbol="SOLUSDT"))
    assert publisher._last_materialized_empty_buckets_through is None



async def test_materializing_many_buckets_looks_up_predecessor_once_per_symbol() -> None:
    """Filling B buckets must not rescan the bucket table once per bucket."""
    publisher = ClosedMarketStatePublisher(
        repository=FakeRuntimeStateRepository(),
        config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
    )
    base = datetime(2026, 7, 3, 0, 0, tzinfo=UTC)
    symbols = ["BTCUSDT", "ETHUSDT"]
    publisher.set_expected_symbols(symbols)
    publisher._observed_symbol_keys = {("research", symbol) for symbol in symbols}
    publisher._exchange_by_symbol_key = {
        ("research", symbol): "binance" for symbol in symbols
    }
    publisher._last_materialized_bucket_by_symbol = {
        ("research", symbol): base for symbol in symbols
    }

    lookups: list[tuple[str, str]] = []
    original = publisher._previous_state_for_symbol

    def counting_lookup(symbol_key, *, before_bucket):
        lookups.append(symbol_key)
        return original(symbol_key, before_bucket=before_bucket)

    publisher._previous_state_for_symbol = counting_lookup
    through = base + timedelta(seconds=15 * 5)
    for symbol in symbols:
        publisher._materialize_buckets_until(
            symbol_key=("research", symbol),
            through_bucket=through,
        )

    assert len(publisher._accumulators_by_bucket) == 2 * 5
    # One lookup per symbol instead of one per materialized bucket (10 buckets).
    assert lookups == [("research", "BTCUSDT"), ("research", "ETHUSDT")]


async def test_one_jump_fill_matches_bucket_by_bucket_fill() -> None:
    """Carrying the predecessor forward must not change the materialized state."""
    base = datetime(2026, 7, 3, 0, 0, tzinfo=UTC)
    symbol_key = ("research", "BTCUSDT")

    async def filled(step: int):
        publisher = ClosedMarketStatePublisher(
            repository=FakeRuntimeStateRepository(),
            config=ClosedMarketStatePublisherConfig(closure_delay_seconds=15),
        )
        publisher.set_expected_symbols(["BTCUSDT"])
        # Real ingest path so the predecessor state and the per-bucket book
        # quote cache are populated the way production populates them.
        await publisher.observe(fixture_book_ticker(0, sequence=1))
        await publisher.observe(fixture_trade(1, price="101", sequence=2))
        assert publisher._last_materialized_bucket_by_symbol.get(symbol_key) is not None

        lookups: list[tuple[str, str]] = []
        original = publisher._previous_state_for_symbol

        def counting_lookup(key, *, before_bucket):
            lookups.append(key)
            return original(key, before_bucket=before_bucket)

        publisher._previous_state_for_symbol = counting_lookup
        for index in range(step, 6, step):
            publisher._materialize_buckets_until(
                symbol_key=symbol_key,
                through_bucket=base + timedelta(seconds=15 * index),
            )
        return publisher, lookups

    one_jump, jump_lookups = await filled(5)
    stepped, stepped_lookups = await filled(1)

    assert one_jump._last_materialized_bucket_by_symbol == (
        stepped._last_materialized_bucket_by_symbol
    )
    assert set(one_jump._accumulators_by_bucket) == set(stepped._accumulators_by_bucket)
    for key, accumulator in one_jump._accumulators_by_bucket.items():
        assert accumulator.snapshot().state == (
            stepped._accumulators_by_bucket[key].snapshot().state
        )
    assert one_jump._realtime_deadlines == stepped._realtime_deadlines
    assert one_jump._durable_deadlines == stepped._durable_deadlines
    # The jump path looks the predecessor up once; the stepped path once per call.
    assert len(jump_lookups) == 1
    assert len(stepped_lookups) > 1
