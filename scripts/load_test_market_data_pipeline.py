"""Deterministic burst test for the in-process market-data pipeline."""

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureRoute,
    CaptureStream,
    DurableArchiveAcknowledgement,
    MarketDataState,
    MarketState15s,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.agg_trade_recovery import (
    AggTradeGapRecoverer,
)
from crypto_momentum_lab.market_data.binance.rest import BinanceAggTrade
from crypto_momentum_lab.market_data.capture.coordinator import CaptureCoordinator
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue
from crypto_momentum_lab.market_data.quality.tracker import StreamQualityTracker
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
    ClosedMarketStatePublisherConfig,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateSequenceRange,
)


@dataclass(frozen=True, slots=True)
class LoadTestReport:
    event_count: int
    elapsed_seconds: float
    events_per_second: float
    queue_high_watermark_events: int
    queue_backpressure_wait_count: int
    queue_backpressure_wait_seconds: float
    active_bucket_high_watermark: int
    aggregation_average_ms: float
    aggregation_max_ms: float
    recovered_gap_count: int
    unrecovered_gap_count: int
    queue_dropped_events: int
    quality_event_count: int


class _NoopArchive:
    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement:
        raise AssertionError(f"unexpected archive append: {envelope.stream}")

    async def close(self) -> None:
        return None


class _NoopRepository:
    def __init__(self) -> None:
        self.quality_event_count = 0

    async def save_quality_event(self, event: QualityEvent) -> None:
        del event
        self.quality_event_count += 1

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None:
        del state, occurred_at, reason


class _NoopRuntimeStateRepository:
    async def save_closed_states(
        self,
        states: tuple[MarketState15s, ...],
        *,
        source_watermark_at: datetime,
        sequence_range: RuntimeStateSequenceRange,
    ) -> None:
        del states, source_watermark_at, sequence_range

    async def mark_incomplete(self, gap: AggTradeGap) -> None:
        del gap


class _UnexpectedHistory:
    async def fetch_agg_trades(
        self,
        symbol: str,
        *,
        from_id: int,
        limit: int,
    ) -> tuple[BinanceAggTrade, ...]:
        raise AssertionError(
            f"unexpected aggTrade gap for {symbol}: {from_id=} {limit=}"
        )


async def run_load_test(
    *,
    event_count: int,
    target_events_per_second: float,
    symbol_count: int,
    queue_max_events: int,
) -> LoadTestReport:
    if event_count <= 0:
        raise ValueError("event_count must be positive")
    if target_events_per_second <= 0:
        raise ValueError("target_events_per_second must be positive")
    if symbol_count <= 0:
        raise ValueError("symbol_count must be positive")
    if queue_max_events <= 0:
        raise ValueError("queue_max_events must be positive")

    queue = BoundedEnvelopeQueue(
        max_events=queue_max_events,
        max_bytes=512 * 1024 * 1024,
    )
    capture_repository = _NoopRepository()
    state_publisher = ClosedMarketStatePublisher(
        repository=_NoopRuntimeStateRepository(),
        config=ClosedMarketStatePublisherConfig(
            realtime_closure_delay_seconds=1,
            durable_closure_delay_seconds=3,
        ),
    )
    recovery = AggTradeGapRecoverer(_UnexpectedHistory())
    coordinator = CaptureCoordinator(
        queue=queue,
        archive=_NoopArchive(),
        quality=StreamQualityTracker(silence_timeout_seconds=30),
        repository=capture_repository,
        realtime_envelope_sink=state_publisher.observe,
        envelope_recovery=recovery,
        gap_sink=state_publisher.mark_incomplete,
        archive_streams=frozenset({CaptureStream.FORCE_ORDER}),
        max_archive_batch_size=1000,
    )
    symbols = tuple(f"LOAD{index:03d}USDT" for index in range(symbol_count))
    base_at = datetime(2026, 8, 24, tzinfo=UTC)
    session_id = UUID(int=1)
    run_task = asyncio.create_task(coordinator.run())
    started_at = time.perf_counter()
    try:
        for index in range(event_count):
            symbol_index = index % symbol_count
            trade_id = index // symbol_count + 1
            event_at = base_at + timedelta(milliseconds=index / 10)
            await coordinator.submit(
                _agg_trade_envelope(
                    symbol=symbols[symbol_index],
                    trade_id=trade_id,
                    local_sequence=index + 1,
                    event_at=event_at,
                    session_id=session_id,
                )
            )
        await queue.join()
    finally:
        await coordinator.stop()
        await run_task
    elapsed_seconds = time.perf_counter() - started_at
    publisher_metrics = state_publisher.metrics
    recovery_metrics = recovery.metrics
    report = LoadTestReport(
        event_count=event_count,
        elapsed_seconds=elapsed_seconds,
        events_per_second=event_count / elapsed_seconds,
        queue_high_watermark_events=queue.high_watermark_events,
        queue_backpressure_wait_count=queue.backpressure_wait_count,
        queue_backpressure_wait_seconds=queue.backpressure_wait_seconds,
        active_bucket_high_watermark=(
            publisher_metrics.active_bucket_high_watermark
        ),
        aggregation_average_ms=(
            publisher_metrics.aggregation_processing_average_ms
        ),
        aggregation_max_ms=publisher_metrics.aggregation_processing_max_ms,
        recovered_gap_count=recovery_metrics.recovered_gap_count,
        unrecovered_gap_count=recovery_metrics.unrecovered_gap_count,
        queue_dropped_events=queue.dropped_events,
        quality_event_count=capture_repository.quality_event_count,
    )
    _assert_acceptance(
        report,
        target_events_per_second=target_events_per_second,
        maximum_active_buckets=symbol_count * 3,
    )
    return report


def _agg_trade_envelope(
    *,
    symbol: str,
    trade_id: int,
    local_sequence: int,
    event_at: datetime,
    session_id: UUID,
) -> RawEnvelope:
    event_ms = int(event_at.timestamp() * 1000)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="load-test",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol=symbol,
        exchange_event_at=event_at,
        received_at=event_at,
        received_monotonic_ns=local_sequence,
        connection_session_id=session_id,
        local_sequence=local_sequence,
        exchange_sequence=str(trade_id),
        subscription_generation=1,
        raw_payload={
            "e": "aggTrade",
            "E": event_ms,
            "s": symbol,
            "a": trade_id,
            "p": "100",
            "q": "1",
            "f": trade_id,
            "l": trade_id,
            "T": event_ms,
            "m": False,
        },
    )


def _assert_acceptance(
    report: LoadTestReport,
    *,
    target_events_per_second: float,
    maximum_active_buckets: int,
) -> None:
    failures: list[str] = []
    if report.events_per_second < target_events_per_second:
        failures.append(
            f"throughput {report.events_per_second:.0f}/s is below "
            f"{target_events_per_second:.0f}/s"
        )
    if report.queue_dropped_events:
        failures.append(f"queue dropped {report.queue_dropped_events} events")
    if report.unrecovered_gap_count:
        failures.append(
            f"found {report.unrecovered_gap_count} unrecovered aggTrade gaps"
        )
    if report.quality_event_count:
        failures.append(f"emitted {report.quality_event_count} quality events")
    if report.active_bucket_high_watermark > maximum_active_buckets:
        failures.append(
            "active aggregation buckets exceeded bounded target: "
            f"{report.active_bucket_high_watermark} > {maximum_active_buckets}"
        )
    if failures:
        raise RuntimeError("; ".join(failures))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load-test the market-data in-process pipeline",
    )
    parser.add_argument("--events", type=int, default=60_000)
    parser.add_argument("--target-eps", type=float, default=10_000)
    parser.add_argument("--symbols", type=int, default=64)
    parser.add_argument("--queue-events", type=int, default=25_000)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    report = await run_load_test(
        event_count=args.events,
        target_events_per_second=args.target_eps,
        symbol_count=args.symbols,
        queue_max_events=args.queue_events,
    )
    rendered = json.dumps(asdict(report), indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(_main())
