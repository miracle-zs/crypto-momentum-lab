import asyncio
import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureStream,
    ConnectionLifecycleEvent,
    DurableArchiveAcknowledgement,
    MarketDataState,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.domain.market.ports import RawArchive
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue


class QualityTracker(Protocol):
    def set_monitored_symbols(self, symbols: frozenset[str]) -> None: ...

    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]: ...

    def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> tuple[QualityEvent, ...]: ...


class QualityRepository(Protocol):
    async def save_quality_event(self, event: QualityEvent) -> None: ...

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None: ...


type AcknowledgementSink = Callable[[DurableArchiveAcknowledgement], object]
type EnvelopeSink = Callable[[RawEnvelope], object]
type GapSink = Callable[[AggTradeGap], object]


class EnvelopeRecovery(Protocol):
    def set_monitored_symbols(self, symbols: frozenset[str]) -> None: ...

    async def expand(
        self,
        batch: tuple[RawEnvelope, ...],
    ) -> "EnvelopeRecoveryBatch": ...


class EnvelopeRecoveryBatch(Protocol):
    @property
    def envelopes(self) -> tuple[RawEnvelope, ...]: ...

    @property
    def unrecovered_gaps(self) -> tuple[AggTradeGap, ...]: ...

_DEFAULT_MAX_ARCHIVE_BATCH_SIZE = 1000


class CaptureCoordinator:
    def __init__(
        self,
        *,
        queue: BoundedEnvelopeQueue,
        archive: RawArchive,
        quality: QualityTracker,
        repository: QualityRepository,
        acknowledgement_sink: AcknowledgementSink | None = None,
        realtime_envelope_sink: EnvelopeSink | None = None,
        archived_envelope_sink: EnvelopeSink | None = None,
        envelope_recovery: EnvelopeRecovery | None = None,
        gap_sink: GapSink | None = None,
        archive_streams: frozenset[CaptureStream] | None = None,
        max_archive_batch_size: int = _DEFAULT_MAX_ARCHIVE_BATCH_SIZE,
    ) -> None:
        if max_archive_batch_size <= 0:
            raise ValueError("max_archive_batch_size must be positive")
        self._queue = queue
        self._archive = archive
        self._quality = quality
        self._repository = repository
        self._acknowledgement_sink = acknowledgement_sink
        self._realtime_envelope_sink = realtime_envelope_sink
        self._archived_envelope_sink = archived_envelope_sink
        self._envelope_recovery = envelope_recovery
        self._gap_sink = gap_sink
        self._archive_streams = archive_streams
        self._max_archive_batch_size = max_archive_batch_size
        self._stopping = False
        self._monitored_symbols: frozenset[str] | None = None
        self._filtered_book_ticker_events = 0

    @property
    def filtered_book_ticker_events(self) -> int:
        return self._filtered_book_ticker_events

    def set_monitored_symbols(self, symbols: frozenset[str]) -> None:
        """Bound ingress and continuity state to the active symbol set."""
        normalized = frozenset(
            symbol.upper() for symbol in symbols
        )
        self._monitored_symbols = normalized
        self._quality.set_monitored_symbols(normalized)
        if self._envelope_recovery is not None:
            self._envelope_recovery.set_monitored_symbols(normalized)

    def accepts_symbol(self, stream: CaptureStream, symbol: str) -> bool:
        if (
            self._monitored_symbols is not None
            and symbol.upper() not in self._monitored_symbols
        ):
            if stream is CaptureStream.BOOK_TICKER:
                self._filtered_book_ticker_events += 1
            return False
        return True

    async def submit(self, envelope: RawEnvelope) -> None:
        if (
            envelope.symbol is not None
            and not self.accepts_symbol(envelope.stream, envelope.symbol)
        ):
            return
        await self._queue.put(envelope)

    async def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> None:
        for quality_event in self._quality.observe_lifecycle(event):
            await self._repository.save_quality_event(quality_event)

    async def run(self) -> None:
        while not self._stopping or self._queue.size:
            try:
                envelope = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            batch = [envelope]
            while len(batch) < self._max_archive_batch_size:
                next_envelope = self._queue.get_nowait()
                if next_envelope is None:
                    break
                batch.append(next_envelope)
            await self._process_batch(tuple(batch))

    async def _process_batch(self, batch: tuple[RawEnvelope, ...]) -> None:
        try:
            processing_batch = batch
            if self._envelope_recovery is not None:
                recovery = await self._envelope_recovery.expand(batch)
                processing_batch = recovery.envelopes
                await self._publish_gaps(recovery.unrecovered_gaps)

            await self._publish_batch(
                self._realtime_envelope_sink,
                processing_batch,
            )
            tasks = tuple(
                asyncio.create_task(self._process_envelope(envelope))
                for envelope in processing_batch
                if self._requires_side_effect_processing(envelope)
            )
            results = (
                await asyncio.gather(*tasks, return_exceptions=True)
                if tasks
                else ()
            )

            failures = tuple(
                result for result in results if isinstance(result, Exception)
            )
            if failures:
                reason = f"archive failure: {failures[0]}"
                await self._halt(reason)
                raise failures[0]

            await self._publish_batch(
                self._archived_envelope_sink,
                processing_batch,
            )
        finally:
            for envelope in batch:
                self._queue.task_done(envelope)

    async def _process_envelope(self, envelope: RawEnvelope) -> None:
        quality_events = self._quality.observe(envelope)
        should_archive = (
            self._archive_streams is None
            or envelope.stream in self._archive_streams
        )
        acknowledgement = (
            await self._archive.append(envelope) if should_archive else None
        )
        for event in quality_events:
            await self._repository.save_quality_event(event)
        if (
            acknowledgement is not None
            and self._acknowledgement_sink is not None
        ):
            result = self._acknowledgement_sink(acknowledgement)
            if inspect.isawaitable(result):
                await result

    def _requires_side_effect_processing(self, envelope: RawEnvelope) -> bool:
        # bookTicker is a realtime-only latest-value stream in the server
        # profile. It has no durable archive and no useful sequence-gap
        # invariant for the strategy, so don't create a task or serialize a
        # quality payload for every quote. The realtime publisher has already
        # consumed the coalesced envelope above.
        return not (
            envelope.stream is CaptureStream.BOOK_TICKER
            and self._archive_streams is not None
            and envelope.stream not in self._archive_streams
        )

    async def _publish_batch(
        self,
        sink: EnvelopeSink | None,
        batch: tuple[RawEnvelope, ...],
    ) -> None:
        if sink is None:
            return
        for envelope in batch:
            result = sink(envelope)
            if inspect.isawaitable(result):
                await result

    async def _publish_gaps(self, gaps: tuple[AggTradeGap, ...]) -> None:
        if self._gap_sink is None:
            return
        for gap in gaps:
            result = self._gap_sink(gap)
            if inspect.isawaitable(result):
                await result

    async def stop(self) -> None:
        self._stopping = True
        await self._queue.close()
        await self._queue.join()
        await self._archive.close()

    async def _halt(self, reason: str) -> None:
        await self._repository.save_process_state(
            state=MarketDataState.HALTED,
            occurred_at=datetime.now(UTC),
            reason=reason,
        )
