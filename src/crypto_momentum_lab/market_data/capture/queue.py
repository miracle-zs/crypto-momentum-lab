import asyncio
import json
import time
from collections.abc import Hashable
from dataclasses import dataclass
from datetime import UTC

from crypto_momentum_lab.domain.market.models import CaptureStream, RawEnvelope


class CaptureQueueFull(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _QueueItem:
    envelope: RawEnvelope
    encoded_size: int
    coalesce_key: Hashable | None


class BoundedEnvelopeQueue:
    """A bounded ingress queue with optional latest-value coalescing.

    Durable streams apply waitable backpressure when capacity is temporarily
    exhausted and only fail when one envelope can never fit. Realtime quote
    streams keep the latest value for each symbol/bucket. This keeps transport
    liveness independent from short downstream stalls without silently
    discarding loss-intolerant events.
    """

    def __init__(
        self,
        *,
        max_events: int,
        max_bytes: int,
        coalescing_streams: frozenset[CaptureStream] = frozenset(),
        coalescing_interval_seconds: float = 0.0,
    ) -> None:
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if coalescing_interval_seconds < 0:
            raise ValueError("coalescing_interval_seconds must be non-negative")
        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue(maxsize=max_events)
        self._max_events = max_events
        self._max_bytes = max_bytes
        self._coalescing_streams = coalescing_streams
        self._coalescing_interval_seconds = coalescing_interval_seconds
        self._pending_by_key: dict[Hashable, _QueueItem] = {}
        self._coalescing_buffers: dict[Hashable, _QueueItem] = {}
        self._coalescing_tasks: dict[Hashable, asyncio.Task[None]] = {}
        self._closed = False
        self._current_bytes = 0
        self._coalesced_replacements = 0
        self._dropped_events = 0
        self._high_watermark_events = 0
        self._high_watermark_bytes = 0
        self._backpressure_wait_count = 0
        self._backpressure_wait_seconds = 0.0
        self._waiting_producers = 0
        self._lock = asyncio.Lock()
        self._capacity_available = asyncio.Event()

    @property
    def current_bytes(self) -> int:
        return self._current_bytes

    @property
    def size(self) -> int:
        return self._queue.qsize() + len(self._coalescing_buffers)

    @property
    def pending_coalesced_events(self) -> int:
        return len(self._coalescing_buffers)

    @property
    def coalesced_replacements(self) -> int:
        return self._coalesced_replacements

    @property
    def dropped_events(self) -> int:
        return self._dropped_events

    @property
    def max_events(self) -> int:
        return self._max_events

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def high_watermark_events(self) -> int:
        return self._high_watermark_events

    @property
    def high_watermark_bytes(self) -> int:
        return self._high_watermark_bytes

    @property
    def backpressure_wait_count(self) -> int:
        return self._backpressure_wait_count

    @property
    def backpressure_wait_seconds(self) -> float:
        return self._backpressure_wait_seconds

    @property
    def waiting_producers(self) -> int:
        return self._waiting_producers

    async def put(self, envelope: RawEnvelope) -> None:
        """Wait for durable capacity while preserving quote coalescing."""
        item = _queue_item(envelope, streams=self._coalescing_streams)
        wait_started_at: float | None = None
        try:
            while True:
                async with self._lock:
                    unavailable_reason = self._try_enqueue_item_locked(item)
                    if unavailable_reason is None:
                        return
                    self._capacity_available.clear()
                    if wait_started_at is None:
                        wait_started_at = time.monotonic()
                        self._backpressure_wait_count += 1
                        self._waiting_producers += 1
                await self._capacity_available.wait()
        finally:
            if wait_started_at is not None:
                self._waiting_producers -= 1
                self._backpressure_wait_seconds += (
                    time.monotonic() - wait_started_at
                )

    async def put_nowait(self, envelope: RawEnvelope) -> None:
        item = _queue_item(envelope, streams=self._coalescing_streams)
        async with self._lock:
            unavailable_reason = self._try_enqueue_item_locked(item)
            if unavailable_reason is not None:
                raise CaptureQueueFull(unavailable_reason)

    def _try_enqueue_item_locked(self, item: _QueueItem) -> str | None:
        encoded_size = item.encoded_size
        coalesce_key = item.coalesce_key
        if self._closed:
            raise CaptureQueueFull("queue is closed")
        if encoded_size > self._max_bytes:
            if coalesce_key is not None:
                self._dropped_events += 1
                return None
            raise CaptureQueueFull("envelope exceeds queue byte limit")

        if coalesce_key is not None:
            buffered = self._coalescing_buffers.get(coalesce_key)
            if buffered is not None:
                if (
                    self._current_bytes
                    - buffered.encoded_size
                    + encoded_size
                    > self._max_bytes
                ):
                    self._dropped_events += 1
                    return None
                self._coalescing_buffers[coalesce_key] = item
                self._current_bytes += encoded_size - buffered.encoded_size
                self._coalesced_replacements += 1
                self._observe_high_watermark_locked()
                return None
            previous = self._pending_by_key.get(coalesce_key)
            if previous is not None:
                if (
                    self._current_bytes
                    - previous.encoded_size
                    + encoded_size
                    > self._max_bytes
                ):
                    self._dropped_events += 1
                    return None
                self._pending_by_key[coalesce_key] = item
                self._current_bytes += encoded_size - previous.encoded_size
                self._coalesced_replacements += 1
                self._observe_high_watermark_locked()
                return None
            if self._coalescing_interval_seconds > 0:
                if self.size >= self._max_events:
                    self._dropped_events += 1
                    return None
                if self._current_bytes + encoded_size > self._max_bytes:
                    self._dropped_events += 1
                    return None
                self._coalescing_buffers[coalesce_key] = item
                self._current_bytes += encoded_size
                self._observe_high_watermark_locked()
                self._coalescing_tasks[coalesce_key] = asyncio.create_task(
                    self._flush_coalesced(coalesce_key)
                )
                return None

        if self._queue.full():
            if coalesce_key is not None:
                self._dropped_events += 1
                return None
            return "queue event limit reached"
        if self._current_bytes + encoded_size > self._max_bytes:
            if coalesce_key is not None:
                self._dropped_events += 1
                return None
            return "queue byte limit reached"

        self._queue.put_nowait(item)
        if coalesce_key is not None:
            self._pending_by_key[coalesce_key] = item
        self._current_bytes += encoded_size
        self._observe_high_watermark_locked()
        return None

    async def get(self) -> RawEnvelope:
        envelope = self._take_item(await self._queue.get())
        self._capacity_available.set()
        return envelope

    def get_nowait(self) -> RawEnvelope | None:
        try:
            item = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        envelope = self._take_item(item)
        self._capacity_available.set()
        return envelope

    def task_done(self, envelope: RawEnvelope) -> None:
        self._current_bytes -= _encoded_payload_size(envelope)
        self._queue.task_done()
        self._capacity_available.set()

    async def join(self) -> None:
        await self._queue.join()

    async def close(self) -> None:
        """Stop delayed coalescing and flush its latest values into the queue."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._capacity_available.set()
            tasks = tuple(self._coalescing_tasks.values())
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with self._lock:
            buffered = tuple(self._coalescing_buffers.values())
            self._coalescing_buffers.clear()
            self._coalescing_tasks.clear()
            for item in buffered:
                self._enqueue_item_locked(item)

    def _take_item(self, item: _QueueItem) -> RawEnvelope:
        if item.coalesce_key is not None:
            latest = self._pending_by_key.pop(item.coalesce_key, item)
            item = latest
        return item.envelope

    async def _flush_coalesced(self, key: Hashable) -> None:
        try:
            await asyncio.sleep(self._coalescing_interval_seconds)
        except asyncio.CancelledError:
            return
        async with self._lock:
            item = self._coalescing_buffers.pop(key, None)
            self._coalescing_tasks.pop(key, None)
            if item is not None:
                self._enqueue_item_locked(item)
        self._capacity_available.set()

    def _enqueue_item_locked(self, item: _QueueItem) -> None:
        if item.encoded_size > self._max_bytes:
            if item.coalesce_key is not None:
                self._dropped_events += 1
                self._current_bytes -= item.encoded_size
                return
            raise CaptureQueueFull("envelope exceeds queue byte limit")
        if self._queue.full():
            if item.coalesce_key is not None:
                self._dropped_events += 1
                self._current_bytes -= item.encoded_size
                return
            raise CaptureQueueFull("queue event limit reached")
        if self._current_bytes > self._max_bytes:
            if item.coalesce_key is not None:
                self._dropped_events += 1
                self._current_bytes -= item.encoded_size
                return
            raise CaptureQueueFull("queue byte limit reached")
        self._queue.put_nowait(item)
        if item.coalesce_key is not None:
            self._pending_by_key[item.coalesce_key] = item
        self._observe_high_watermark_locked()

    def _observe_high_watermark_locked(self) -> None:
        self._high_watermark_events = max(
            self._high_watermark_events,
            self.size,
        )
        self._high_watermark_bytes = max(
            self._high_watermark_bytes,
            self._current_bytes,
        )


def _queue_item(
    envelope: RawEnvelope,
    *,
    streams: frozenset[CaptureStream],
) -> _QueueItem:
    return _QueueItem(
        envelope=envelope,
        encoded_size=_encoded_payload_size(envelope),
        coalesce_key=_coalesce_key(envelope, streams=streams),
    )


def _coalesce_key(
    envelope: RawEnvelope,
    *,
    streams: frozenset[CaptureStream],
) -> Hashable | None:
    if envelope.stream not in streams or envelope.symbol is None:
        return None
    observed_at = envelope.exchange_event_at or envelope.received_at
    utc_at = observed_at.astimezone(UTC)
    bucket_start = utc_at.replace(
        second=(utc_at.second // 15) * 15,
        microsecond=0,
    )
    return (
        envelope.environment,
        envelope.stream,
        envelope.symbol,
        bucket_start,
    )


def _encoded_payload_size(envelope: RawEnvelope) -> int:
    if envelope.raw_payload_size_bytes is not None:
        return envelope.raw_payload_size_bytes
    return len(
        json.dumps(
            envelope.raw_payload,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    )
