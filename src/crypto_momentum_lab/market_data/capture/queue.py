import asyncio
import json

from crypto_momentum_lab.domain.market.models import RawEnvelope


class CaptureQueueFull(RuntimeError):
    pass


class BoundedEnvelopeQueue:
    def __init__(self, *, max_events: int, max_bytes: int) -> None:
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._queue: asyncio.Queue[tuple[RawEnvelope, int]] = asyncio.Queue(
            maxsize=max_events
        )
        self._max_bytes = max_bytes
        self._current_bytes = 0
        self._lock = asyncio.Lock()

    @property
    def current_bytes(self) -> int:
        return self._current_bytes

    @property
    def size(self) -> int:
        return self._queue.qsize()

    async def put_nowait(self, envelope: RawEnvelope) -> None:
        encoded_size = _encoded_payload_size(envelope)
        async with self._lock:
            if encoded_size > self._max_bytes:
                raise CaptureQueueFull("envelope exceeds queue byte limit")
            if self._queue.full():
                raise CaptureQueueFull("queue event limit reached")
            if self._current_bytes + encoded_size > self._max_bytes:
                raise CaptureQueueFull("queue byte limit reached")
            self._queue.put_nowait((envelope, encoded_size))
            self._current_bytes += encoded_size

    async def get(self) -> RawEnvelope:
        envelope, _ = await self._queue.get()
        return envelope

    def task_done(self, envelope: RawEnvelope) -> None:
        self._current_bytes -= _encoded_payload_size(envelope)
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()


def _encoded_payload_size(envelope: RawEnvelope) -> int:
    return len(
        json.dumps(
            envelope.raw_payload,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    )
