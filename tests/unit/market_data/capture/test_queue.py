import json

import pytest

from crypto_momentum_lab.domain.market.models import RawEnvelope
from crypto_momentum_lab.market_data.capture.queue import (
    BoundedEnvelopeQueue,
    CaptureQueueFull,
)


async def test_queue_enforces_event_and_byte_limits(
    raw_envelope: RawEnvelope,
) -> None:
    queue = BoundedEnvelopeQueue(max_events=1, max_bytes=100000)
    await queue.put_nowait(raw_envelope)

    with pytest.raises(CaptureQueueFull):
        await queue.put_nowait(raw_envelope)

    item = await queue.get()
    assert item == raw_envelope
    queue.task_done(item)
    assert queue.current_bytes == 0


async def test_queue_rejects_single_oversized_envelope(
    raw_envelope: RawEnvelope,
) -> None:
    size = len(
        json.dumps(
            raw_envelope.raw_payload,
            separators=(",", ":"),
        ).encode()
    )
    queue = BoundedEnvelopeQueue(max_events=10, max_bytes=size - 1)

    with pytest.raises(CaptureQueueFull, match="byte limit"):
        await queue.put_nowait(raw_envelope)
