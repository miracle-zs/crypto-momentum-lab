import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
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


async def test_queue_put_waits_for_capacity_without_dropping(
    raw_envelope: RawEnvelope,
) -> None:
    queue = BoundedEnvelopeQueue(max_events=1, max_bytes=100000)
    second = replace(raw_envelope, local_sequence=2)
    await queue.put(raw_envelope)

    blocked_put = asyncio.create_task(queue.put(second))
    await asyncio.sleep(0)
    assert not blocked_put.done()
    assert queue.backpressure_wait_count == 1
    assert queue.waiting_producers == 1
    assert queue.high_watermark_events == 1

    first = await queue.get()
    queue.task_done(first)
    await asyncio.wait_for(blocked_put, timeout=1)

    queued_second = await queue.get()
    assert queued_second == second
    queue.task_done(queued_second)
    assert queue.waiting_producers == 0
    assert queue.backpressure_wait_seconds >= 0


async def test_queue_coalesces_realtime_book_ticker_within_bucket(
    raw_envelope: RawEnvelope,
) -> None:
    queue = BoundedEnvelopeQueue(
        max_events=1,
        max_bytes=100000,
        coalescing_streams=frozenset({CaptureStream.BOOK_TICKER}),
    )
    first = replace(
        raw_envelope,
        route=CaptureRoute.PUBLIC,
        stream=CaptureStream.BOOK_TICKER,
        exchange_sequence="1",
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "u": 1,
            "b": "100",
            "a": "101",
        },
    )
    latest = replace(
        first,
        local_sequence=2,
        exchange_sequence="2",
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "u": 2,
            "b": "102",
            "a": "103",
        },
    )

    await queue.put_nowait(first)
    await queue.put_nowait(latest)

    assert queue.size == 1
    item = await queue.get()
    assert item.raw_payload == latest.raw_payload
    queue.task_done(item)


async def test_queue_drops_realtime_book_ticker_when_new_bucket_is_full(
    raw_envelope: RawEnvelope,
) -> None:
    queue = BoundedEnvelopeQueue(
        max_events=1,
        max_bytes=100000,
        coalescing_streams=frozenset({CaptureStream.BOOK_TICKER}),
    )
    first = replace(
        raw_envelope,
        route=CaptureRoute.PUBLIC,
        stream=CaptureStream.BOOK_TICKER,
        exchange_sequence="1",
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "u": 1,
            "b": "100",
            "a": "101",
        },
    )
    next_bucket = replace(
        first,
        exchange_event_at=first.exchange_event_at + timedelta(seconds=15),
        local_sequence=2,
        exchange_sequence="2",
    )

    await queue.put_nowait(first)
    await queue.put_nowait(next_bucket)

    assert queue.dropped_events == 1
    item = await queue.get()
    assert item == first
    queue.task_done(item)


async def test_queue_delays_realtime_book_ticker_and_emits_latest_value(
    raw_envelope: RawEnvelope,
) -> None:
    queue = BoundedEnvelopeQueue(
        max_events=10,
        max_bytes=100000,
        coalescing_streams=frozenset({CaptureStream.BOOK_TICKER}),
        coalescing_interval_seconds=0.01,
    )
    first = replace(
        raw_envelope,
        route=CaptureRoute.PUBLIC,
        stream=CaptureStream.BOOK_TICKER,
        exchange_sequence="1",
        raw_payload={"e": "bookTicker", "s": "BTCUSDT", "u": 1},
    )
    latest = replace(
        first,
        local_sequence=2,
        exchange_sequence="2",
        raw_payload={"e": "bookTicker", "s": "BTCUSDT", "u": 2},
    )

    await queue.put_nowait(first)
    await queue.put_nowait(latest)
    assert queue.pending_coalesced_events == 1
    assert queue.coalesced_replacements == 1

    await asyncio.sleep(0.02)
    item = await queue.get()
    assert item.raw_payload == latest.raw_payload
    queue.task_done(item)
    await queue.close()
