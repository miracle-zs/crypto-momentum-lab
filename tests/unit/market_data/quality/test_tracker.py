from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    QualityCategory,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.quality.tracker import (
    StreamQualityTracker,
)


def test_duplicate_exchange_sequence_is_recorded(
    raw_envelope: RawEnvelope,
) -> None:
    tracker = StreamQualityTracker()

    assert tracker.observe(raw_envelope) == ()
    events = tracker.observe(raw_envelope)

    assert [event.category for event in events] == [
        QualityCategory.DUPLICATE
    ]


def test_numeric_sequence_gap_is_recorded(
    raw_envelope: RawEnvelope,
) -> None:
    assert raw_envelope.symbol is not None
    tracker = StreamQualityTracker()
    tracker.observe(replace(raw_envelope, exchange_sequence="10"))

    events = tracker.observe(
        replace(
            raw_envelope,
            local_sequence=2,
            exchange_sequence="12",
        )
    )

    assert events[0].category is QualityCategory.SEQUENCE_GAP
    assert events[0].details == {"previous": "10", "current": "12"}
    assert (
        tracker.known_gap_count(
            connection_session_id=raw_envelope.connection_session_id,
            stream=raw_envelope.stream,
            symbol=raw_envelope.symbol,
        )
        == 1
    )


def test_event_time_regression_and_silence_are_recorded(
    raw_envelope: RawEnvelope,
) -> None:
    assert raw_envelope.exchange_event_at is not None
    tracker = StreamQualityTracker(silence_timeout_seconds=30)
    tracker.observe(raw_envelope)

    regression = tracker.observe(
        replace(
            raw_envelope,
            local_sequence=2,
            exchange_event_at=raw_envelope.exchange_event_at
            - timedelta(seconds=1),
        )
    )
    silence = tracker.check_silence(
        now=raw_envelope.received_at + timedelta(seconds=31)
    )

    assert regression[0].category is QualityCategory.EVENT_TIME_REGRESSION
    assert silence[0].category is QualityCategory.SILENCE


def test_connection_lifecycle_events_are_recorded() -> None:
    tracker = StreamQualityTracker()
    opened = ConnectionLifecycleEvent(
        session_id=UUID(int=1),
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbols=("BTCUSDT",),
        occurred_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        opened=True,
        reason=None,
    )
    closed = replace(opened, opened=False, reason="closed")

    events = (
        *tracker.observe_lifecycle(opened),
        *tracker.observe_lifecycle(closed),
    )

    assert [event.category for event in events] == [
        QualityCategory.CONNECTION_OPENED,
        QualityCategory.CONNECTION_CLOSED,
    ]
    assert events[1].details["reason"] == "closed"
