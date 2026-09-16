"""Throttle high-volume market-data quality events before they hit PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    QualityCategory,
    QualityEvent,
)
from crypto_momentum_lab.persistence.postgres.capture_repository import (
    PostgresCaptureRepository,
)


def _event(
    *,
    category: QualityCategory,
    occurred_at: datetime,
    stream: CaptureStream | None = CaptureStream.AGG_TRADE,
    symbol: str = "BTCUSDT",
) -> QualityEvent:
    return QualityEvent(
        event_id=f"{category}-{symbol}-{occurred_at.isoformat()}",
        category=category,
        occurred_at=occurred_at,
        route=CaptureRoute.MARKET,
        stream=stream,
        symbol=symbol,
        connection_session_id=None,
        local_sequence=None,
        details={},
    )


def test_gap_events_are_throttled_per_stream() -> None:
    repo = PostgresCaptureRepository(session_factory=None)  # type: ignore[arg-type]
    t0 = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
    events = [
        _event(
            category=QualityCategory.RECONNECT_GAP,
            occurred_at=t0 + timedelta(seconds=i),
            symbol=f"S{i}USDT",
        )
        for i in range(20)
    ]
    kept = repo._throttle_quality_events(events)
    assert len(kept) == 1
    assert kept[0].symbol == "S0USDT"


def test_gap_events_pass_again_after_window() -> None:
    repo = PostgresCaptureRepository(session_factory=None)  # type: ignore[arg-type]
    t0 = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
    first = repo._throttle_quality_events(
        [_event(category=QualityCategory.SEQUENCE_GAP, occurred_at=t0)]
    )
    too_soon = repo._throttle_quality_events(
        [
            _event(
                category=QualityCategory.SEQUENCE_GAP,
                occurred_at=t0 + timedelta(minutes=4),
            )
        ]
    )
    later = repo._throttle_quality_events(
        [
            _event(
                category=QualityCategory.SEQUENCE_GAP,
                occurred_at=t0 + timedelta(minutes=6),
            )
        ]
    )
    assert len(first) == 1
    assert too_soon == []
    assert len(later) == 1


def test_connection_lifecycle_is_not_throttled() -> None:
    repo = PostgresCaptureRepository(session_factory=None)  # type: ignore[arg-type]
    t0 = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
    events = [
        _event(
            category=QualityCategory.CONNECTION_CLOSED,
            occurred_at=t0 + timedelta(seconds=i),
            stream=CaptureStream.AGG_TRADE,
        )
        for i in range(5)
    ]
    kept = repo._throttle_quality_events(events)
    assert len(kept) == 5
