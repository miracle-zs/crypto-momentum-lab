import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    JsonValue,
    QualityCategory,
    QualityEvent,
    RawEnvelope,
)

type _StateKey = tuple[UUID, CaptureStream, str]


@dataclass(slots=True)
class _ObservedState:
    route: CaptureRoute
    last_exchange_sequence: str | None
    last_exchange_event_at: datetime | None
    last_received_at: datetime
    last_payload: bytes
    silence_reported: bool = False


class StreamQualityTracker:
    def __init__(self, *, silence_timeout_seconds: float = 30) -> None:
        self._silence_timeout_seconds = silence_timeout_seconds
        self._observed: dict[_StateKey, _ObservedState] = {}
        self._known_gap_counts: dict[_StateKey, int] = {}

    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]:
        symbol = envelope.symbol or "_global"
        key = (envelope.connection_session_id, envelope.stream, symbol)
        previous = self._observed.get(key)
        current_payload = _payload_bytes(envelope)
        events: list[QualityEvent] = []

        if previous is not None:
            if (
                previous.last_exchange_event_at is not None
                and envelope.exchange_event_at is not None
                and envelope.exchange_event_at < previous.last_exchange_event_at
            ):
                events.append(
                    self._quality_event(
                        QualityCategory.EVENT_TIME_REGRESSION,
                        envelope,
                        {
                            "previous": previous.last_exchange_event_at.isoformat(),
                            "current": envelope.exchange_event_at.isoformat(),
                        },
                    )
                )

            if (
                envelope.stream is CaptureStream.AGG_TRADE
                and envelope.exchange_sequence is not None
                and envelope.exchange_sequence == previous.last_exchange_sequence
                and current_payload == previous.last_payload
            ):
                events.append(
                    self._quality_event(
                        QualityCategory.DUPLICATE,
                        envelope,
                        {"exchange_sequence": envelope.exchange_sequence},
                    )
                )

            gap = _agg_trade_gap(
                previous.last_exchange_sequence,
                envelope.exchange_sequence,
                envelope.stream,
            )
            if gap is not None:
                events.append(
                    self._quality_event(
                        QualityCategory.SEQUENCE_GAP,
                        envelope,
                        {
                            "previous": previous.last_exchange_sequence,
                            "current": envelope.exchange_sequence,
                        },
                    )
                )
                self._known_gap_counts[key] = (
                    self._known_gap_counts.get(key, 0) + gap
                )
            elif (
                envelope.stream is not CaptureStream.KLINE_1M
                and envelope.stream is not CaptureStream.AGG_TRADE
                and current_payload == previous.last_payload
            ):
                events.append(
                    self._quality_event(
                        QualityCategory.DUPLICATE,
                        envelope,
                        {"stream": envelope.stream.value},
                    )
                )

        self._observed[key] = _ObservedState(
            route=envelope.route,
            last_exchange_sequence=envelope.exchange_sequence,
            last_exchange_event_at=envelope.exchange_event_at,
            last_received_at=envelope.received_at,
            last_payload=current_payload,
            silence_reported=False,
        )
        return tuple(events)

    def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> tuple[QualityEvent, ...]:
        category = (
            QualityCategory.CONNECTION_OPENED
            if event.opened
            else QualityCategory.CONNECTION_CLOSED
        )
        symbol = event.symbols[0] if len(event.symbols) == 1 else None
        event_id_symbol = symbol or ",".join(event.symbols) or "_connection"
        return (
            QualityEvent(
                event_id=_event_id(
                    category,
                    event.session_id,
                    None,
                    event_id_symbol,
                ),
                category=category,
                occurred_at=event.occurred_at,
                route=event.route,
                stream=event.stream,
                symbol=symbol,
                connection_session_id=event.session_id,
                local_sequence=None,
                details={
                    "reason": event.reason,
                    "symbols": list(event.symbols),
                },
            ),
        )

    def check_silence(self, *, now: datetime) -> tuple[QualityEvent, ...]:
        events = []
        for (
            connection_session_id,
            stream,
            symbol,
        ), state in self._observed.items():
            elapsed = (
                now.astimezone(UTC)
                - state.last_received_at.astimezone(UTC)
            ).total_seconds()
            if elapsed <= self._silence_timeout_seconds or state.silence_reported:
                continue
            state.silence_reported = True
            events.append(
                QualityEvent(
                    event_id=_event_id(
                        QualityCategory.SILENCE,
                        connection_session_id,
                        None,
                        symbol,
                    ),
                    category=QualityCategory.SILENCE,
                    occurred_at=now,
                    route=state.route,
                    stream=stream,
                    symbol=None if symbol == "_global" else symbol,
                    connection_session_id=connection_session_id,
                    local_sequence=None,
                    details={"seconds": elapsed},
                )
            )
        return tuple(events)

    def known_gap_count(
        self,
        *,
        connection_session_id: UUID,
        stream: CaptureStream,
        symbol: str,
    ) -> int:
        return self._known_gap_counts.get(
            (connection_session_id, stream, symbol),
            0,
        )

    def _quality_event(
        self,
        category: QualityCategory,
        envelope: RawEnvelope,
        details: dict[str, JsonValue],
    ) -> QualityEvent:
        return QualityEvent(
            event_id=_event_id(
                category,
                envelope.connection_session_id,
                envelope.local_sequence,
                envelope.symbol or "_global",
            ),
            category=category,
            occurred_at=envelope.received_at,
            route=envelope.route,
            stream=envelope.stream,
            symbol=envelope.symbol,
            connection_session_id=envelope.connection_session_id,
            local_sequence=envelope.local_sequence,
            details=details,
        )


def _payload_bytes(envelope: RawEnvelope) -> bytes:
    return json.dumps(
        envelope.raw_payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _agg_trade_gap(
    previous: str | None,
    current: str | None,
    stream: CaptureStream,
) -> int | None:
    if stream is not CaptureStream.AGG_TRADE or previous is None or current is None:
        return None
    try:
        previous_id = int(previous)
        current_id = int(current)
    except ValueError:
        return None
    if current_id > previous_id + 1:
        return current_id - previous_id - 1
    return None


def _event_id(
    category: QualityCategory,
    connection_session_id: UUID,
    local_sequence: int | None,
    symbol: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "quality:"
            f"{category.value}:"
            f"{connection_session_id}:"
            f"{local_sequence}:"
            f"{symbol}"
        ),
    )
