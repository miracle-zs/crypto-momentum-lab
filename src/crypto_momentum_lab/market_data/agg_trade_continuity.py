"""Continuity checks for the latency-sensitive aggregate-trade path."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureStream,
    RawEnvelope,
)


@dataclass(frozen=True, slots=True)
class _SeenTrade:
    aggregate_trade_id: int
    event_at: datetime
    connection_session_id: UUID


class AggTradeContinuityTracker:
    """Detect a live aggTrade discontinuity without recovery or archive I/O."""

    def __init__(self) -> None:
        self._last_seen: dict[tuple[str, str], _SeenTrade] = {}
        self._monitored_symbols: frozenset[str] | None = None

    def set_monitored_symbols(self, symbols: frozenset[str]) -> None:
        normalized = frozenset(symbol.upper() for symbol in symbols)
        self._monitored_symbols = normalized
        self._last_seen = {
            key: seen
            for key, seen in self._last_seen.items()
            if key[1] in normalized
        }

    def observe(self, envelope: RawEnvelope) -> tuple[bool, AggTradeGap | None]:
        """Return whether to publish *envelope* and its preceding gap, if any."""
        parsed = _agg_trade_identity(envelope)
        if parsed is None:
            return True, None
        key, current_id, event_at = parsed
        if (
            self._monitored_symbols is not None
            and key[1] not in self._monitored_symbols
        ):
            # The dense decision universe is not a capture filter. Preserve
            # the envelope for aggregation, but avoid retaining continuity
            # state for a symbol that the universe intentionally removed.
            return True, None
        previous = self._last_seen.get(key)
        current = _SeenTrade(current_id, event_at, envelope.connection_session_id)
        if previous is None:
            self._last_seen[key] = current
            return True, None
        if current_id <= previous.aggregate_trade_id:
            return False, None
        self._last_seen[key] = current
        if current_id == previous.aggregate_trade_id + 1:
            return True, None
        reason = "realtime_continuity_gap"
        if previous.connection_session_id != envelope.connection_session_id:
            reason = f"reconnect_{reason}"
        return (
            True,
            AggTradeGap(
                environment=envelope.environment,
                symbol=envelope.symbol or key[1],
                previous_id=previous.aggregate_trade_id,
                current_id=current_id,
                previous_event_at=previous.event_at,
                current_event_at=event_at,
                missing_count=current_id - previous.aggregate_trade_id - 1,
                reason=reason,
            ),
        )


def _agg_trade_identity(
    envelope: RawEnvelope,
) -> tuple[tuple[str, str], int, datetime] | None:
    if (
        envelope.stream is not CaptureStream.AGG_TRADE
        or envelope.symbol is None
        or envelope.exchange_sequence is None
        or envelope.exchange_event_at is None
    ):
        return None
    try:
        aggregate_trade_id = int(envelope.exchange_sequence)
    except ValueError:
        return None
    return (
        (envelope.environment, envelope.symbol),
        aggregate_trade_id,
        envelope.exchange_event_at,
    )
