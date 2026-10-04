"""Small telemetry capabilities consumed by independent runtime publishers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Protocol

from crypto_momentum_lab.domain.market.models import JsonValue

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderEvent,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.market.models import MarketState15s
    from crypto_momentum_lab.execution_account.hub import AccountEvent


type LiveLane = Literal["entry", "exit", "unknown"]
type LiveTriggerSource = Literal["account", "quote", "market", "candle", "grace"]
type TerminalReasonSummary = dict[str, dict[str, dict[str, int]]]

LIVE_LANE_ENTRY: LiveLane = "entry"
LIVE_LANE_EXIT: LiveLane = "exit"
LIVE_LANE_UNKNOWN: LiveLane = "unknown"

LIVE_TRIGGER_SOURCE_ACCOUNT: LiveTriggerSource = "account"
LIVE_TRIGGER_SOURCE_QUOTE: LiveTriggerSource = "quote"
LIVE_TRIGGER_SOURCE_MARKET: LiveTriggerSource = "market"
LIVE_TRIGGER_SOURCE_CANDLE: LiveTriggerSource = "candle"
LIVE_TRIGGER_SOURCE_GRACE: LiveTriggerSource = "grace"
LIVE_TRIGGER_SOURCES: frozenset[LiveTriggerSource] = frozenset(
    {
        LIVE_TRIGGER_SOURCE_ACCOUNT,
        LIVE_TRIGGER_SOURCE_QUOTE,
        LIVE_TRIGGER_SOURCE_MARKET,
        LIVE_TRIGGER_SOURCE_CANDLE,
        LIVE_TRIGGER_SOURCE_GRACE,
    }
)


def _require_non_empty_text(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _optional_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass(frozen=True, slots=True)
class TraceKey:
    """Stable identity for one source event within a live run.

    ``source_event_id`` must come from the ingress adapter (or its durable
    envelope) and must not be regenerated when a message is retried.  Keeping
    the pair as structured fields avoids treating a symbol/bucket as a unique
    event: the same bucket can be produced by multiple source messages and by
    both live lanes.
    """

    run_id: str
    source_event_id: str

    def __post_init__(self) -> None:
        _require_non_empty_text(self.run_id, "run_id")
        _require_non_empty_text(self.source_event_id, "source_event_id")

    def as_id(self) -> str:
        """Return a deterministic, collision-resistant string representation."""

        # Length-prefix both components so ``("a:b", "c")`` cannot collide
        # with ``("a", "b:c")`` when persisted as one text key.
        return (
            f"{len(self.run_id)}:{self.run_id}:"
            f"{len(self.source_event_id)}:{self.source_event_id}"
        )


@dataclass(frozen=True, slots=True)
class SourceIngress:
    """Normalized source metadata captured at the first process boundary.

    ``source_occurred_at`` is the source/exchange timestamp when available;
    ``received_at`` is the local monotonic-wall-clock observation timestamp
    used for latency accounting.  They are deliberately separate so event
    time cannot be mistaken for local receive time.
    """

    run_id: str
    source_event_id: str
    lane: LiveLane
    trigger_source: LiveTriggerSource | None
    received_at: datetime
    source_occurred_at: datetime | None = None
    symbol: str | None = None
    bucket_start: datetime | None = None

    def __post_init__(self) -> None:
        _require_non_empty_text(self.run_id, "run_id")
        _require_non_empty_text(self.source_event_id, "source_event_id")
        if self.lane not in {
            LIVE_LANE_ENTRY,
            LIVE_LANE_EXIT,
            LIVE_LANE_UNKNOWN,
        }:
            raise ValueError(f"unsupported live lane: {self.lane!r}")
        if (
            self.trigger_source is not None
            and self.trigger_source not in LIVE_TRIGGER_SOURCES
        ):
            raise ValueError(f"unsupported trigger source: {self.trigger_source!r}")
        if self.lane == LIVE_LANE_EXIT and self.trigger_source is None:
            raise ValueError("exit ingress requires a trigger_source")
        _require_aware(self.received_at, "received_at")
        if self.source_occurred_at is not None:
            _require_aware(self.source_occurred_at, "source_occurred_at")
        if self.bucket_start is not None:
            _require_aware(self.bucket_start, "bucket_start")
        if self.symbol is not None:
            _require_non_empty_text(self.symbol, "symbol")

    @property
    def trace_key(self) -> TraceKey:
        return TraceKey(self.run_id, self.source_event_id)

    @property
    def trace_id(self) -> str:
        return self.trace_key.as_id()

    def details(self) -> dict[str, JsonValue]:
        """Return safe, JSON-compatible metadata for a runtime event."""

        return {
            "source_event_id": self.source_event_id,
            "source_occurred_at": _optional_iso(self.source_occurred_at),
            "source_received_at": self.received_at.isoformat(),
            "source_trace_id": self.trace_id,
            "trigger_source": self.trigger_source,
            "trace_id": self.trace_id,
        }




class ConsumerHealthSink(Protocol):
    def consumer_health(
        self,
        *,
        consumer: str,
        available: bool,
        occurred_at: datetime,
        reason: str | None = None,
        recovery: bool = False,
        lag: bool = False,
        sequence: int | None = None,
    ) -> None: ...



class AccountFillSink(Protocol):
    async def account_fill(
        self,
        event: AccountEvent,
        *,
        occurred_at: datetime,
    ) -> None: ...



class OrderEventSink(Protocol):
    async def order_event(
        self, plan: OrderExecutionPlan, event: ExchangeOrderEvent
    ) -> None: ...


class MarketAdmissionSink(Protocol):
    async def context_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        prefetched: bool,
        reloaded: bool,
        ingress: SourceIngress | None = None,
    ) -> None: ...
