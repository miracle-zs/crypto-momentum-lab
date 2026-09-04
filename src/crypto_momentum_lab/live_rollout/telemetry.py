"""Low-overhead phase telemetry for the live strategy lanes.

The recorder keeps the trading path independent from the database writer.  A
phase is recorded in memory immediately, while a bounded queue forwards the
same event to ``strategy_runtime_events`` when a persistence sink is present.
This makes latency measurement useful during a database incident without
turning telemetry into another reason to stop submitting or closing orders.
"""

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

import structlog

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.execution_account.hub import AccountEvent

log = structlog.get_logger()

type LiveLane = Literal["entry", "exit", "unknown"]
type LiveTriggerSource = Literal[
    "account", "quote", "market", "candle", "grace"
]
type TerminalReasonSummary = dict[str, dict[str, dict[str, int]]]

LIVE_LANE_ENTRY: LiveLane = "entry"
LIVE_LANE_EXIT: LiveLane = "exit"
LIVE_LANE_UNKNOWN: LiveLane = "unknown"

SOURCE_RECEIVED = "source_received"
TRACE_TERMINATED = "trace_terminated"

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

MARKET_STATE_RECEIVED = "market_state_received"
CONTEXT_READY = "context_ready"
GATE_EVALUATED = "gate_evaluated"
STRATEGY_DECISION = "strategy_decision"
ENTRY_FILTER_READY = "entry_filter_ready"
SIGNAL_RECORDED = "signal_recorded"
CANDIDATE_ACCEPTED = "candidate_accepted"
RISK_APPROVED = "risk_approved"
INTENT_SAVED = "intent_saved"
SUBMITTING = "submitting"
EXCHANGE_REQUEST_STARTED = "exchange_request_started"
EXCHANGE_RESPONSE_RECEIVED = "exchange_response_received"
EXCHANGE_FILLED = "exchange_filled"
ACCOUNT_FILL = "account_fill"

_PHASE_ORDER = (
    SOURCE_RECEIVED,
    MARKET_STATE_RECEIVED,
    CONTEXT_READY,
    GATE_EVALUATED,
    STRATEGY_DECISION,
    ENTRY_FILTER_READY,
    SIGNAL_RECORDED,
    CANDIDATE_ACCEPTED,
    RISK_APPROVED,
    INTENT_SAVED,
    SUBMITTING,
    EXCHANGE_REQUEST_STARTED,
    EXCHANGE_RESPONSE_RECEIVED,
    EXCHANGE_FILLED,
    ACCOUNT_FILL,
    TRACE_TERMINATED,
)
_REPEATABLE_PHASES = frozenset({ACCOUNT_FILL})
_EXCHANGE_BOUNDARY_EVENTS = frozenset(
    {EXCHANGE_REQUEST_STARTED, EXCHANGE_RESPONSE_RECEIVED}
)
_MAX_EVENT_BATCH = 128
_PERSIST_BATCH_TIMEOUT_SECONDS = 0.25

# Market-state and strategy-decision events remain available in the in-memory
# trace, but are intentionally not durable by default.  Persisting those
# high-frequency events turns observability into a steady WAL producer and can
# lengthen checkpoints for the execution data plane.  Order lifecycle events
# are sparse enough to retain for post-incident reconstruction.
PERSISTED_ORDER_TELEMETRY_EVENTS = frozenset(
    {
        CANDIDATE_ACCEPTED,
        RISK_APPROVED,
        INTENT_SAVED,
        SUBMITTING,
        EXCHANGE_REQUEST_STARTED,
        EXCHANGE_RESPONSE_RECEIVED,
        EXCHANGE_FILLED,
        ACCOUNT_FILL,
    }
)


class LiveTelemetrySink(Protocol):
    async def source_received(self, ingress: "SourceIngress") -> None: ...

    async def trace_terminated(
        self,
        ingress: "SourceIngress",
        *,
        occurred_at: datetime,
        reason: str,
        details: Mapping[str, JsonValue] | None = None,
    ) -> None: ...

    async def market_state_received(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        lane: str = LIVE_LANE_ENTRY,
        ingress: "SourceIngress | None" = None,
    ) -> None: ...

    async def context_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        prefetched: bool,
        reloaded: bool,
        ingress: "SourceIngress | None" = None,
    ) -> None: ...

    async def gate_evaluated(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        approved: bool,
        reasons: tuple[str, ...],
    ) -> None: ...

    async def strategy_decision(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        signal_count: int,
        candidate_count: int,
    ) -> None: ...

    async def entry_filter_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        candidate_count: int,
        symbol_pool_loaded: bool,
        ema_context_loaded: bool,
    ) -> None: ...

    async def signal_recorded(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        candidate_count: int,
    ) -> None: ...

    async def candidate_accepted(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        ingress: "SourceIngress | None" = None,
    ) -> None: ...

    async def risk_approved(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        evaluation_id: str,
        ingress: "SourceIngress | None" = None,
    ) -> None: ...

    async def intent_saved(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        ingress: "SourceIngress | None" = None,
    ) -> None: ...

    async def order_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None: ...

    async def exchange_request_started(
        self,
        plan: OrderExecutionPlan,
        phase: str,
        occurred_at: datetime,
    ) -> None: ...

    async def exchange_response_received(
        self,
        plan: OrderExecutionPlan,
        phase: str,
        occurred_at: datetime,
    ) -> None: ...

    async def account_fill(
        self,
        event: AccountEvent,
        *,
        occurred_at: datetime,
    ) -> None: ...


RuntimeEventBatchSink = Callable[
    [tuple[Mapping[str, object], ...]],
    Awaitable[None],
]


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
            raise ValueError(
                f"unsupported trigger source: {self.trigger_source!r}"
            )
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


@dataclass(frozen=True, slots=True)
class LiveRuntimeEvent:
    event_id: str
    run_id: str
    event_type: str
    occurred_at: datetime
    symbol: str | None
    bucket_start: datetime | None
    details: dict[str, JsonValue]

    def row(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at,
            "symbol": self.symbol,
            "bucket_start": self.bucket_start,
            "details": self.details,
        }


@dataclass(slots=True)
class _Trace:
    lane: str
    symbol: str | None
    bucket_start: datetime | None
    phase_at: dict[str, datetime] = field(default_factory=dict)


def state_trace_id(state: MarketState15s, lane: str) -> str:
    """Return the stable trace key for one symbol/bucket/lane."""

    return f"{lane}:{state.symbol}:{state.bucket_start.isoformat()}"


class LiveRuntimeTelemetry:
    """Record live lifecycle phases and calculate latency percentiles.

    The persistence sink receives batches on a background task.  ``record``
    methods still update phase timestamps synchronously before yielding, so a
    slow or unavailable database cannot hide the latency of the live lanes.
    ``persist_event_types`` only controls the durable stream; omitted values
    preserve the diagnostic behavior of persisting every event type.
    ``persist_exchange_operations`` is an optional allow-list applied only to
    exchange request/response boundary events.  ``None`` preserves the
    existing behavior and persists every exchange operation; an explicit set
    can be used to keep, for example, only ``submit`` and ``cancel`` while
    leaving the in-memory trace unchanged.
    """

    def __init__(
        self,
        *,
        run_id: str,
        persist: RuntimeEventBatchSink | None = None,
        persist_event_types: Collection[str] | None = None,
        persist_exchange_operations: Collection[str] | None = None,
        queue_size: int = 4096,
        max_trace_count: int = 8192,
        max_samples_per_metric: int = 4096,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")
        if max_trace_count <= 0:
            raise ValueError("max_trace_count must be positive")
        if max_samples_per_metric <= 0:
            raise ValueError("max_samples_per_metric must be positive")
        self._run_id = run_id
        self._persist = persist
        self._persist_event_types = (
            None
            if persist_event_types is None
            else frozenset(persist_event_types)
        )
        self._persist_exchange_operations = (
            None
            if persist_exchange_operations is None
            else _normalize_exchange_operations(persist_exchange_operations)
        )
        self._queue_size = queue_size
        self._max_trace_count = max_trace_count
        self._max_samples_per_metric = max_samples_per_metric
        self._queue: asyncio.Queue[LiveRuntimeEvent | None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._traces: dict[str, _Trace] = {}
        self._source_ingress_by_trace: dict[str, SourceIngress] = {}
        self._source_trace_order: deque[str] = deque()
        self._order_trace_by_client: dict[str, str] = {}
        self._order_lane_by_client: dict[str, str] = {}
        self._order_clients: deque[str] = deque()
        self._samples: dict[tuple[str, str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=self._max_samples_per_metric)
        )
        self._recent_events: deque[LiveRuntimeEvent] = deque(maxlen=queue_size)
        # Keep the production-facing aggregate intentionally low-cardinality:
        # lane, trigger source and terminal reason only.  Source ids and
        # symbols remain available on the bounded event trace, not in this
        # counter map.
        self._terminal_reason_counts: dict[tuple[str, str, str], int] = {}
        self._recorded_event_count = 0
        self._dropped_event_count = 0
        self._persist_failure_count = 0

    @property
    def recorded_event_count(self) -> int:
        return self._recorded_event_count

    @property
    def dropped_event_count(self) -> int:
        return self._dropped_event_count

    @property
    def persist_failure_count(self) -> int:
        return self._persist_failure_count

    @property
    def recent_events(self) -> tuple[LiveRuntimeEvent, ...]:
        return tuple(self._recent_events)

    def _trace_id_for_ingress(
        self,
        ingress: SourceIngress | None,
        *,
        lane: str,
    ) -> str | None:
        if ingress is None:
            return None
        if ingress.run_id != self._run_id:
            raise ValueError(
                "source ingress run_id does not match telemetry run_id"
            )
        if ingress.lane != lane:
            raise ValueError(
                "source ingress lane does not match telemetry lane"
            )
        return ingress.trace_id

    async def start(self) -> None:
        if self._persist is None or self._writer_task is not None:
            return
        self._queue = asyncio.Queue(maxsize=self._queue_size)
        self._writer_task = asyncio.create_task(
            self._write_events(),
            name="live-runtime-telemetry-writer",
        )

    async def stop(self) -> None:
        writer_task = self._writer_task
        queue = self._queue
        if writer_task is None or queue is None:
            return
        await queue.put(None)
        await writer_task
        self._writer_task = None
        self._queue = None
        log.info(
            "live_latency_telemetry_stopped",
            run_id=self._run_id,
            recorded_event_count=self._recorded_event_count,
            dropped_event_count=self._dropped_event_count,
            persist_failure_count=self._persist_failure_count,
            persist_exchange_operations=(
                None
                if self._persist_exchange_operations is None
                else sorted(self._persist_exchange_operations)
            ),
            latency_summary=self.latency_summary(),
            terminal_reason_summary=self.terminal_reason_summary(),
        )

    async def source_received(self, ingress: SourceIngress) -> None:
        """Record the first normalized observation of a source event."""

        if ingress.run_id != self._run_id:
            raise ValueError(
                "source ingress run_id does not match telemetry run_id"
            )
        await self._record_phase(
            phase=SOURCE_RECEIVED,
            trace_id=ingress.trace_id,
            lane=ingress.lane,
            symbol=ingress.symbol,
            bucket_start=ingress.bucket_start,
            occurred_at=ingress.received_at,
            details=ingress.details(),
        )

    async def trace_terminated(
        self,
        ingress: SourceIngress,
        *,
        occurred_at: datetime,
        reason: str,
        details: Mapping[str, JsonValue] | None = None,
    ) -> None:
        """Record why a source trace stopped before another lifecycle phase.

        Terminal reasons deliberately stay on the source trace instead of the
        execution state machine.  This keeps rejection, disabled-path and
        recovery outcomes observable without changing order semantics or
        making a high-frequency source event durable by default.
        """

        self._trace_id_for_ingress(ingress, lane=ingress.lane)
        _require_non_empty_text(reason, "reason")
        event_details: dict[str, JsonValue] = {
            **_ingress_details(ingress),
            **(details or {}),
            "reason": reason,
        }
        await self._record_phase(
            phase=TRACE_TERMINATED,
            trace_id=ingress.trace_id,
            lane=ingress.lane,
            symbol=ingress.symbol,
            bucket_start=ingress.bucket_start,
            occurred_at=occurred_at,
            details=event_details,
        )
        source = ingress.trigger_source or LIVE_LANE_UNKNOWN
        counter_key = (ingress.lane, source, reason)
        self._terminal_reason_counts[counter_key] = (
            self._terminal_reason_counts.get(counter_key, 0) + 1
        )

    async def market_state_received(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        lane: str = LIVE_LANE_ENTRY,
        ingress: SourceIngress | None = None,
    ) -> None:
        trace_id = self._trace_id_for_ingress(ingress, lane=lane)
        await self._record_phase(
            phase=MARKET_STATE_RECEIVED,
            trace_id=trace_id or state_trace_id(state, lane),
            lane=lane,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "source_first_received_at": _optional_iso(state.first_received_at),
                "source_last_received_at": _optional_iso(state.last_received_at),
                "source_event_count": state.source_event_count,
                **_ingress_details(ingress),
            },
        )

    async def context_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        prefetched: bool,
        reloaded: bool,
        ingress: SourceIngress | None = None,
    ) -> None:
        lane = LIVE_LANE_ENTRY if ingress is None else ingress.lane
        trace_id = self._trace_id_for_ingress(ingress, lane=lane)
        await self._record_phase(
            phase=CONTEXT_READY,
            trace_id=trace_id or state_trace_id(state, lane),
            lane=lane,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "prefetched": prefetched,
                "reloaded": reloaded,
                **_ingress_details(ingress),
            },
        )

    async def gate_evaluated(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        approved: bool,
        reasons: tuple[str, ...],
    ) -> None:
        await self._record_phase(
            phase=GATE_EVALUATED,
            trace_id=state_trace_id(state, LIVE_LANE_ENTRY),
            lane=LIVE_LANE_ENTRY,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={"approved": approved, "reasons": list(reasons)},
        )

    async def strategy_decision(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        signal_count: int,
        candidate_count: int,
    ) -> None:
        await self._record_phase(
            phase=STRATEGY_DECISION,
            trace_id=state_trace_id(state, LIVE_LANE_ENTRY),
            lane=LIVE_LANE_ENTRY,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "signal_count": signal_count,
                "candidate_count": candidate_count,
            },
        )

    async def entry_filter_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        candidate_count: int,
        symbol_pool_loaded: bool,
        ema_context_loaded: bool,
    ) -> None:
        await self._record_phase(
            phase=ENTRY_FILTER_READY,
            trace_id=state_trace_id(state, LIVE_LANE_ENTRY),
            lane=LIVE_LANE_ENTRY,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "candidate_count": candidate_count,
                "symbol_pool_loaded": symbol_pool_loaded,
                "ema_context_loaded": ema_context_loaded,
            },
        )

    async def signal_recorded(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        candidate_count: int,
    ) -> None:
        await self._record_phase(
            phase=SIGNAL_RECORDED,
            trace_id=state_trace_id(state, LIVE_LANE_ENTRY),
            lane=LIVE_LANE_ENTRY,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={"candidate_count": candidate_count},
        )

    async def candidate_accepted(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        ingress: SourceIngress | None = None,
    ) -> None:
        candidate_id = _required_text(candidate, "candidate_id")
        trace_id = self._trace_id_for_ingress(ingress, lane=lane)
        self._remember_source_ingress(candidate_id, ingress)
        await self._record_phase(
            phase=CANDIDATE_ACCEPTED,
            trace_id=candidate_id,
            parent_trace_id=trace_id or state_trace_id(state, lane),
            lane=lane,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "candidate_id": candidate_id,
                "signal_id": _optional_text(candidate, "signal_id"),
                "reduce_only": bool(getattr(candidate, "reduce_only", False)),
                **_ingress_details(ingress),
            },
        )

    async def risk_approved(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        evaluation_id: str,
        ingress: SourceIngress | None = None,
    ) -> None:
        candidate_id = _required_text(candidate, "candidate_id")
        self._trace_id_for_ingress(ingress, lane=lane)
        self._remember_source_ingress(candidate_id, ingress)
        await self._record_phase(
            phase=RISK_APPROVED,
            trace_id=candidate_id,
            lane=lane,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "candidate_id": candidate_id,
                "evaluation_id": evaluation_id,
                **_ingress_details(ingress),
            },
        )

    async def intent_saved(
        self,
        candidate: object,
        *,
        state: MarketState15s,
        occurred_at: datetime,
        lane: str,
        ingress: SourceIngress | None = None,
    ) -> None:
        candidate_id = _required_text(candidate, "candidate_id")
        self._trace_id_for_ingress(ingress, lane=lane)
        self._remember_source_ingress(candidate_id, ingress)
        await self._record_phase(
            phase=INTENT_SAVED,
            trace_id=candidate_id,
            lane=lane,
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            occurred_at=occurred_at,
            details={
                "candidate_id": candidate_id,
                **_ingress_details(ingress),
            },
        )

    async def order_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        lane = LIVE_LANE_EXIT if plan.reduce_only else LIVE_LANE_ENTRY
        if plan.client_order_id not in self._order_trace_by_client:
            self._order_clients.append(plan.client_order_id)
        self._order_trace_by_client[plan.client_order_id] = plan.intent_id
        self._order_lane_by_client[plan.client_order_id] = lane
        while len(self._order_trace_by_client) > self._max_trace_count:
            oldest_client_order_id = self._order_clients.popleft()
            self._order_trace_by_client.pop(oldest_client_order_id, None)
            self._order_lane_by_client.pop(oldest_client_order_id, None)
        if event.state is ExchangeOrderState.SUBMITTING:
            phase = SUBMITTING
        elif event.state is ExchangeOrderState.FILLED:
            phase = EXCHANGE_FILLED
        else:
            return
        source_ingress = self._source_ingress_by_trace.get(plan.intent_id)
        await self._record_phase(
            phase=phase,
            trace_id=plan.intent_id,
            lane=lane,
            symbol=plan.symbol,
            bucket_start=self._trace_bucket_start(plan.intent_id),
            occurred_at=event.occurred_at,
            details={
                "client_order_id": plan.client_order_id,
                "intent_id": plan.intent_id,
                "exchange_order_id": event.exchange_order_id,
                "order_state": event.state.value,
                "reduce_only": plan.reduce_only,
                **_ingress_details(source_ingress),
                **event.details,
            },
        )

    async def exchange_request_started(
        self,
        plan: OrderExecutionPlan,
        phase: str,
        occurred_at: datetime,
    ) -> None:
        await self._record_exchange_boundary(plan, phase, occurred_at)

    async def exchange_response_received(
        self,
        plan: OrderExecutionPlan,
        phase: str,
        occurred_at: datetime,
    ) -> None:
        await self._record_exchange_boundary(plan, phase, occurred_at)

    async def _record_exchange_boundary(
        self,
        plan: OrderExecutionPlan,
        phase: str,
        occurred_at: datetime,
    ) -> None:
        lane = LIVE_LANE_EXIT if plan.reduce_only else LIVE_LANE_ENTRY
        source_ingress = self._source_ingress_by_trace.get(plan.intent_id)
        await self._record_phase(
            phase=(
                EXCHANGE_REQUEST_STARTED
                if phase.endswith("request_started")
                else EXCHANGE_RESPONSE_RECEIVED
            ),
            trace_id=plan.intent_id,
            lane=lane,
            symbol=plan.symbol,
            bucket_start=self._trace_bucket_start(plan.intent_id),
            occurred_at=occurred_at,
            details={
                "client_order_id": plan.client_order_id,
                "intent_id": plan.intent_id,
                "operation": phase.removesuffix("_request_started").removesuffix(
                    "_response_received"
                ),
                "reduce_only": plan.reduce_only,
                **_ingress_details(source_ingress),
            },
        )

    async def account_fill(
        self,
        event: AccountEvent,
        *,
        occurred_at: datetime,
    ) -> None:
        if not event.has_fill:
            return
        client_order_id = event.client_order_id
        trace_id = (
            self._order_trace_by_client.get(client_order_id or "")
            or client_order_id
            or event.event_id
        )
        trace = self._traces.get(trace_id)
        lane = self._order_lane_by_client.get(
            client_order_id or "",
            trace.lane if trace is not None else LIVE_LANE_UNKNOWN,
        )
        symbol = event.symbol or (trace.symbol if trace is not None else None)
        bucket_start = trace.bucket_start if trace is not None else None
        source_ingress = self._source_ingress_by_trace.get(trace_id)
        await self._record_phase(
            phase=ACCOUNT_FILL,
            trace_id=trace_id,
            lane=lane,
            symbol=symbol,
            bucket_start=bucket_start,
            occurred_at=occurred_at,
            details={
                "account_event_id": event.event_id,
                "client_order_id": client_order_id,
                "trade_id": event.trade_id,
                "order_status": event.order_status,
                "source_event_at": event.event_at.isoformat(),
                "source_received_at": event.received_at.isoformat(),
                **_ingress_details(source_ingress),
            },
        )

    def latency_summary(
        self,
    ) -> dict[str, dict[str, dict[str, dict[str, float | int]]]]:
        summary: dict[
            str,
            dict[str, dict[str, dict[str, float | int]]],
        ] = {}
        for (symbol, lane, transition), values in sorted(self._samples.items()):
            symbol_summary = summary.setdefault(symbol, {})
            lane_summary = symbol_summary.setdefault(lane, {})
            sorted_values = sorted(values)
            lane_summary[transition] = {
                "count": len(sorted_values),
                "p50_ms": _percentile(sorted_values, 0.50),
                "p95_ms": _percentile(sorted_values, 0.95),
                "max_ms": sorted_values[-1],
            }
        return summary

    def terminal_reason_summary(self) -> TerminalReasonSummary:
        """Return a detached run-scoped count by lane, source and reason.

        The summary deliberately omits symbols and source event ids so it can
        be sampled or exported as a low-cardinality operational metric.  Detailed
        identity and context remain on ``recent_events`` while this method is
        strictly observational and does not alter the recorder state.
        """

        summary: TerminalReasonSummary = {}
        for (lane, source, reason), count in sorted(
            self._terminal_reason_counts.items()
        ):
            lane_summary = summary.setdefault(lane, {})
            source_summary = lane_summary.setdefault(source, {})
            source_summary[reason] = count
        return summary

    async def _record_phase(
        self,
        *,
        phase: str,
        trace_id: str,
        lane: str,
        symbol: str | None,
        bucket_start: datetime | None,
        occurred_at: datetime,
        details: Mapping[str, JsonValue],
        parent_trace_id: str | None = None,
    ) -> None:
        _require_aware(occurred_at, "occurred_at")
        trace = self._ensure_trace(
            trace_id=trace_id,
            parent_trace_id=parent_trace_id,
            lane=lane,
            symbol=symbol,
            bucket_start=bucket_start,
        )
        event_details: dict[str, JsonValue] = {
            key: _json_value(value) for key, value in details.items()
        }
        event_details.update(
            {
                "phase": phase,
                "lane": lane,
                "trace_id": trace_id,
            }
        )
        previous_phase = _previous_phase(phase, trace.phase_at)
        if previous_phase is not None:
            previous_at = trace.phase_at[previous_phase]
            delta_ms = (occurred_at - previous_at).total_seconds() * 1000
            event_details["previous_phase"] = previous_phase
            event_details["latency_ms_from_previous"] = delta_ms
            if delta_ms >= 0 and (
                phase not in trace.phase_at or phase in _REPEATABLE_PHASES
            ):
                self._add_sample(
                    symbol=symbol or trace.symbol or "UNKNOWN",
                    lane=lane,
                    transition=f"{previous_phase}->{phase}",
                    value=delta_ms,
                )
        if (
            MARKET_STATE_RECEIVED in trace.phase_at
            and phase != MARKET_STATE_RECEIVED
            and (phase not in trace.phase_at or phase in _REPEATABLE_PHASES)
        ):
            origin_delta_ms = (
                occurred_at - trace.phase_at[MARKET_STATE_RECEIVED]
            ).total_seconds() * 1000
            event_details["latency_ms_from_market_state"] = origin_delta_ms
            if origin_delta_ms >= 0:
                self._add_sample(
                    symbol=symbol or trace.symbol or "UNKNOWN",
                    lane=lane,
                    transition=f"{MARKET_STATE_RECEIVED}->{phase}",
                    value=origin_delta_ms,
                )
        if phase not in trace.phase_at or phase in _REPEATABLE_PHASES:
            trace.phase_at[phase] = occurred_at
        event = LiveRuntimeEvent(
            event_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"live-runtime:{self._run_id}:{phase}:{trace_id}:"
                    f"{occurred_at.isoformat()}",
                )
            ),
            run_id=self._run_id,
            event_type=phase,
            occurred_at=occurred_at,
            symbol=symbol or trace.symbol,
            bucket_start=bucket_start or trace.bucket_start,
            details=event_details,
        )
        self._recorded_event_count += 1
        self._recent_events.append(event)
        self._enqueue(event)

    def _ensure_trace(
        self,
        *,
        trace_id: str,
        parent_trace_id: str | None,
        lane: str,
        symbol: str | None,
        bucket_start: datetime | None,
    ) -> _Trace:
        trace = self._traces.get(trace_id)
        if trace is None:
            parent = self._traces.get(parent_trace_id or "")
            trace = _Trace(
                lane=lane,
                symbol=symbol or (parent.symbol if parent is not None else None),
                bucket_start=(
                    bucket_start
                    or (parent.bucket_start if parent is not None else None)
                ),
                phase_at=(dict(parent.phase_at) if parent is not None else {}),
            )
            self._traces[trace_id] = trace
            self._trim_traces()
        return trace

    def _remember_source_ingress(
        self,
        trace_id: str,
        ingress: SourceIngress | None,
    ) -> None:
        if ingress is None:
            return
        if trace_id not in self._source_ingress_by_trace:
            self._source_trace_order.append(trace_id)
        self._source_ingress_by_trace[trace_id] = ingress
        while len(self._source_ingress_by_trace) > self._max_trace_count:
            oldest_trace_id = self._source_trace_order.popleft()
            self._source_ingress_by_trace.pop(oldest_trace_id, None)

    def _trace_bucket_start(self, trace_id: str) -> datetime | None:
        trace = self._traces.get(trace_id)
        return None if trace is None else trace.bucket_start

    def _trim_traces(self) -> None:
        while len(self._traces) > self._max_trace_count:
            oldest = next(iter(self._traces))
            self._traces.pop(oldest, None)

    def _add_sample(
        self,
        *,
        symbol: str,
        lane: str,
        transition: str,
        value: float,
    ) -> None:
        self._samples[(symbol, lane, transition)].append(value)

    def _enqueue(self, event: LiveRuntimeEvent) -> None:
        if self._queue is None:
            return
        if (
            self._persist_event_types is not None
            and event.event_type not in self._persist_event_types
        ):
            return
        if (
            self._persist_exchange_operations is not None
            and event.event_type in _EXCHANGE_BOUNDARY_EVENTS
            and not _exchange_operation_is_allowed(
                event.details.get("operation"),
                self._persist_exchange_operations,
            )
        ):
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped_event_count += 1

    async def _write_events(self) -> None:
        if self._queue is None or self._persist is None:
            return
        while True:
            first = await self._queue.get()
            if first is None:
                return
            batch = [first]
            stop_after_batch = False
            for _ in range(_MAX_EVENT_BATCH - 1):
                try:
                    next_event = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if next_event is None:
                    stop_after_batch = True
                    break
                batch.append(next_event)
            try:
                await asyncio.wait_for(
                    self._persist(tuple(event.row() for event in batch)),
                    timeout=_PERSIST_BATCH_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._persist_failure_count += len(batch)
                log.warning(
                    "live_runtime_telemetry_persist_failed",
                    run_id=self._run_id,
                    event_count=len(batch),
                    error_type=type(error).__name__,
                )
            if stop_after_batch:
                return


def _previous_phase(phase: str, phase_at: Mapping[str, datetime]) -> str | None:
    try:
        phase_index = _PHASE_ORDER.index(phase)
    except ValueError:
        return None
    for previous in reversed(_PHASE_ORDER[:phase_index]):
        if previous in phase_at:
            return previous
    return None


def _percentile(values: list[float], percentile: float) -> float:
    position = (len(values) - 1) * percentile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(values) - 1)
    weight = position - lower_index
    return values[lower_index] + (
        values[upper_index] - values[lower_index]
    ) * weight


def _required_text(value: object, field_name: str) -> str:
    result = _optional_text(value, field_name)
    if result is None:
        raise ValueError(f"{field_name} must be non-empty")
    return result


def _require_non_empty_text(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _normalize_exchange_operations(
    operations: Collection[str],
) -> frozenset[str]:
    normalized: set[str] = set()
    for operation in operations:
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError(
                "persist_exchange_operations must contain non-empty names"
            )
        normalized.add(operation.strip())
    return frozenset(normalized)


def _exchange_operation_is_allowed(
    operation: JsonValue,
    allowed_operations: frozenset[str],
) -> bool:
    return isinstance(operation, str) and operation in allowed_operations


def _optional_text(value: object, field_name: str) -> str | None:
    result = getattr(value, field_name, None)
    if result is None:
        return None
    text = str(result).strip()
    return text or None


def _optional_iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _ingress_details(ingress: SourceIngress | None) -> dict[str, JsonValue]:
    return {} if ingress is None else ingress.details()


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return str(value)


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "ACCOUNT_FILL",
    "CANDIDATE_ACCEPTED",
    "CONTEXT_READY",
    "EXCHANGE_FILLED",
    "ENTRY_FILTER_READY",
    "GATE_EVALUATED",
    "INTENT_SAVED",
    "LIVE_LANE_ENTRY",
    "LIVE_LANE_EXIT",
    "LIVE_LANE_UNKNOWN",
    "LIVE_TRIGGER_SOURCE_ACCOUNT",
    "LIVE_TRIGGER_SOURCE_CANDLE",
    "LIVE_TRIGGER_SOURCE_GRACE",
    "LIVE_TRIGGER_SOURCE_MARKET",
    "LIVE_TRIGGER_SOURCE_QUOTE",
    "LIVE_TRIGGER_SOURCES",
    "LiveLane",
    "LiveRuntimeEvent",
    "LiveRuntimeTelemetry",
    "LiveTelemetrySink",
    "LiveTriggerSource",
    "MARKET_STATE_RECEIVED",
    "PERSISTED_ORDER_TELEMETRY_EVENTS",
    "RISK_APPROVED",
    "SIGNAL_RECORDED",
    "SOURCE_RECEIVED",
    "SourceIngress",
    "STRATEGY_DECISION",
    "SUBMITTING",
    "TerminalReasonSummary",
    "TRACE_TERMINATED",
    "TraceKey",
    "state_trace_id",
]
