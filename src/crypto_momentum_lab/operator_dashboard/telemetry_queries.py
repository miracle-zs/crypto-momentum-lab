"""Historical runtime telemetry queries for the operator dashboard.

This module owns the decision-SLO read model: window validation, the bounded
database query, and the low-cardinality aggregation.  The dashboard facade
only supplies the session adapter and clock, so the telemetry domain can be
tested without learning the rest of the dashboard query surface.
"""

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.operator_dashboard.schemas import (
    DecisionSLOConsumerResponse,
    DecisionSLOLatencyResponse,
    DecisionSLOResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import StrategyRuntimeEventRow

_DECISION_SLO_WINDOWS = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}
_DECISION_SLO_MAX_EVENTS = 50_000
_DECISION_SLO_LATENCY_KEY = "decision_slo_latency_ms"
_CONSUMER_HEALTH_EVENT = "consumer_health"
_TERMINAL_REASON_EVENTS = frozenset(
    {"terminal_reason", "trace_terminated"}
)
_DECISION_SLO_EVENT_TYPES = (
    "candidate_accepted",
    "risk_approved",
    "intent_saved",
    "submitting",
    "exchange_request_started",
    "exchange_response_received",
    "exchange_filled",
    "account_fill",
    _CONSUMER_HEALTH_EVENT,
    "terminal_reason",
    "trace_terminated",
)


class _DecisionSLOConsumerStats(TypedDict):
    observed_event_count: int
    recovery_count: int
    unavailable_event_count: int
    lag_event_count: int
    last_available: bool | None
    last_recovery_reason: str | None
    last_observed_at: datetime | None


def _decision_slo_response(
    rows: Sequence[Any],
    *,
    window: str,
    window_start: datetime,
    window_end: datetime,
    truncated: bool,
) -> DecisionSLOResponse:
    latency_samples: dict[str, list[float]] = {}
    terminal_reasons: dict[str, dict[str, dict[str, int]]] = {}
    consumer_stats: dict[str, _DecisionSLOConsumerStats] = {}
    for row in rows:
        details = row.details if isinstance(row.details, Mapping) else {}
        recorded_transitions: set[str] = set()
        decision_slo_latencies = details.get(_DECISION_SLO_LATENCY_KEY)
        if isinstance(decision_slo_latencies, Mapping):
            for transition, raw_latency in decision_slo_latencies.items():
                transition_name = _slo_text(transition)
                latency = _non_negative_float(raw_latency)
                if transition_name is None or latency is None:
                    continue
                latency_samples.setdefault(transition_name, []).append(latency)
                recorded_transitions.add(transition_name)
        previous_phase = details.get("previous_phase")
        latency = _non_negative_float(details.get("latency_ms_from_previous"))
        if previous_phase is not None and latency is not None:
            transition = f"{previous_phase}->{row.event_type}"
            if transition not in recorded_transitions:
                latency_samples.setdefault(transition, []).append(latency)

        if row.event_type in _TERMINAL_REASON_EVENTS:
            reason = _slo_text(details.get("reason"))
            if reason is not None:
                lane = _slo_text(details.get("lane")) or "unknown"
                source = _slo_text(details.get("trigger_source")) or "unknown"
                lane_summary = terminal_reasons.setdefault(lane, {})
                source_summary = lane_summary.setdefault(source, {})
                source_summary[reason] = source_summary.get(reason, 0) + 1

        if row.event_type != _CONSUMER_HEALTH_EVENT:
            continue
        consumer = _slo_text(details.get("consumer")) or "unknown"
        stats = consumer_stats.setdefault(
            consumer,
            {
                "observed_event_count": 0,
                "recovery_count": 0,
                "unavailable_event_count": 0,
                "lag_event_count": 0,
                "last_available": None,
                "last_recovery_reason": None,
                "last_observed_at": None,
            },
        )
        stats["observed_event_count"] = int(stats["observed_event_count"]) + 1
        if details.get("lag") is True:
            stats["lag_event_count"] = int(stats["lag_event_count"]) + 1
        if details.get("recovery") is True:
            stats["recovery_count"] = int(stats["recovery_count"]) + 1
            recovery_reason = _slo_text(details.get("reason"))
            if recovery_reason is not None:
                stats["last_recovery_reason"] = recovery_reason
        if details.get("available") is False:
            stats["unavailable_event_count"] = (
                int(stats["unavailable_event_count"]) + 1
            )
        observed_at = row.occurred_at
        last_observed_at = stats["last_observed_at"]
        if last_observed_at is None or observed_at > last_observed_at:
            stats["last_observed_at"] = observed_at
            available = details.get("available")
            stats["last_available"] = (
                available if isinstance(available, bool) else None
            )

    return DecisionSLOResponse(
        status=OperationalStatus.READY if rows else OperationalStatus.NO_DATA,
        window=cast(Literal["1h", "6h", "24h", "7d"], window),
        window_start=window_start,
        window_end=window_end,
        persisted_event_count=len(rows),
        truncated=truncated,
        phase_latency={
            transition: DecisionSLOLatencyResponse(
                sample_count=len(values),
                p50_ms=_percentile(values, 0.50),
                p95_ms=_percentile(values, 0.95),
                max_ms=max(values),
            )
            for transition, values in sorted(latency_samples.items())
        },
        terminal_reasons=terminal_reasons,
        consumers=[
            DecisionSLOConsumerResponse(
                consumer=consumer,
                observed_event_count=int(stats["observed_event_count"]),
                recovery_count=int(stats["recovery_count"]),
                unavailable_event_count=int(stats["unavailable_event_count"]),
                lag_event_count=int(stats["lag_event_count"]),
                last_available=stats["last_available"],
                last_recovery_reason=stats["last_recovery_reason"],
                last_observed_at=stats["last_observed_at"],
            )
            for consumer, stats in sorted(consumer_stats.items())
        ],
    )


def _non_negative_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) and result >= 0 else None


def _slo_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = position - lower_index
    return ordered[lower_index] + (
        ordered[upper_index] - ordered[lower_index]
    ) * weight


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class DecisionSLOQueries:
    """Deep query module for historical decision-path telemetry."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse:
        """Aggregate bounded historical decision and consumer telemetry."""

        duration = _DECISION_SLO_WINDOWS.get(window)
        if duration is None:
            raise ValueError(
                "window must be one of: "
                + ", ".join(sorted(_DECISION_SLO_WINDOWS))
            )
        window_end = _as_utc(self._clock())
        window_start = window_end - duration
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.scalars(
                        select(StrategyRuntimeEventRow)
                        .where(
                            StrategyRuntimeEventRow.occurred_at >= window_start,
                            StrategyRuntimeEventRow.occurred_at <= window_end,
                            StrategyRuntimeEventRow.event_type.in_(
                                _DECISION_SLO_EVENT_TYPES
                            ),
                        )
                        .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                        .limit(_DECISION_SLO_MAX_EVENTS + 1)
                    )
                ).all()
            )
        truncated = len(rows) > _DECISION_SLO_MAX_EVENTS
        if truncated:
            rows = rows[:_DECISION_SLO_MAX_EVENTS]
        return _decision_slo_response(
            rows,
            window=window,
            window_start=window_start,
            window_end=window_end,
            truncated=truncated,
        )


__all__ = ["DecisionSLOQueries"]
