"""Domain models and pure services for execution readiness and progress contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


class ExecutionReadiness(StrEnum):
    """Execution readiness states enforcing non-blocking degraded operations."""

    INDEPENDENT_EXECUTABLE = "independent_executable"
    PROGRESS_LAGGING = "progress_lagging"
    STALLED = "stalled"


@dataclass(frozen=True, slots=True)
class ProgressFreshnessSLA:
    """Configurable SLA boundaries for progress evaluation."""

    max_lag_seconds_for_execution: float = 90.0
    max_lag_seconds_for_stall: float = 300.0
    allow_exits_during_lag: bool = True

    def __post_init__(self) -> None:
        if self.max_lag_seconds_for_execution <= 0:
            raise ValueError("max_lag_seconds_for_execution must be positive")
        if self.max_lag_seconds_for_stall <= self.max_lag_seconds_for_execution:
            raise ValueError(
                "max_lag_seconds_for_stall must be strictly greater than"
                "max_lag_seconds_for_execution"
            )


@dataclass(frozen=True, slots=True)
class ReadinessAssessment:
    """Structured assessment result for execution readiness."""

    readiness: ExecutionReadiness
    lag_seconds: float
    allows_entries: bool
    allows_exits: bool
    reason: str
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.evaluated_at.tzinfo is None:
            raise ValueError("evaluated_at must be timezone-aware")


class ReadinessEvaluator:
    """Pure domain service evaluating execution readiness without blocking.

    Invariants:
    - Never throws;
    - Strictly preserves non-blocking loop execution;
    - Restricts new risk (entries) when lagging while maintaining exit privileges.
    """

    DEFAULT_SLA = ProgressFreshnessSLA()

    @classmethod
    def evaluate(
        cls,
        *,
        current_time: datetime,
        watermark_time: datetime | None,
        reconciliation_gap: Decimal = Decimal("0"),
        sla: ProgressFreshnessSLA | None = None,
        context_reason: str = "",
    ) -> ReadinessAssessment:
        effective_sla = sla or cls.DEFAULT_SLA

        if current_time.tzinfo is None:
            raise ValueError("current_time must be timezone-aware")

        if watermark_time is None:
            return ReadinessAssessment(
                readiness=ExecutionReadiness.PROGRESS_LAGGING,
                lag_seconds=float("inf"),
                allows_entries=False,
                allows_exits=effective_sla.allow_exits_during_lag,
                reason="watermark_missing",
                evaluated_at=current_time,
            )

        if watermark_time.tzinfo is None:
            raise ValueError("watermark_time must be timezone-aware")

        lag = (current_time - watermark_time).total_seconds()
        # Handle micro-clockskew gracefully
        if lag < 0:
            lag = 0.0

        if lag > effective_sla.max_lag_seconds_for_stall:
            return ReadinessAssessment(
                readiness=ExecutionReadiness.STALLED,
                lag_seconds=lag,
                allows_entries=False,
                allows_exits=False,
                reason=f"staleness_threshold_exceeded:{lag:.1f}s",
                evaluated_at=current_time,
            )

        reconciliation_unclean = reconciliation_gap != Decimal("0")

        if lag > effective_sla.max_lag_seconds_for_execution or reconciliation_unclean:
            lag_reasons: list[str] = []
            if lag > effective_sla.max_lag_seconds_for_execution:
                lag_reasons.append(f"lag:{lag:.1f}s")
            if reconciliation_unclean:
                lag_reasons.append(f"reconciliation_gap:{reconciliation_gap}")
            if context_reason:
                lag_reasons.append(context_reason)

            return ReadinessAssessment(
                readiness=ExecutionReadiness.PROGRESS_LAGGING,
                lag_seconds=lag,
                allows_entries=False,
                allows_exits=effective_sla.allow_exits_during_lag,
                reason=";".join(lag_reasons),
                evaluated_at=current_time,
            )

        return ReadinessAssessment(
            readiness=ExecutionReadiness.INDEPENDENT_EXECUTABLE,
            lag_seconds=lag,
            allows_entries=True,
            allows_exits=True,
            reason="fresh",
            evaluated_at=current_time,
        )


__all__ = [
    "ExecutionReadiness",
    "ProgressFreshnessSLA",
    "ReadinessAssessment",
    "ReadinessEvaluator",
]
