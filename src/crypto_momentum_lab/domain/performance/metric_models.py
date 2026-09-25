"""Domain models for account performance metrics and cash flow facts (R5).

Obeys Astra Architecture Blueprint 2026-09-25:
- Unifies identity and origin of cash flows, fees, funding, and equity observations;
- Reuses immutable facts from AccountJournal;
- Distinguishes NET_EQUITY_DELTA, CASH_FLOW_ADJUSTED_PNL, TWR, MWR without aliasing;
- Honors coverage and explicitly marks unverified/insufficient data instead of guessing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class MetricFamily(StrEnum):
    """Authoritative metric families avoiding misrepresentation or naming collisions."""

    NET_EQUITY_DELTA = "net_equity_delta"
    CASH_FLOW_ADJUSTED_PNL = "cash_flow_adjusted_pnl"
    TIME_WEIGHTED_RETURN = "time_weighted_return"
    MONEY_WEIGHTED_RETURN = "money_weighted_return"
    MAX_DRAWDOWN = "max_drawdown"
    SHARPE_RATIO = "sharpe_ratio"


class MetricStatus(StrEnum):
    """Reliability and integrity status of an evaluated performance metric."""

    CONFIRMED = "confirmed"
    UNVERIFIED_ESTIMATE = "unverified_estimate"
    INSUFFICIENT_COVERAGE = "insufficient_coverage"
    STALE = "stale"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CashFlowFact:
    """Authoritative immutable cash flow record with proof and approval lineage."""

    correction_id: str
    account_label: str
    amount: Decimal
    cash_flow_type: str  # deposit, withdrawal, fee_rebate, audit_adjustment
    effective_at: datetime
    reason: str
    approval_ref: str
    evidence_hash: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.correction_id.strip():
            raise ValueError("correction_id must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.amount == Decimal("0"):
            raise ValueError("cash flow amount cannot be zero")
        if not self.cash_flow_type.strip():
            raise ValueError("cash_flow_type must not be empty")
        if self.effective_at.tzinfo is None:
            raise ValueError("effective_at must be timezone-aware")
        if not self.reason.strip():
            raise ValueError("reason must not be empty")
        if not self.approval_ref.strip():
            raise ValueError("approval_ref must not be empty")

    def to_live_adjustment(self) -> Any:
        from crypto_momentum_lab.operator_dashboard.common_equity import (
            LiveCashFlowAdjustment,
        )

        return LiveCashFlowAdjustment(
            account_label=self.account_label,
            effective_at=self.effective_at,
            amount=self.amount,
            cash_flow_type=self.cash_flow_type,
        )


@dataclass(frozen=True, slots=True)
class ValuationPoint:
    """Subinterval valuation point used for continuous returns and TWR."""

    timestamp: datetime
    equity: Decimal

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        if self.equity < Decimal("0"):
            raise ValueError("equity cannot be negative")


@dataclass(frozen=True, slots=True)
class AccountEquityCut:
    """Bounded, immutable point-in-time equity cut consumed by performance evaluation.
    """

    account_label: str
    start_equity: Decimal
    end_equity: Decimal
    start_time: datetime
    end_time: datetime
    cash_flows: tuple[CashFlowFact, ...] = ()
    valuation_points: tuple[ValuationPoint, ...] = ()
    realized_pnl: Decimal = Decimal("0.00")
    unrealized_pnl: Decimal = Decimal("0.00")
    fees_paid: Decimal = Decimal("0.00")
    funding_fees: Decimal = Decimal("0.00")
    has_unknown_cash_flows: bool = False
    as_of: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")
        if self.end_time < self.start_time:
            raise ValueError("end_time cannot precede start_time")
        if self.start_equity < Decimal("0") or self.end_equity < Decimal("0"):
            raise ValueError("equity values cannot be negative")


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """Specification of an intended performance metric calculation."""

    name: str
    family: MetricFamily
    version: str = "v1"
    unit: str = "USDT"
    benchmark: str | None = None
    annualization_factor: int = 365

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if not self.version.strip():
            raise ValueError("version must not be empty")


@dataclass(frozen=True, slots=True)
class MetricValue:
    """Authoritative outcome of metric evaluation preserving provenance and coverage."""

    metric_name: str
    family: MetricFamily
    metric_version: str
    value: Decimal | None
    unit: str
    interval_start: datetime
    interval_end: datetime
    as_of: datetime
    source_refs: tuple[str, ...]
    status: MetricStatus
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.metric_name.strip():
            raise ValueError("metric_name must not be empty")
        if self.interval_start.tzinfo is None or self.interval_end.tzinfo is None:
            raise ValueError("interval bounds must be timezone-aware")
        if self.interval_end < self.interval_start:
            raise ValueError("interval_end cannot precede interval_start")
