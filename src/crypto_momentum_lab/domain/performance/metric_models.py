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
    MODIFIED_DIETZ = "modified_dietz"
    MAX_DRAWDOWN = "max_drawdown"


class MetricStatus(StrEnum):
    """Reliability and integrity status of an evaluated performance metric."""

    CONFIRMED = "confirmed"
    UNVERIFIED_ESTIMATE = "unverified_estimate"
    INSUFFICIENT_COVERAGE = "insufficient_coverage"
    STALE = "stale"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class LiveCashFlowAdjustment:
    account_label: str
    effective_at: datetime
    amount: Decimal
    cash_flow_type: str = "deposit"


@dataclass(frozen=True, slots=True)
class CashFlowFact:
    """Authoritative immutable cash flow record with proof and approval lineage (R6)."""

    correction_id: str
    account_label: str
    amount: Decimal
    cash_flow_type: str  # deposit, withdrawal, fee_rebate, audit_adjustment, etc.
    effective_at: datetime
    reason: str
    approval_ref: str
    evidence_hash: str
    asset: str = "USDT"
    external_identity: str | None = None
    source: str = "manual_correction"
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
        if not self.asset.strip():
            raise ValueError("asset must not be empty")
        if not self.source.strip():
            raise ValueError("source must not be empty")


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
class CoverageReceipt:
    """Receipt proving evidence coverage over an account/asset interval (R6)."""

    account_label: str
    asset: str
    interval_start: datetime
    interval_end: datetime
    source: str
    cursor_boundary: datetime | str | None = None
    gaps: tuple[tuple[datetime, datetime], ...] = ()
    is_gapless: bool = True
    is_empty_proven: bool = False
    revision: str | int = "v1"
    as_of: datetime = field(default_factory=lambda: datetime.now(UTC))
    details: str | dict[str, Any] = ""

    def __post_init__(self) -> None:
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.asset.strip():
            raise ValueError("asset must not be empty")
        if self.interval_start.tzinfo is None or self.interval_end.tzinfo is None:
            raise ValueError("interval bounds must be timezone-aware")
        if self.interval_end < self.interval_start:
            raise ValueError("interval_end cannot precede interval_start")
        if self.gaps and self.is_gapless:
            raise ValueError("is_gapless cannot be True when gaps are present")
        if self.is_empty_proven:
            if not self.is_gapless or self.gaps:
                raise ValueError("cannot claim is_empty_proven when coverage has gaps")


@dataclass(frozen=True, slots=True)
class AccountEquityCut:
    """Bounded, immutable point-in-time equity cut consumed by evaluation (R6)."""

    account_label: str
    start_equity: Decimal
    end_equity: Decimal
    start_time: datetime
    end_time: datetime
    cash_flows: tuple[CashFlowFact, ...] = ()
    valuation_points: tuple[ValuationPoint, ...] = ()
    realized_pnl: Decimal = Decimal("0.00")
    unrealized_pnl: Decimal = Decimal("0.00")
    has_unknown_cash_flows: bool = False
    valuation_basis: str = "wallet"
    asset: str = "USDT"
    environment: str = "live"
    currency_conversion_source: str | None = None
    coverage_receipt: CoverageReceipt | None = None
    source_refs: tuple[str, ...] = ()
    as_of: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.asset.strip():
            raise ValueError("asset must not be empty")
        if not self.valuation_basis.strip():
            raise ValueError("valuation_basis must not be empty")
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")
        if self.end_time < self.start_time:
            raise ValueError("end_time cannot precede start_time")
        if self.start_equity < Decimal("0") or self.end_equity < Decimal("0"):
            raise ValueError("equity values cannot be negative")
        if self.coverage_receipt is not None:
            if self.coverage_receipt.account_label != self.account_label:
                raise ValueError("coverage_receipt account_label does not match cut")
            if self.coverage_receipt.asset != self.asset:
                raise ValueError("coverage_receipt asset does not match cut")
            if self.coverage_receipt.interval_start > self.start_time:
                raise ValueError(
                    "coverage_receipt interval_start is after cut start_time"
                )
            if self.coverage_receipt.interval_end < self.end_time:
                raise ValueError("coverage_receipt interval_end is before cut end_time")
        if (
            any(cf.asset != self.asset for cf in self.cash_flows)
            and not self.currency_conversion_source
        ):
            raise ValueError(
                "multi-asset cash flows detected without explicit "
                "currency_conversion_source"
            )


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """Specification of an intended performance metric calculation."""

    name: str
    family: MetricFamily
    version: str = field(default="v1", kw_only=True)
    unit: str = field(default="USDT", kw_only=True)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if not self.version.strip():
            raise ValueError("version must not be empty")


@dataclass(frozen=True, slots=True)
class MetricValue:
    """Authoritative outcome of metric evaluation preserving provenance
    and coverage (R6).
    """

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
    method: str = ""
    coverage: CoverageReceipt | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.metric_name.strip():
            raise ValueError("metric_name must not be empty")
        if self.interval_start.tzinfo is None or self.interval_end.tzinfo is None:
            raise ValueError("interval bounds must be timezone-aware")
        if self.interval_end < self.interval_start:
            raise ValueError("interval_end cannot precede interval_start")
