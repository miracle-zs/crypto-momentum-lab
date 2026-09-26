"""Shared performance summary builder for dashboard queries.

Centralises the construction of AccountPerformanceSummaryResponse from
raw equity snapshots and cash-flow correction rows, ensuring a single
source of truth for metric calculation and coverage semantics.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.performance.account_performance import (
    AccountPerformanceCalculator,
)
from crypto_momentum_lab.domain.performance.metric_models import (
    AccountEquityCut,
    CashFlowFact,
    CoverageReceipt,
    MetricFamily,
    MetricSpec,
    ValuationPoint,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountPerformanceSummaryResponse,
)


def build_cash_flow_facts(
    cf_rows: Sequence[Any],
) -> tuple[CashFlowFact, ...]:
    """Converts ORM cash-flow correction rows into domain CashFlowFacts."""
    return tuple(
        CashFlowFact(
            correction_id=row.correction_id,
            account_label=row.account_label,
            amount=row.amount,
            cash_flow_type=row.cash_flow_type,
            effective_at=row.effective_at,
            reason=row.reason or "audit",
            approval_ref=row.approval_ref or "system",
            evidence_hash=row.evidence_hash or "0" * 64,
        )
        for row in cf_rows
    )


def build_valuation_points(
    equity_rows: Sequence[Any],
) -> tuple[ValuationPoint, ...]:
    """Converts ORM equity snapshot rows into domain ValuationPoints."""
    return tuple(
        ValuationPoint(
            timestamp=r.observed_at,
            equity=r.wallet_balance,
        )
        for r in equity_rows
    )


# ── Canonical metric spec definitions ──────────────────────────────
_METRIC_SPECS = (
    MetricSpec("cash_flow_adjusted_pnl", MetricFamily.CASH_FLOW_ADJUSTED_PNL, "USDT"),
    MetricSpec("net_equity_delta", MetricFamily.NET_EQUITY_DELTA, "USDT"),
    MetricSpec("twr", MetricFamily.TIME_WEIGHTED_RETURN, "ratio"),
    MetricSpec("modified_dietz", MetricFamily.MODIFIED_DIETZ, "ratio"),
    MetricSpec("mwr", MetricFamily.MONEY_WEIGHTED_RETURN, "ratio"),
)


_HEX_64_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
_PLACEHOLDER_APPROVALS = frozenset({"", "system", "unknown", "none", "n/a", "tbd"})


def compute_cash_flow_evidence_hash(
    *,
    correction_id: str,
    account_label: str,
    amount: Decimal,
    cash_flow_type: str,
    effective_at: datetime,
    reason: str,
    approval_ref: str,
) -> str:
    """Canonical content hash for a cash-flow correction.

    Certification requires ``evidence_hash`` to equal this value; a
    well-formed but unrelated hex string is not proof of the record.
    """
    payload = "|".join(
        (
            correction_id,
            account_label,
            str(amount),
            cash_flow_type,
            effective_at.isoformat(),
            reason,
            approval_ref,
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_content_hash_matches(row: Any, ev_hash_str: str) -> bool | None:
    """Return True/False when content fields allow a hash check, else None."""
    fields = {
        name: getattr(row, name, None)
        for name in (
            "correction_id",
            "account_label",
            "amount",
            "cash_flow_type",
            "effective_at",
            "reason",
            "approval_ref",
        )
    }
    if any(v is None for v in fields.values()):
        return None
    eff_at = fields["effective_at"]
    if not isinstance(eff_at, datetime):
        return None
    expected = compute_cash_flow_evidence_hash(
        correction_id=str(fields["correction_id"]),
        account_label=str(fields["account_label"]),
        amount=Decimal(str(fields["amount"])),
        cash_flow_type=str(fields["cash_flow_type"]),
        effective_at=eff_at,
        reason=str(fields["reason"]),
        approval_ref=str(fields["approval_ref"]),
    )
    return expected.lower() == ev_hash_str.lower()


def _assess_coverage(
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
    equity_rows: Sequence[Any] = (),
    max_equity_gap: timedelta | None = None,
    is_empty_proven: bool = False,
) -> tuple[bool, str, str]:
    """Determine coverage status from cash-flow facts from first principles.

    Returns (is_certified, coverage_status, coverage_proof).

    A window is certified only when:
    1. Equity valuation points bracket the window (completeness proof);
    2. Every fact has a required effective_at inside the window;
    3. Every fact carries a real approval_ref (not a placeholder);
    4. Every fact's evidence_hash is non-placeholder hex and, when content
       fields are available, equals the canonical content hash.
    """
    s_time = (
        start_time if start_time.tzinfo is not None else start_time.replace(tzinfo=UTC)
    )
    e_time = (
        end_time if end_time.tzinfo is not None else end_time.replace(tzinfo=UTC)
    )

    if equity_rows:
        if len(equity_rows) < 2:
            return (
                False,
                "uncertified",
                "uncertified_equity_window_incomplete",
            )
        first_at = getattr(equity_rows[0], "observed_at", None)
        last_at = getattr(equity_rows[-1], "observed_at", None)
        if first_at is None or last_at is None:
            return (
                False,
                "uncertified",
                "uncertified_equity_window_incomplete",
            )
        first_utc = (
            first_at if first_at.tzinfo is not None else first_at.replace(tzinfo=UTC)
        )
        last_utc = (
            last_at if last_at.tzinfo is not None else last_at.replace(tzinfo=UTC)
        )
        if first_utc > s_time or last_utc < e_time:
            return (
                False,
                "uncertified",
                "uncertified_equity_window_incomplete",
            )
        prev_utc = first_utc
        for r in equity_rows[1:]:
            cur_at = getattr(r, "observed_at", None)
            if cur_at is None:
                return (
                    False,
                    "uncertified",
                    "uncertified_equity_window_incomplete",
                )
            cur_utc = (
                cur_at if cur_at.tzinfo is not None else cur_at.replace(tzinfo=UTC)
            )
            if cur_utc < prev_utc:
                return (
                    False,
                    "uncertified",
                    "uncertified_equity_sequence_out_of_order",
                )
            if max_equity_gap is not None and (cur_utc - prev_utc) > max_equity_gap:
                return (
                    False,
                    "uncertified",
                    "uncertified_equity_window_gap_detected",
                )
            prev_utc = cur_utc

    if not cf_rows:
        if is_empty_proven:
            return (
                True,
                "confirmed",
                "proven_zero_cash_flows",
            )
        return (
            False,
            "uncertified",
            "uncertified_zero_cash_flow_facts",
        )


    for r in cf_rows:
        rec_id = getattr(r, "correction_id", "unknown")
        ev_hash = getattr(r, "evidence_hash", None)
        if not ev_hash:
            return (
                False,
                "uncertified",
                f"uncertified_missing_evidence_hash_in_record_{rec_id}",
            )
        ev_hash_str = str(ev_hash).strip()
        if not _HEX_64_PATTERN.fullmatch(ev_hash_str):
            return (
                False,
                "uncertified",
                f"uncertified_invalid_evidence_hash_format_in_record_{rec_id}",
            )
        if ev_hash_str.lower() == "0" * 64:
            return (
                False,
                "uncertified",
                f"uncertified_zero_placeholder_evidence_in_record_{rec_id}",
            )
        appr = getattr(r, "approval_ref", None)
        appr_str = str(appr).strip().lower() if appr is not None else ""
        if appr_str in _PLACEHOLDER_APPROVALS:
            return (
                False,
                "uncertified",
                f"uncertified_missing_approval_ref_in_record_{rec_id}",
            )
        eff_at = getattr(r, "effective_at", None)
        if eff_at is None:
            return (
                False,
                "uncertified",
                f"uncertified_missing_effective_at_in_record_{rec_id}",
            )
        eff_time = (
            eff_at if eff_at.tzinfo is not None else eff_at.replace(tzinfo=UTC)
        )
        if eff_time < s_time or eff_time > e_time:
            return (
                False,
                "uncertified",
                f"uncertified_cash_flow_out_of_interval_{rec_id}",
            )
        content_ok = _row_content_hash_matches(r, ev_hash_str)
        if content_ok is not True:
            return (
                False,
                "uncertified",
                f"uncertified_evidence_hash_content_mismatch_{rec_id}",
            )

    return (
        True,
        "confirmed",
        f"audited_records_count_{len(cf_rows)}_with_verified_evidence",
    )


def build_performance_summary(
    *,
    account_label: str,
    equity_rows: Sequence[Any],
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
    max_equity_gap: timedelta | None = None,
    environment: str = "live",
    asset: str = "USDT",
    valuation_basis: str = "wallet",
    is_empty_proven: bool = False,
) -> AccountPerformanceSummaryResponse | None:
    """Builds a fully-audited AccountPerformanceSummaryResponse.

    Returns ``None`` when there are no equity snapshots.
    """
    if not equity_rows:
        return None

    effective_max_gap = max_equity_gap
    if effective_max_gap is None and len(equity_rows) >= 3:
        first_at = getattr(equity_rows[0], "observed_at", None)
        last_at = getattr(equity_rows[-1], "observed_at", None)
        if first_at is not None and last_at is not None and last_at > first_at:
            expected_step = (last_at - first_at) / (len(equity_rows) - 1)
            effective_max_gap = max(timedelta(hours=2), expected_step * 4)

    start_eq: Decimal = equity_rows[0].wallet_balance
    end_eq: Decimal = equity_rows[-1].wallet_balance

    cash_facts = build_cash_flow_facts(cf_rows)
    vps = build_valuation_points(equity_rows)

    is_certified, coverage_status, coverage_proof = _assess_coverage(
        cf_rows,
        start_time,
        end_time,
        equity_rows=equity_rows,
        max_equity_gap=effective_max_gap,
        is_empty_proven=is_empty_proven,
    )

    coverage_receipt = CoverageReceipt(
        account_label=account_label,
        asset=asset,
        interval_start=start_time,
        interval_end=end_time,
        source="account_balance_snapshots+cash_flow_corrections",
        is_gapless=is_certified,
        is_empty_proven=(is_certified and len(cf_rows) == 0),
        details=coverage_proof,
    )

    cut = AccountEquityCut(
        account_label=account_label,
        start_equity=start_eq,
        end_equity=end_eq,
        start_time=start_time,
        end_time=end_time,
        cash_flows=cash_facts,
        valuation_points=vps,
        valuation_basis=valuation_basis,
        asset=asset,
        environment=environment,
        coverage_receipt=coverage_receipt,
        as_of=end_time,
    )

    metrics = {
        spec.name: AccountPerformanceCalculator.calculate(spec, cut)
        for spec in _METRIC_SPECS
    }

    pnl = metrics["cash_flow_adjusted_pnl"]
    delta = metrics["net_equity_delta"]
    twr = metrics["twr"]
    dietz = metrics["modified_dietz"]
    mwr = metrics["mwr"]

    # Uncertified windows must not publish return figures as if audited.
    def _certified_value(metric: Any) -> str | None:
        if not is_certified:
            return None
        return str(metric.value) if metric.value is not None else None

    return AccountPerformanceSummaryResponse(
        start_equity=str(start_eq),
        end_equity=str(end_eq),
        net_equity_delta=_certified_value(delta),
        cash_flow_adjusted_pnl=_certified_value(pnl),
        twr=_certified_value(twr),
        modified_dietz=_certified_value(dietz),
        mwr=_certified_value(mwr),
        status=twr.status.value,
        is_certified=is_certified,
        coverage_status=coverage_status,
        cash_flow_coverage_proof=coverage_proof,
        cash_flow_corrections_count=len(cf_rows),
        valuation_basis=valuation_basis,
        asset=asset,
        environment=environment,
        method=twr.method,
    )


def build_performance_summary_dict(
    *,
    account_label: str,
    equity_rows: Sequence[Any],
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
    max_equity_gap: timedelta | None = None,
    environment: str = "live",
    asset: str = "USDT",
    valuation_basis: str = "wallet",
    is_empty_proven: bool = False,
) -> dict[str, object]:
    """Dict variant for the /api/account-performance endpoint."""
    summary = build_performance_summary(
        account_label=account_label,
        equity_rows=equity_rows,
        cf_rows=cf_rows,
        start_time=start_time,
        end_time=end_time,
        max_equity_gap=max_equity_gap,
        environment=environment,
        asset=asset,
        valuation_basis=valuation_basis,
        is_empty_proven=is_empty_proven,
    )

    if summary is None:
        return {"status": "no_data", "account_label": account_label}

    return {
        "account_label": account_label,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "start_equity": summary.start_equity,
        "end_equity": summary.end_equity,
        "net_equity_delta": summary.net_equity_delta,
        "cash_flow_adjusted_pnl": summary.cash_flow_adjusted_pnl,
        "twr": summary.twr,
        "modified_dietz": summary.modified_dietz,
        "mwr": summary.mwr,
        "status": summary.status,
        "is_certified": summary.is_certified,
        "coverage_status": summary.coverage_status,
        "cash_flow_coverage_proof": summary.cash_flow_coverage_proof,
        "cash_flow_corrections_count": summary.cash_flow_corrections_count,
        "valuation_basis": summary.valuation_basis,
        "asset": summary.asset,
        "environment": summary.environment,
        "method": summary.method,
    }
