"""Shared performance summary builder for dashboard queries.

Centralises the construction of AccountPerformanceSummaryResponse from
raw equity snapshots and cash-flow correction rows, ensuring a single
source of truth for metric calculation and coverage semantics.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.performance.account_performance import (
    AccountPerformanceCalculator,
)
from crypto_momentum_lab.domain.performance.metric_models import (
    AccountEquityCut,
    CashFlowFact,
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


def _assess_coverage(
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
) -> tuple[bool, str, str]:
    """Determine coverage status from cash-flow facts from first principles.

    Returns (is_certified, coverage_status, coverage_proof).

    A window is certified when:
    1. Cash-flow facts are present;
    2. Every fact contains verified cryptographic evidence (strictly valid 64-char
       hex hash, non-zero/placeholder) and a non-empty approval reference;
    3. Every fact's effective_at falls strictly within [start_time, end_time].
    If no facts exist or if any fact has unverified/dummy evidence or falls outside
    the evaluated interval, it remains uncertified.
    """
    if not cf_rows:
        return (
            False,
            "uncertified",
            "uncertified_zero_cash_flow_facts",
        )

    s_time = (
        start_time if start_time.tzinfo is not None else start_time.replace(tzinfo=UTC)
    )
    e_time = (
        end_time if end_time.tzinfo is not None else end_time.replace(tzinfo=UTC)
    )

    for r in cf_rows:
        ev_hash = getattr(r, "evidence_hash", None)
        rec_id = getattr(r, "correction_id", "unknown")
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
        if not appr or not str(appr).strip():
            return (
                False,
                "uncertified",
                f"uncertified_missing_approval_ref_in_record_{rec_id}",
            )
        eff_at = getattr(r, "effective_at", None)
        if eff_at is not None:
            eff_time = (
                eff_at if eff_at.tzinfo is not None else eff_at.replace(tzinfo=UTC)
            )
            if eff_time < s_time or eff_time > e_time:
                return (
                    False,
                    "uncertified",
                    f"uncertified_cash_flow_out_of_interval_{rec_id}",
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
) -> AccountPerformanceSummaryResponse | None:
    """Builds a fully-audited AccountPerformanceSummaryResponse.

    Returns ``None`` when there are no equity snapshots.
    """
    if not equity_rows:
        return None

    start_eq: Decimal = equity_rows[0].wallet_balance
    end_eq: Decimal = equity_rows[-1].wallet_balance

    cash_facts = build_cash_flow_facts(cf_rows)
    vps = build_valuation_points(equity_rows)

    cut = AccountEquityCut(
        account_label=account_label,
        start_equity=start_eq,
        end_equity=end_eq,
        start_time=start_time,
        end_time=end_time,
        cash_flows=cash_facts,
        valuation_points=vps,
        as_of=end_time,
    )

    metrics = {
        spec.name: AccountPerformanceCalculator.calculate(spec, cut)
        for spec in _METRIC_SPECS
    }

    is_certified, coverage_status, coverage_proof = _assess_coverage(
        cf_rows, start_time, end_time,
    )

    pnl = metrics["cash_flow_adjusted_pnl"]
    delta = metrics["net_equity_delta"]
    twr = metrics["twr"]
    dietz = metrics["modified_dietz"]
    mwr = metrics["mwr"]

    return AccountPerformanceSummaryResponse(
        start_equity=str(start_eq),
        end_equity=str(end_eq),
        net_equity_delta=(
            str(delta.value) if delta.value is not None else None
        ),
        cash_flow_adjusted_pnl=(
            str(pnl.value) if pnl.value is not None else None
        ),
        twr=str(twr.value) if twr.value is not None else None,
        modified_dietz=(
            str(dietz.value) if dietz.value is not None else None
        ),
        mwr=str(mwr.value) if mwr.value is not None else None,
        status=twr.status.value,
        is_certified=is_certified,
        coverage_status=coverage_status,
        cash_flow_coverage_proof=coverage_proof,
        cash_flow_corrections_count=len(cf_rows),
    )


def build_performance_summary_dict(
    *,
    account_label: str,
    equity_rows: Sequence[Any],
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, object]:
    """Dict variant for the /api/account-performance endpoint."""
    summary = build_performance_summary(
        account_label=account_label,
        equity_rows=equity_rows,
        cf_rows=cf_rows,
        start_time=start_time,
        end_time=end_time,
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
    }
