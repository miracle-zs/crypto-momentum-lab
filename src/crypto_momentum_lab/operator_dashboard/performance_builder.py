"""Shared performance summary builder for dashboard queries.

Centralises the construction of AccountPerformanceSummaryResponse from
raw equity snapshots and cash-flow correction rows, ensuring a single
source of truth for metric calculation and coverage semantics.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
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


def _assess_coverage(
    cf_rows: Sequence[Any],
    start_time: datetime,
    end_time: datetime,
) -> tuple[bool, str, str]:
    """Determine coverage status from cash-flow facts.

    Returns (is_certified, coverage_status, coverage_proof).

    A window is only certified when *every* sub-period between
    successive cash-flow events is explicitly covered.  A mere
    non-zero count of correction records is not sufficient—it could
    be a single deposit record that says nothing about the rest of
    the window.  Until a proper coverage-interval checker is built,
    we conservatively mark everything as uncertified and report
    the raw fact count so the operator can reason about it.
    """
    if not cf_rows:
        return (
            False,
            "uncertified",
            "uncertified_zero_cash_flow_facts",
        )
    # Even with records present we cannot yet prove full-window
    # coverage, so we stay conservative.
    return (
        False,
        "uncertified",
        f"uncertified_has_{len(cf_rows)}_facts_but_coverage_unproven",
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
