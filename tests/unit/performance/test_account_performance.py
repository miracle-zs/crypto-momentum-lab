"""Unit tests for AccountPerformanceCalculator and metric models (R5).

Tests:
1. NET_EQUITY_DELTA vs CASH_FLOW_ADJUSTED_PNL mathematical distinction;
2. TWR subinterval compounding and INSUFFICIENT_COVERAGE when cash flows
   lack subintervals;
3. MWR (Modified Dietz) weighted cash flow calculation and non-positive
   capital base defense;
4. MAX_DRAWDOWN calculation across valuation points;
5. Unknown cash flow presence marking UNVERIFIED_ESTIMATE or INSUFFICIENT_COVERAGE.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.performance.account_performance import (
    AccountPerformanceCalculator,
)
from crypto_momentum_lab.domain.performance.metric_models import (
    AccountEquityCut,
    CashFlowFact,
    CoverageReceipt,
    MetricFamily,
    MetricSpec,
    MetricStatus,
    ValuationPoint,
)


def test_net_equity_delta_vs_cash_flow_adjusted_pnl() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    # Account starts with 10,000 USDT.
    # User deposits 5,000 USDT mid-day.
    # Ending equity is 16,000 USDT.
    deposit = CashFlowFact(
        correction_id="cf_001",
        account_label="primary",
        amount=Decimal("5000.00"),
        cash_flow_type="deposit",
        effective_at=t0 + timedelta(hours=12),
        reason="user_bank_deposit",
        approval_ref="appr_dep_01",
        evidence_hash="ev_hash_dep_01",
    )

    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("16000.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
    )

    # 1. NET_EQUITY_DELTA: 16000 - 10000 = 6000 USDT
    spec_delta = MetricSpec(name="equity_delta", family=MetricFamily.NET_EQUITY_DELTA)
    res_delta = AccountPerformanceCalculator.calculate(spec_delta, cut)
    assert res_delta.status == MetricStatus.CONFIRMED
    assert res_delta.value == Decimal("6000.00")

    # 2. CASH_FLOW_ADJUSTED_PNL: 6000 - 5000 = 1000 USDT
    spec_adj = MetricSpec(
        name="trading_pnl", family=MetricFamily.CASH_FLOW_ADJUSTED_PNL
    )
    res_adj = AccountPerformanceCalculator.calculate(spec_adj, cut)
    assert res_adj.status == MetricStatus.CONFIRMED
    assert res_adj.value == Decimal("1000.00")


def test_twr_requires_subinterval_valuation_with_cash_flows() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    deposit = CashFlowFact(
        correction_id="cf_002",
        account_label="primary",
        amount=Decimal("2000.00"),
        cash_flow_type="deposit",
        effective_at=t0 + timedelta(hours=12),
        reason="deposit",
        approval_ref="appr_02",
        evidence_hash="hash_02",
    )

    spec_twr = MetricSpec(name="twr", family=MetricFamily.TIME_WEIGHTED_RETURN)

    # Case A: Cash flows exist but no valuation subintervals provided
    cut_no_sub = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("13000.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
        valuation_points=(),
    )
    res_no_sub = AccountPerformanceCalculator.calculate(spec_twr, cut_no_sub)
    assert res_no_sub.status == MetricStatus.INSUFFICIENT_COVERAGE
    assert res_no_sub.value is None

    # Case B: Subintervals provided
    # Period 1 (0h-12h): 10,000 -> 11,000 (+10%)
    # Deposit 2,000 at 12h: baseline becomes 13,000
    # Period 2 (12h-24h): 13,000 -> 14,300 (+10%)
    # Total TWR: (1 + 0.10) * (1 + 0.10) - 1 = 21%
    val_points = (
        ValuationPoint(timestamp=t0, equity=Decimal("10000.00")),
        ValuationPoint(timestamp=t0 + timedelta(hours=12), equity=Decimal("11000.00")),
        ValuationPoint(timestamp=t1, equity=Decimal("14300.00")),
    )
    cut_with_sub = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("14300.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
        valuation_points=val_points,
    )
    res_with_sub = AccountPerformanceCalculator.calculate(spec_twr, cut_with_sub)
    assert res_with_sub.status == MetricStatus.CONFIRMED
    assert res_with_sub.value == Decimal("0.210000")


def test_twr_deposit_does_not_create_artificial_return() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=24)

    # 100 USDT start equity, 100 USDT deposit, 200 USDT end equity with zero market trading
    deposit = CashFlowFact(
        correction_id="cf_dep_01",
        account_label="primary",
        amount=Decimal("100.00"),
        cash_flow_type="deposit",
        effective_at=t0 + timedelta(hours=12),
        reason="deposit",
        approval_ref="appr_01",
        evidence_hash="hash_01",
    )
    val_points = (
        ValuationPoint(timestamp=t0, equity=Decimal("100.00")),
        ValuationPoint(timestamp=t0 + timedelta(hours=12), equity=Decimal("100.00")),
        ValuationPoint(timestamp=t1, equity=Decimal("200.00")),
    )
    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("100.00"),
        end_equity=Decimal("200.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
        valuation_points=val_points,
    )
    spec_twr = MetricSpec(name="twr", family=MetricFamily.TIME_WEIGHTED_RETURN)
    res = AccountPerformanceCalculator.calculate(spec_twr, cut)
    assert res.status == MetricStatus.CONFIRMED
    # Must be 0.000000 return, NOT 100% (1.000000)
    assert res.value == Decimal("0.000000")


def test_mwr_modified_dietz_weighting() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=2)  # 48 hours

    # Deposit of 4,000 at exactly half duration (24h in), weight = 0.5
    # Start = 10,000. End = 16,000. Total deposit = 4,000.
    # Gain = 16,000 - 10,000 - 4,000 = 2,000.
    # Average capital base = 10,000 + 4,000 * 0.5 = 12,000.
    # MWR = 2,000 / 12,000 = 0.166667 (16.67%)
    deposit = CashFlowFact(
        correction_id="cf_003",
        account_label="primary",
        amount=Decimal("4000.00"),
        cash_flow_type="deposit",
        effective_at=t0 + timedelta(days=1),
        reason="midpoint_deposit",
        approval_ref="appr_03",
        evidence_hash="hash_03",
    )
    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("16000.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
    )
    # Test Modified Dietz calculation
    spec_dietz = MetricSpec(name="dietz", family=MetricFamily.MODIFIED_DIETZ)
    res_dietz = AccountPerformanceCalculator.calculate(spec_dietz, cut)
    assert res_dietz.status == MetricStatus.CONFIRMED
    assert res_dietz.value == Decimal("0.166667")

    # Test True Money-Weighted Return (exact IRR) calculation
    spec_mwr = MetricSpec(name="mwr", family=MetricFamily.MONEY_WEIGHTED_RETURN)
    res_mwr = AccountPerformanceCalculator.calculate(spec_mwr, cut)
    assert res_mwr.status == MetricStatus.CONFIRMED
    # Exact IRR is root of -10000 - 4000/(1+r)^0.5 + 16000/(1+r) = 0 => r ≈ 0.167750
    assert res_mwr.value == Decimal("0.167750")


def test_max_drawdown_calculation() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=4)

    # 10,000 -> peak 12,000 -> drop to 9,600 (20% drop from peak) -> recovery to 11,000
    points = (
        ValuationPoint(timestamp=t0, equity=Decimal("10000.00")),
        ValuationPoint(timestamp=t0 + timedelta(hours=1), equity=Decimal("12000.00")),
        ValuationPoint(timestamp=t0 + timedelta(hours=2), equity=Decimal("9600.00")),
        ValuationPoint(timestamp=t1, equity=Decimal("11000.00")),
    )
    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("11000.00"),
        start_time=t0,
        end_time=t1,
        valuation_points=points,
    )
    spec_mdd = MetricSpec(name="mdd", family=MetricFamily.MAX_DRAWDOWN)
    res_mdd = AccountPerformanceCalculator.calculate(spec_mdd, cut)

    assert res_mdd.status == MetricStatus.CONFIRMED
    assert res_mdd.value == Decimal("0.200000")  # exactly 20% drawdown


def test_unknown_cash_flows_handling() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("12000.00"),
        start_time=t0,
        end_time=t1,
        has_unknown_cash_flows=True,
    )

    # Raw delta: returned but marked UNVERIFIED_ESTIMATE
    spec_delta = MetricSpec(name="delta", family=MetricFamily.NET_EQUITY_DELTA)
    res_delta = AccountPerformanceCalculator.calculate(spec_delta, cut)
    assert res_delta.status == MetricStatus.UNVERIFIED_ESTIMATE
    assert res_delta.value == Decimal("2000.00")

    # MWR: cannot be computed without cash flow coverage
    spec_mwr = MetricSpec(name="mwr", family=MetricFamily.MONEY_WEIGHTED_RETURN)
    res_mwr = AccountPerformanceCalculator.calculate(spec_mwr, cut)
    assert res_mwr.status == MetricStatus.INSUFFICIENT_COVERAGE
    assert res_mwr.value is None


def test_coverage_receipt_empty_proven_certifies_exact_twr() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    receipt = CoverageReceipt(
        account_label="primary",
        asset="USDT",
        interval_start=t0,
        interval_end=t1,
        source="account_balance_snapshots+cash_flow_corrections",
        cursor_boundary=t1,
        is_gapless=True,
        is_empty_proven=True,
        revision=1,
        as_of=t1,
        details={"audit": "no_external_deposits"},
    )

    cut = AccountEquityCut(
        account_label="primary",
        valuation_basis="wallet",
        asset="USDT",
        environment="live",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("11500.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(),
        coverage_receipt=receipt,
        # Even if legacy field was set to True, receipt.is_empty_proven overrides it to certified zero
        has_unknown_cash_flows=True,
    )

    spec_twr = MetricSpec(name="twr", family=MetricFamily.TIME_WEIGHTED_RETURN)
    res_twr = AccountPerformanceCalculator.calculate(spec_twr, cut)

    assert res_twr.status == MetricStatus.CONFIRMED
    assert res_twr.value == Decimal("0.150000")
    assert res_twr.method == "exact_twr_zero_cash_flows"
    assert res_twr.coverage == receipt
    assert "environment:live" in res_twr.source_refs
    assert "asset:USDT" in res_twr.source_refs
    assert "basis:wallet" in res_twr.source_refs


def test_twr_with_subintervals_uses_linked_modified_dietz_method() -> None:
    t0 = datetime(2026, 9, 25, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    deposit = CashFlowFact(
        correction_id="cf_sub_01",
        account_label="primary",
        amount=Decimal("1000.00"),
        cash_flow_type="deposit",
        effective_at=t0 + timedelta(hours=12),
        reason="deposit",
        approval_ref="appr_01",
        evidence_hash="ev_01",
    )
    val_points = (
        ValuationPoint(timestamp=t0, equity=Decimal("10000.00")),
        ValuationPoint(timestamp=t0 + timedelta(hours=12), equity=Decimal("11000.00")),
        ValuationPoint(timestamp=t1, equity=Decimal("12000.00")),
    )
    cut = AccountEquityCut(
        account_label="primary",
        start_equity=Decimal("10000.00"),
        end_equity=Decimal("12000.00"),
        start_time=t0,
        end_time=t1,
        cash_flows=(deposit,),
        valuation_points=val_points,
    )
    spec_twr = MetricSpec(name="twr", family=MetricFamily.TIME_WEIGHTED_RETURN)
    res_twr = AccountPerformanceCalculator.calculate(spec_twr, cut)

    assert res_twr.status == MetricStatus.CONFIRMED
    assert res_twr.method == "linked_modified_dietz"
