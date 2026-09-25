"""Pure domain service for authoritative account performance metrics evaluation (R5).

Obeys Astra Architecture Blueprint 2026-09-25:
- calculate(metric_spec, account_cut) -> MetricValue
- Strict mathematical distinctions: NET_EQUITY_DELTA, CASH_FLOW_ADJUSTED_PNL, TWR, MWR;
- Never aliases different return formulas;
- Transparent status reporting: CONFIRMED, UNVERIFIED_ESTIMATE,
  INSUFFICIENT_COVERAGE;
- Defends against zero/negative capital base, missing subintervals,
  or unknown cash flows.
"""

from __future__ import annotations

from decimal import Decimal

from crypto_momentum_lab.domain.performance.metric_models import (
    AccountEquityCut,
    MetricFamily,
    MetricSpec,
    MetricStatus,
    MetricValue,
)


class AccountPerformanceCalculator:
    """Pure domain calculator for account metrics without side effects."""

    @classmethod
    def calculate(
        cls,
        spec: MetricSpec,
        cut: AccountEquityCut,
    ) -> MetricValue:
        """Evaluates an authoritative MetricValue from an AccountEquityCut."""
        source_refs = (
            f"account:{cut.account_label}",
            f"cut_as_of:{cut.as_of.isoformat()}",
        )
        as_of = cut.as_of

        # 1. NET_EQUITY_DELTA
        if spec.family == MetricFamily.NET_EQUITY_DELTA:
            delta = cut.end_equity - cut.start_equity
            status = (
                MetricStatus.UNVERIFIED_ESTIMATE
                if cut.has_unknown_cash_flows
                else MetricStatus.CONFIRMED
            )
            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=delta,
                unit=spec.unit,
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=status,
                details={
                    "start_equity": str(cut.start_equity),
                    "end_equity": str(cut.end_equity),
                    "raw_delta": str(delta),
                },
            )

        # 2. CASH_FLOW_ADJUSTED_PNL
        if spec.family == MetricFamily.CASH_FLOW_ADJUSTED_PNL:
            if cut.has_unknown_cash_flows:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.INSUFFICIENT_COVERAGE,
                    details={"error": "unknown_cash_flows_present"},
                )

            total_cash_flow = sum(
                (cf.amount for cf in cut.cash_flows),
                start=Decimal("0.00"),
            )
            adjusted_pnl = (cut.end_equity - cut.start_equity) - total_cash_flow
            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=adjusted_pnl,
                unit=spec.unit,
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=MetricStatus.CONFIRMED,
                details={
                    "total_cash_flow": str(total_cash_flow),
                    "cash_flow_count": len(cut.cash_flows),
                    "raw_delta": str(cut.end_equity - cut.start_equity),
                    "coverage_status": "confirmed" if cut.cash_flows else "uncertified",
                    "is_certified": bool(cut.cash_flows),
                    "cash_flow_coverage_proof": (
                        f"audited_records_count_{len(cut.cash_flows)}"
                        if cut.cash_flows
                        else "uncertified_zero_cash_flow_facts"
                    ),
                },
            )

        # 3. TIME_WEIGHTED_RETURN (TWR)
        if spec.family == MetricFamily.TIME_WEIGHTED_RETURN:
            if cut.has_unknown_cash_flows:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.INSUFFICIENT_COVERAGE,
                    details={"error": "unknown_cash_flows_present"},
                )

            # If no cash flows, simple return equals TWR
            if not cut.cash_flows:
                if cut.start_equity <= Decimal("0"):
                    return MetricValue(
                        metric_name=spec.name,
                        family=spec.family,
                        metric_version=spec.version,
                        value=None,
                        unit=spec.unit,
                        interval_start=cut.start_time,
                        interval_end=cut.end_time,
                        as_of=as_of,
                        source_refs=source_refs,
                        status=MetricStatus.UNKNOWN,
                        details={"error": "zero_or_negative_starting_equity"},
                    )
                ret = (cut.end_equity - cut.start_equity) / cut.start_equity
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=ret.quantize(Decimal("0.000001")),
                    unit="ratio",
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.CONFIRMED,
                    details={
                        "subinterval_count": 1,
                        "coverage_status": "uncertified",
                        "is_certified": False,
                        "cash_flow_coverage_proof": "uncertified_zero_cash_flow_facts",
                    },
                )

            # Cash flows exist: requires subinterval valuation points
            if len(cut.valuation_points) < 2:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.INSUFFICIENT_COVERAGE,
                    details={"error": "valuation_subintervals_required_for_twr"},
                )

            # Compound subinterval returns
            compounded = Decimal("1.0")
            prev_eq = cut.valuation_points[0].equity
            for vp in cut.valuation_points[1:]:
                if prev_eq <= Decimal("0"):
                    return MetricValue(
                        metric_name=spec.name,
                        family=spec.family,
                        metric_version=spec.version,
                        value=None,
                        unit=spec.unit,
                        interval_start=cut.start_time,
                        interval_end=cut.end_time,
                        as_of=as_of,
                        source_refs=source_refs,
                        status=MetricStatus.UNKNOWN,
                        details={"error": "subinterval_equity_zero_or_negative"},
                    )
                sub_ret = (vp.equity - prev_eq) / prev_eq
                compounded *= Decimal("1.0") + sub_ret
                prev_eq = vp.equity

            twr_ret = compounded - Decimal("1.0")
            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=twr_ret.quantize(Decimal("0.000001")),
                unit="ratio",
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=MetricStatus.CONFIRMED,
                details={
                    "subinterval_count": len(cut.valuation_points),
                    "coverage_status": "confirmed",
                    "is_certified": True,
                    "cash_flow_coverage_proof": f"audited_records_count_{len(cut.cash_flows)}",
                },
            )

        # 4. MODIFIED_DIETZ
        if spec.family == MetricFamily.MODIFIED_DIETZ:
            if cut.has_unknown_cash_flows:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.INSUFFICIENT_COVERAGE,
                    details={"error": "unknown_cash_flows_present"},
                )

            total_duration = Decimal(
                str((cut.end_time - cut.start_time).total_seconds())
            )
            if total_duration <= Decimal("0"):
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.UNKNOWN,
                    details={"error": "zero_or_negative_duration"},
                )

            total_cash_flow = sum(
                (cf.amount for cf in cut.cash_flows),
                start=Decimal("0.00"),
            )
            gain = (cut.end_equity - cut.start_equity) - total_cash_flow

            # Compute time-weighted cash flow capital base
            weighted_cash_flows = Decimal("0.00")
            for cf in cut.cash_flows:
                elapsed = Decimal(
                    str((cf.effective_at - cut.start_time).total_seconds())
                )
                remaining = max(Decimal("0.00"), total_duration - elapsed)
                weight = remaining / total_duration
                weighted_cash_flows += cf.amount * weight

            average_capital = cut.start_equity + weighted_cash_flows
            if average_capital <= Decimal("0"):
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.UNKNOWN,
                    details={
                        "error": "average_capital_base_zero_or_negative",
                        "average_capital": str(average_capital),
                    },
                )

            dietz_ret = gain / average_capital
            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=dietz_ret.quantize(Decimal("0.000001")),
                unit="ratio",
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=MetricStatus.CONFIRMED,
                details={
                    "gain": str(gain),
                    "average_capital": str(average_capital),
                    "cash_flow_count": len(cut.cash_flows),
                    "coverage_status": "confirmed" if cut.cash_flows else "uncertified",
                    "is_certified": bool(cut.cash_flows),
                    "cash_flow_coverage_proof": (
                        f"audited_records_count_{len(cut.cash_flows)}"
                        if cut.cash_flows
                        else "uncertified_zero_cash_flow_facts"
                    ),
                },
            )

        # 5. MONEY_WEIGHTED_RETURN (True Internal Rate of Return - IRR)
        if spec.family == MetricFamily.MONEY_WEIGHTED_RETURN:
            if cut.has_unknown_cash_flows:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.INSUFFICIENT_COVERAGE,
                    details={"error": "unknown_cash_flows_present"},
                )

            if not cut.cash_flows:
                if cut.start_equity <= Decimal("0"):
                    return MetricValue(
                        metric_name=spec.name,
                        family=spec.family,
                        metric_version=spec.version,
                        value=None,
                        unit=spec.unit,
                        interval_start=cut.start_time,
                        interval_end=cut.end_time,
                        as_of=as_of,
                        source_refs=source_refs,
                        status=MetricStatus.UNKNOWN,
                        details={"error": "zero_or_negative_starting_equity"},
                    )
                ret = (cut.end_equity - cut.start_equity) / cut.start_equity
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=ret.quantize(Decimal("0.000001")),
                    unit="ratio",
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.CONFIRMED,
                    details={"method": "simple_return_no_cash_flows"},
                )

            total_seconds = Decimal(
                str((cut.end_time - cut.start_time).total_seconds())
            )
            if total_seconds <= Decimal("0") or cut.start_equity <= Decimal("0"):
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.UNKNOWN,
                    details={"error": "invalid_duration_or_starting_equity"},
                )

            # Solve IRR: -E0 - sum(CF_i / (1+r)^w_i) + ET / (1+r) = 0
            cf_weights = [
                (
                    float(cf.amount),
                    float(
                        Decimal(
                            str((cf.effective_at - cut.start_time).total_seconds())
                        )
                        / total_seconds
                    ),
                )
                for cf in cut.cash_flows
            ]
            e0 = float(cut.start_equity)
            et = float(cut.end_equity)

            def npv(r: float) -> float:
                if 1.0 + r <= 0.0:
                    return float("inf")
                val = -e0
                for amt, w in cf_weights:
                    val -= amt / ((1.0 + r) ** w)
                val += et / (1.0 + r)
                return val

            low, high = -0.999, 50.0
            f_low = npv(low)
            f_high = npv(high)
            irr_val = None
            if f_low * f_high < 0:
                for _ in range(60):
                    mid = (low + high) / 2.0
                    f_mid = npv(mid)
                    if abs(f_mid) < 1e-9:
                        irr_val = Decimal(str(mid)).quantize(Decimal("0.000001"))
                        break
                    if f_low * f_mid < 0:
                        high = mid
                        f_high = f_mid
                    else:
                        low = mid
                        f_low = f_mid
                if irr_val is None:
                    irr_val = Decimal(str((low + high) / 2.0)).quantize(
                        Decimal("0.000001")
                    )

            if irr_val is None:
                return MetricValue(
                    metric_name=spec.name,
                    family=spec.family,
                    metric_version=spec.version,
                    value=None,
                    unit=spec.unit,
                    interval_start=cut.start_time,
                    interval_end=cut.end_time,
                    as_of=as_of,
                    source_refs=source_refs,
                    status=MetricStatus.UNKNOWN,
                    details={"error": "irr_convergence_failed"},
                )

            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=irr_val,
                unit="ratio",
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=MetricStatus.CONFIRMED,
                details={
                    "method": "exact_irr_bisection",
                    "cash_flow_count": len(cut.cash_flows),
                },
            )

        # 5. MAX_DRAWDOWN
        if spec.family == MetricFamily.MAX_DRAWDOWN:
            points = [vp.equity for vp in cut.valuation_points]
            if not points:
                points = [cut.start_equity, cut.end_equity]

            peak = points[0]
            max_dd = Decimal("0.00")
            for eq in points:
                if eq > peak:
                    peak = eq
                elif peak > Decimal("0"):
                    dd = (peak - eq) / peak
                    if dd > max_dd:
                        max_dd = dd

            return MetricValue(
                metric_name=spec.name,
                family=spec.family,
                metric_version=spec.version,
                value=max_dd.quantize(Decimal("0.000001")),
                unit="ratio",
                interval_start=cut.start_time,
                interval_end=cut.end_time,
                as_of=as_of,
                source_refs=source_refs,
                status=MetricStatus.CONFIRMED,
                details={"peak_valuation": str(peak)},
            )

        return MetricValue(
            metric_name=spec.name,
            family=spec.family,
            metric_version=spec.version,
            value=None,
            unit=spec.unit,
            interval_start=cut.start_time,
            interval_end=cut.end_time,
            as_of=as_of,
            source_refs=source_refs,
            status=MetricStatus.UNKNOWN,
            details={"error": f"unsupported_metric_family_{spec.family}"},
        )
