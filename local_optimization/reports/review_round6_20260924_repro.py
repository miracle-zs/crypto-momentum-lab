"""Read-only synthetic review evidence; run with python -m ...review_round6_20260924_repro."""

import json
from datetime import UTC, datetime, time, timedelta

from crypto_momentum_lab.live_rollout.scheduled_risk_window import ScheduledRiskWindowConfig
from local_optimization.evaluation_context import (
    EvaluationContext, ScenarioSpec, evaluate_candidate, to_trade_records,
)
from local_optimization.mtm_engine import TradeRecord
from local_optimization.opportunity import RawOpportunity, OpportunityStatus
from local_optimization.simulation_ledger import PortfolioState


def opportunity(start, entry, exit_time, exit_price):
    return RawOpportunity(
        opportunity_id="new", symbol="BTCUSDT", direction="LONG",
        detected_at=start, detected_epoch=start.timestamp(),
        entry_eligible_at=entry, entry_reference_price=100,
        impulse_window_buckets=2, confirmation_buckets=1,
        impulse_return_pct=.8, aggressive_imbalance=.4,
        confirmation_min_imbalance=.4, notional_intensity=3.5, volume_ratio=1.5,
        exit_time=exit_time, exit_price=exit_price, status=OpportunityStatus.DETECTED,
    )


def collect_evidence():
    start = datetime(2026, 9, 1, 10, tzinfo=UTC)
    end = start + timedelta(hours=1)
    prices = {"BTCUSDT": (
        [start.timestamp(), (start + timedelta(minutes=30)).timestamp(), end.timestamp()],
        [100, 105, 120],
    )}
    evidence = {}

    short_end = start + timedelta(minutes=1)
    brief = opportunity(start, start + timedelta(seconds=1), start + timedelta(seconds=2), 110)
    short_prices = {"BTCUSDT": ([start.timestamp(), short_end.timestamp()], [100, 110])}
    evidence["sub_grid_margin"] = []
    for curve in (False, True):
        result = evaluate_candidate(
            EvaluationContext(start, short_end, short_prices, events=[brief]), {},
            ScenarioSpec(compounding=True, f=.5, margin_cap=30), include_curve=curve,
        )
        evidence["sub_grid_margin"].append({
            "include_curve": curve, "reported_peak_margin": result.peak_margin,
            "actual_trade_margin": result.admitted_trades[0].initial_margin_usdt,
            "cap": 30, "reported_feasible": result.is_feasible, "expected_feasible": False,
        })

    carry = TradeRecord(
        trade_id="carry", symbol="BTCUSDT", entry_time=start - timedelta(hours=1),
        entry_price=100, exit_time=end + timedelta(hours=1), exit_price=130,
        notional_usdt=100, leverage=5, fee_rate=.0005, slippage_rate=.0002,
    )
    state = PortfolioState.create(start, 1000, 999.93, (carry,))
    risk = ScheduledRiskWindowConfig(
        timezone="UTC", entry_stop_at=time(10, 30), flatten_start_at=time(10, 30),
        flatten_deadline_at=time(10, 30, 15), verify_at=time(10, 30, 30), reopen_at=time(10, 31),
    )
    evidence["scheduled_carry_exit"] = []
    for curve in (False, True):
        result = evaluate_candidate(
            EvaluationContext(start, end, prices, state_in=state, scheduled_risk_window=risk),
            {}, include_curve=curve,
        )
        evidence["scheduled_carry_exit"].append({
            "include_curve": curve, "reported_equity": result.terminal_equity,
            "ledger_equity": result.state_out.total_equity_mtm,
            "expected_equity": 1004.8565,
            "reported_exit": result.admitted_trades[0].exit_time.isoformat(),
            "expected_exit": (start + timedelta(minutes=30)).isoformat(),
            "state_open_positions": len(result.state_out.active_positions),
        })

    closed = dict(symbol="BTCUSDT", entry_at=start, entry_price=100,
                  exit_at=end, exit_price=110, net_pnl_usdt=9.86)
    converted = to_trade_records([closed])[0]
    missing_pnl = dict(closed, exit_at=end.isoformat())
    del missing_pnl["net_pnl_usdt"]
    evidence["dict_conversion"] = {
        "datetime_exit_actual": converted.exit_time, "datetime_exit_expected": end.isoformat(),
        "reported_open": converted.is_open, "expected_open": False,
        "missing_pnl_actual": to_trade_records([missing_pnl])[0].calculated_net_pnl,
        "missing_pnl_expected": TradeRecord(
            trade_id="expected", symbol="BTCUSDT", entry_time=start, entry_price=100,
            exit_time=end, exit_price=110, fee_rate=.0005, slippage_rate=.0002,
        ).calculated_net_pnl,
    }

    new = opportunity(start, start + timedelta(minutes=1), end + timedelta(hours=1), 130)
    result = evaluate_candidate(
        EvaluationContext(start, end, prices, events=[new]), {}, ScenarioSpec(compounding=True, f=.2),
    )
    evidence["compounding_state"] = {
        "reported_equity": result.terminal_equity,
        "state_equity": result.state_out.total_equity_mtm,
        "evaluated_notional": result.admitted_trades[0].notional_usdt,
        "state_notional": result.state_out.active_positions[0].notional_usdt,
        "expected": "state must retain the evaluated equity and position size",
    }
    return evidence


if __name__ == "__main__":
    print(json.dumps(collect_evidence(), ensure_ascii=False, indent=2, default=str))
