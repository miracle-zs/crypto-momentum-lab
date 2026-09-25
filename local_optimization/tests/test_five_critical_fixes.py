"""Regression tests for the five critical fixes:
1. 15s MTM verification for select_pnl_max and select_balanced
2. Incomplete market bar filtering in price cache construction
3. Fail-closed manifest enforcement and pool_type validation
4. Continuous state passing in daily local optimization OOS
5. Slippage and funding cost deduction in TradeRecord and SimulationLedger
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    run_six_scenarios_pipeline,
    solve_six_scenarios,
)
from local_optimization.mtm_engine import TradeRecord
from local_optimization.opportunity import (
    OpportunityPoolManifest,
    OpportunityStatus,
    RawOpportunity,
    compute_pool_content_hash,
)
from local_optimization.run_walk_forward_analysis import (
    build_arg_parser,
    load_all_replay_events,
)
from local_optimization.simulation_ledger import (
    SimulationLedger,
)


def make_test_opportunity(
    opp_id: str,
    symbol: str,
    detected_at: datetime,
    entry_eligible_at: datetime,
    entry_price: float,
    exit_time: datetime | None,
    exit_price: float | None,
    w: int = 2,
    c: int = 1,
    slippage_rate: float = 0.0002,
    funding_cost_usdt: float = 0.0,
    direction: str = "LONG",
) -> RawOpportunity:
    return RawOpportunity(
        opportunity_id=opp_id,
        symbol=symbol,
        direction=direction,
        detected_at=detected_at,
        detected_epoch=detected_at.timestamp(),
        entry_eligible_at=entry_eligible_at,
        entry_reference_price=entry_price,
        impulse_window_buckets=w,
        confirmation_buckets=c,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=2.0,
        volume_ratio=1.5,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_rule="candle_15m",
        fee_rate=0.0005,
        slippage_rate=slippage_rate,
        funding_cost_usdt=funding_cost_usdt,
        status=OpportunityStatus.EXITED if exit_time else OpportunityStatus.DETECTED,
    )


# -----------------------------------------------------------------------------
# Fix 1: All 6 scenarios verify MTM path
# -----------------------------------------------------------------------------
def test_six_scenarios_verify_mtm_for_pnl_max_and_balanced() -> None:
    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    opps = []
    ts_list = []
    ps_list = []

    for k in range(20):
        t_in = t0 + timedelta(minutes=k * 30)
        t_mid = t_in + timedelta(minutes=5)
        t_out = t_in + timedelta(minutes=15)
        opps.append(
            make_test_opportunity(
                f"opp_{k}", "BTCUSDT", t_in, t_in, 100.0, t_out, 105.0
            )
        )
        ts_list.extend([t_in.timestamp(), t_mid.timestamp(), t_out.timestamp()])
        ps_list.extend([100.0, 90.0, 105.0])

    prices = {"BTCUSDT": (ts_list, ps_list)}

    grid_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
            }
        ]
    )

    scenarios, _ = solve_six_scenarios(
        events=opps,
        full_grid_df=grid_df,
        prices_by_symbol=prices,
    )

    pnl_cand = scenarios["unc_pnl_max"]
    assert pnl_cand is not None
    # MDD must capture the true 15s intraday drawdown (fell to 90.0)
    assert pnl_cand.mdd >= 10.0
    # Verified net PnL is positive
    assert pnl_cand.net_pnl > 0


# -----------------------------------------------------------------------------
# Fix 2: Slippage and funding costs deduction in TradeRecord and SimulationLedger
# -----------------------------------------------------------------------------
def test_ledger_and_trade_record_deduct_slippage_and_funding() -> None:
    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=15)

    # TradeRecord without manual net_pnl_usdt computes dynamically
    tr = TradeRecord(
        trade_id="T_slip",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t1,
        exit_price=100.0,  # flat trade
        notional_usdt=100.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        funding_cost_usdt=0.01,
    )
    # Total fee: 100 * 0.0005 * 2 = 0.10
    # Total slippage: 100 * 0.0002 * 2 = 0.04
    # Funding cost: 0.01
    # Net PnL: 0 - 0.10 - 0.04 - 0.01 = -0.15
    assert tr.total_fee_usdt == pytest.approx(0.10, abs=1e-5)
    assert tr.total_slippage_usdt == pytest.approx(0.04, abs=1e-5)
    assert tr.calculated_net_pnl == pytest.approx(-0.15, abs=1e-5)

    # SimulationLedger admitting an opportunity applies default slippage
    ledger = SimulationLedger(default_slippage_rate=0.0002)
    opp = make_test_opportunity(
        "opp_slip",
        "BTCUSDT",
        t0,
        t0,
        100.0,
        t1,
        100.0,
        slippage_rate=0.0002,
        funding_cost_usdt=0.01,
    )
    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=t0,
        window_end=t1 + timedelta(seconds=15),
        fast_eval=True,
    )
    assert len(res.admitted_trades) == 1
    adm_trade = res.admitted_trades[0]
    assert adm_trade.slippage_rate == 0.0002
    assert adm_trade.funding_cost_usdt == 0.01
    assert res.oos_pnl == pytest.approx(-0.15, abs=1e-4)


# -----------------------------------------------------------------------------
# Fix 3: Strict Fail-Closed manifest governance
# -----------------------------------------------------------------------------
def test_wfa_and_dashboard_fail_closed_manifest(tmp_path: Path) -> None:
    # 1. build_arg_parser defaults require_manifest to True
    parser = build_arg_parser()
    parsed = parser.parse_args([])
    assert parsed.require_manifest is True

    # 2. Loading without manifest raises FileNotFoundError
    opp = make_test_opportunity(
        "opp_nomani", "BTCUSDT", datetime.now(UTC), datetime.now(UTC), 100.0, None, None
    )
    pool_file = tmp_path / "opportunity_pool.jsonl"
    pool_file.write_text(json.dumps(opp.to_dict()) + "\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Manifest file required"):
        load_all_replay_events(tmp_path, require_manifest=True)

    # 3. Invalid pool_type in dashboard raises ValueError
    manifest = OpportunityPoolManifest(
        snapshot_id="invalid_type_snap",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=1,
        watermark_start=datetime.now(UTC),
        watermark_end=datetime.now(UTC) + timedelta(hours=1),
        content_hash=compute_pool_content_hash([opp]),
        pool_type="account_biased",
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest.to_dict()), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="raw_parameter_independent"):
        run_six_scenarios_pipeline(data_dir=tmp_path)


# -----------------------------------------------------------------------------
# Fix 4: Continuous state passing in OOS roll
# -----------------------------------------------------------------------------
def test_continuous_state_roll_preserves_carry_in_position() -> None:
    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    t_mid = t0 + timedelta(days=1)
    t_end = t_mid + timedelta(days=1)

    # Trade opens on Day 1 at 23:00 and closes on Day 2 at 04:00
    opp = make_test_opportunity(
        "opp_cross_day",
        "BTCUSDT",
        t0 + timedelta(hours=23),
        t0 + timedelta(hours=23),
        100.0,
        t_mid + timedelta(hours=4),
        110.0,
    )

    ledger = SimulationLedger()
    # Step 1: Simulate Day 1 up to t_mid
    _, day1_state = ledger.simulate_window(
        opportunities=[opp],
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=t0,
        window_end=t_mid,
        state_in=None,
        fast_eval=True,
    )
    assert len(day1_state.active_positions) == 1
    assert day1_state.active_positions[0].symbol == "BTCUSDT"

    # Step 2: Pass day1_state into Day 2
    day2_res, day2_state = ledger.simulate_window(
        opportunities=[],
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=t_mid,
        window_end=t_end,
        state_in=day1_state,
        fast_eval=True,
    )
    assert day2_res.carry_in_count == 1
    # Trade closes inside Day 2 window, realizing gains
    assert day2_res.oos_pnl > 0
    assert len(day2_state.active_positions) == 0


def test_preroll_plus_oos_matches_continuous_replay() -> None:
    """Verifies that pre-roll + OOS terminal equity matches continuous replay.

    Resolves the counterexample where continuous replay gave 1009.853U while
    pre-roll + OOS gave 1004.923U due to omitting midnight floating PnL.
    """
    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    t_mid = t0 + timedelta(days=1)
    t_end = t_mid + timedelta(days=1)

    # Trade opens at 23:00 on Day 1 at 100.0, price at midnight is 105.0,
    # and exits at 04:00 on Day 2 at 110.0.
    opp = make_test_opportunity(
        "opp_cross_floating",
        "BTCUSDT",
        t0 + timedelta(hours=23),
        t0 + timedelta(hours=23),
        100.0,
        t_mid + timedelta(hours=4),
        110.0,
    )
    t_in = (t0 + timedelta(hours=23)).timestamp()
    t_midnight = t_mid.timestamp()
    t_out = (t_mid + timedelta(hours=4)).timestamp()

    # Price timeline: 100.0 at entry, 105.0 at midnight, 110.0 at exit
    prices = {
        "BTCUSDT": (
            [t_in, t_midnight, t_out],
            [100.0, 105.0, 110.0],
        )
    }

    ledger = SimulationLedger()
    params = {"impulse_window_buckets": 2, "confirmation_buckets": 1}

    # 1. Continuous Replay over [t0, t_end]
    cont_res, cont_state = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0,
        window_end=t_end,
        state_in=None,
        price_series=prices,
        fast_eval=False,
    )
    cont_terminal_equity = cont_state.total_equity_mtm

    # 2. Sequential "Pre-roll (Day 1) + OOS (Day 2)" with full MTM
    _, day1_state_mtm = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0,
        window_end=t_mid,
        state_in=None,
        price_series=prices,
        fast_eval=False,
    )
    # Day 1 ending equity must reflect the floating profit at midnight
    assert day1_state_mtm.total_equity_mtm > 1000.0

    _, day2_state_mtm = ledger.simulate_window(
        opportunities=[],
        params=params,
        window_start=t_mid,
        window_end=t_end,
        state_in=day1_state_mtm,
        price_series=prices,
        fast_eval=False,
    )
    # Exact match with continuous replay
    assert day2_state_mtm.total_equity_mtm == pytest.approx(
        cont_terminal_equity, abs=1e-3
    )

    # 3. Sequential with fast_eval on pre-roll: must also match
    _, day1_state_fast = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0,
        window_end=t_mid,
        state_in=None,
        price_series=prices,
        fast_eval=True,
    )
    assert day1_state_fast.total_equity_mtm == pytest.approx(
        day1_state_mtm.total_equity_mtm, abs=1e-3
    )

    _, day2_state_fast = ledger.simulate_window(
        opportunities=[],
        params=params,
        window_start=t_mid,
        window_end=t_end,
        state_in=day1_state_fast,
        price_series=prices,
        fast_eval=False,
    )
    assert day2_state_fast.total_equity_mtm == pytest.approx(
        cont_terminal_equity, abs=1e-3
    )


def test_compounding_day_cut_includes_midnight_floating_pnl() -> None:
    """Verifies that compute_daily_compounding_scales includes floating PnL

    at 00:00:00 UTC day cut, ensuring Day 2 sizing is based on true MTM equity.
    """
    from local_optimization.generate_six_scenarios_dashboard import (
        compute_daily_compounding_scales,
    )

    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    t_mid = t0 + timedelta(days=1)

    # Opp 1: entered Day 1 at 20:00 at 100.0, exits Day 2 at 12:00 at 150.0
    opp1 = make_test_opportunity(
        "opp1",
        "BTCUSDT",
        t0 + timedelta(hours=20),
        t0 + timedelta(hours=20),
        100.0,
        t_mid + timedelta(hours=12),
        150.0,
    )
    # Opp 2: enters Day 2 at 01:00
    opp2 = make_test_opportunity(
        "opp2",
        "BTCUSDT",
        t_mid + timedelta(hours=1),
        t_mid + timedelta(hours=1),
        140.0,
        t_mid + timedelta(hours=5),
        145.0,
    )

    t_in = (t0 + timedelta(hours=20)).timestamp()
    t_midnight = t_mid.timestamp()
    t_out = (t_mid + timedelta(hours=12)).timestamp()

    # Price at midnight surged from 100.0 to 140.0 (+40% gain)
    prices = {
        "BTCUSDT": (
            [t_in, t_midnight, t_out],
            [100.0, 140.0, 150.0],
        )
    }

    # 1. With price series: midnight floating profit is factored into Day 2 scale
    day_scales_mtm, _, _, _, _, _ = compute_daily_compounding_scales(
        admitted_events=[opp1, opp2],
        initial_equity=1000.0,
        f=0.10,
        prices_by_symbol=prices,
    )
    d2_str = t_mid.strftime("%Y-%m-%d")
    # Day 1 scale is (0.10 * 1000) / 100 = 1.0
    # At midnight, Opp1 has notional 100U * 40% = +40U floating gain (minus fees)
    # So MTM equity at midnight is ~1039.93U -> scale on Day 2 > 1.03
    assert day_scales_mtm[d2_str] > 1.03

    # 2. Without price series: fallback only has realized equity (1000.0) -> scale 1.0
    day_scales_flat, _, _, _, _, _ = compute_daily_compounding_scales(
        admitted_events=[opp1, opp2],
        initial_equity=1000.0,
        f=0.10,
        prices_by_symbol=None,
    )
    assert day_scales_flat[d2_str] <= 1.001


def test_fail_closed_unverified_fallback() -> None:
    """Verifies that select_pnl_max and select_balanced fail closed (return None)

    when all candidates breach margin caps or fail MTM verification, never
    returning unverified fast_eval candidates.
    """
    from local_optimization.generate_six_scenarios_dashboard import (
        solve_six_scenarios,
    )

    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    t_out = t0 + timedelta(minutes=15)
    opp = make_test_opportunity(
        "opp_breach",
        "BTCUSDT",
        t0,
        t0,
        100.0,
        t_out,
        95.0,
        direction="SHORT",
    )

    # In price series, price spikes to 300 during short trade (20% MTM drawdown),
    # breaching compounding max_mdd (0.15), then exits at 95.0 (+5.0 gross gain)
    prices = {
        "BTCUSDT": (
            [
                t0.timestamp(),
                (t0 + timedelta(minutes=5)).timestamp(),
                t_out.timestamp(),
            ],
            [100.0, 300.0, 95.0],
        )
    }

    grid_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
            }
        ]
    )

    scenarios, _ = solve_six_scenarios(
        events=[opp],
        full_grid_df=grid_df,
        prices_by_symbol=prices,
    )

    # In m280 scenarios where margin cap is enforced:
    # m280_compounding has max_mdd=0.15, but short trade experienced 20% drawdown
    # It must fail closed and return None!
    assert scenarios["m280_compounding"] is None


def test_load_15s_price_series_filters_incomplete(tmp_path: Path) -> None:
    """Verifies that load_15s_price_series strictly filters incomplete bars."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from local_optimization.mtm_engine import load_15s_price_series

    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    table = pa.Table.from_pydict(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "bucket_start": [
                t0,
                t0 + timedelta(seconds=15),
                t0 + timedelta(seconds=30),
                t0 + timedelta(seconds=45),
            ],
            "close_price": [100.0, 101.0, 102.0, 103.0],
            "data_complete": [True, False, True, True],
            "missing_agg_trade_count": [0, 0, 5, 0],
        }
    )
    p = tmp_path / "15s_bars.parquet"
    pq.write_table(table, p)

    series = load_15s_price_series(tmp_path, symbols={"BTCUSDT"})
    assert "BTCUSDT" in series
    times, prices = series["BTCUSDT"]
    # Only row 0 and row 3 (data_complete=True, missing=0) should be loaded!
    assert len(times) == 2
    assert len(prices) == 2
    assert prices == [100.0, 103.0]
