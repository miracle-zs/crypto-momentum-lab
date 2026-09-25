"""Regression tests covering all issues identified in the audit report
(local_optimization_review_20260924.md).

Covered issues:
1. MTM verification strictly respects research study window [start_time, end_time].
   Trades exiting after window_end do not leak realized profit into window evaluation.
2. Scheduled risk window configuration and window bounds are preserved during
   single-process and selector execution.
3. Scheduled flatten correctly utilizes market price series rather than
   defaulting to entry price.
4. Candidates are immutable across six scenarios: subsequent evaluations do not
   mutate prior scenario results or shared candidate objects, ensuring scenario
   order independence.
5. Opportunities in SimulationLedger are strictly sorted chronologically (even
   if middle elements are out of order) with stable tie-break.
6. verify_contenders_batch memo cache decouples raw MTM metrics from max_mdd
   acceptance thresholds.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pytest

import local_optimization.generate_six_scenarios_dashboard as gsd
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    _ensure_worker_initialized,
    _init_verification_worker,
    _verify_single_contender,
    compute_daily_compounding_scales,
    select_balanced,
    select_compounding,
    select_pnl_max,
    verify_contenders_batch,
)
from local_optimization.mtm_engine import TradeRecord
from local_optimization.opportunity import RawOpportunity
from local_optimization.simulation_ledger import SimulationLedger


def _make_opp(
    opportunity_id: str,
    entry_eligible_at: datetime,
    exit_time: datetime,
    entry_price: float = 100.0,
    exit_price: float = 110.0,
    symbol: str = "BTCUSDT",
    detected_at: datetime | None = None,
) -> RawOpportunity:
    det_at = detected_at or entry_eligible_at
    return RawOpportunity(
        opportunity_id=opportunity_id,
        symbol=symbol,
        direction="LONG",
        detected_at=det_at,
        detected_epoch=det_at.timestamp(),
        entry_eligible_at=entry_eligible_at,
        entry_reference_price=entry_price,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=5.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.3,
        notional_intensity=2.0,
        volume_ratio=1.0,
        exit_time=exit_time,
        exit_price=exit_price,
    )


# -----------------------------------------------------------------------------
# 1. P1: MTM verification strictly respects research study window
# -----------------------------------------------------------------------------
def test_mtm_verification_respects_study_window() -> None:
    """Counterexample from report:

    Window 00:00-00:05.
    Trade enters at 00:01 at price 100.
    At 00:05 price is 100.
    At 00:10 trade exits at price 110 (outside window).
    Notional = 100U.

    If window is respected, terminal net PnL at 00:05 is ~ -0.07U (entry fee + slip).
    If window is not respected and runs until 00:10, net PnL is +9.85U.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 0, 5, 0, tzinfo=UTC)

    # 15s price grid from 00:00 to 00:15
    epochs: list[float] = []
    prices: list[float] = []
    curr = w_start
    t_end = w_start + timedelta(minutes=15)
    while curr <= t_end:
        ep = curr.timestamp()
        epochs.append(ep)
        if curr <= w_end:
            prices.append(100.0)
        elif curr < w_start + timedelta(minutes=10):
            prices.append(105.0)
        else:
            prices.append(110.0)
        curr += timedelta(seconds=15)

    price_series = {"BTCUSDT": (epochs, prices)}

    opp = _make_opp(
        opportunity_id="opp_win_01",
        entry_eligible_at=datetime(2026, 9, 1, 0, 1, 0, tzinfo=UTC),
        exit_time=datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_price=110.0,
    )

    opps_by_wc = {(2, 1): [opp]}
    _init_verification_worker(
        prices=price_series,
        opps_by_wc=opps_by_wc,
        w_start=w_start,
        w_end=w_end,
    )

    cand_params = {
        "max_open_positions": 1,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    item = (0, cand_params, None, False, None)
    idx, res = _verify_single_contender(item)
    assert res is not None

    # Crucial assertion: the net PnL evaluated by the worker MUST NOT include
    # the 00:10 exit!
    # At 00:05 (window_end), price is 100.0, so net PnL is negative
    # (entry fee + slip ~ -0.07U).
    assert res["net_pnl"] < 0.0, f"Expected negative PnL at 00:05, got {res['net_pnl']}"
    assert res["net_pnl"] > -1.0


def test_compounding_scales_respect_study_window() -> None:
    """Verify that compute_daily_compounding_scales bounds evaluation dates

    and does not realize post-window exits.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 2, 0, 0, 0, tzinfo=UTC)

    # Trade entered on 2026-09-01, exits on 2026-09-05 (outside 1-day window)
    tr = TradeRecord(
        trade_id="T_comp_01",
        symbol="BTCUSDT",
        entry_time=datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_time=datetime(2026, 9, 5, 12, 0, 0, tzinfo=UTC),
        exit_price=200.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
    )

    epochs = [w_start.timestamp(), w_end.timestamp()]
    prices = [100.0, 100.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    # Without window bounds, it would iterate to 2026-09-05 and realize 200.0
    day_scales, score, mdd, ui, term_eq, peak_m = compute_daily_compounding_scales(
        admitted_events=[tr],
        initial_equity=1000.0,
        prices_by_symbol=prices_by_symbol,
        w_start=w_start,
        w_end=w_end,
    )

    # Dates should only span 2026-09-01 to 2026-09-02
    assert "2026-09-05" not in day_scales
    assert len(day_scales) <= 2
    # At 2026-09-02 price was 100.0, so terminal equity should be around
    # initial equity (~1000.0), NOT 1100.0
    assert term_eq < 1005.0


# -----------------------------------------------------------------------------
# 2. P1: Scheduled Risk Price Propagation and Preservation
# -----------------------------------------------------------------------------
def test_scheduled_risk_price_propagation_to_flatten() -> None:
    """Verify that when a scheduled risk window triggers a flatten,

    simulate_window receives price_series and sets effective_exit_price to
    the actual market price at flatten time rather than entry_reference_price.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 2, 0, 0, tzinfo=UTC)

    # Flatten time at 00:30 UTC
    risk_cfg = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(0, 30),
        flatten_start_at=time(0, 30),
        flatten_deadline_at=time(0, 35),
        verify_at=time(0, 40),
        reopen_at=time(1, 0),
    )

    # Price at entry (00:10) is 100. Price at flatten (00:30) is 105.
    # Natural exit (01:30) is 110.
    epochs = [
        datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 0, 30, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 1, 30, 0, tzinfo=UTC).timestamp(),
    ]
    prices = [100.0, 100.0, 105.0, 110.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    opp = _make_opp(
        opportunity_id="opp_risk_01",
        entry_eligible_at=datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC),
        exit_time=datetime(2026, 9, 1, 1, 30, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_price=110.0,
    )

    opps_by_wc = {(2, 1): [opp]}
    _init_verification_worker(
        prices=prices_by_symbol,
        opps_by_wc=opps_by_wc,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=risk_cfg,
    )

    cand_params = {
        "max_open_positions": 1,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    item = (0, cand_params, None, False, None)
    idx, res = _verify_single_contender(item)
    assert res is not None

    # If flatten took market price 105.0: gross return = 100 * (105-100)/100 = +5.0U.
    # Minus fees (~0.14U) -> net PnL is ~ +4.85U.
    # If it fell back to entry price 100.0, net PnL would be negative (~ -0.14U).
    assert res["net_pnl"] > 4.0, (
        f"Expected net PnL ~ +4.85U from market price 105.0, got {res['net_pnl']}"
    )


def test_ensure_worker_initialized_preserves_scheduled_risk_and_window() -> None:
    """Verify that calling _ensure_worker_initialized from selectors without

    explicit risk config or window does NOT wipe existing worker config.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 0, 5, 0, tzinfo=UTC)
    risk_cfg = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(0, 0),
        flatten_start_at=time(0, 0),
        flatten_deadline_at=time(0, 5),
        verify_at=time(0, 10),
        reopen_at=time(0, 30),
    )

    prices: dict[str, Any] = {"BTCUSDT": ([0.0, 100.0], [100.0, 100.0])}
    opps: dict[tuple[int, int], list[Any]] = {(2, 1): []}

    _init_verification_worker(
        prices=prices,
        opps_by_wc=opps,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=risk_cfg,
    )

    assert gsd._worker_scheduled_risk_window is risk_cfg
    assert gsd._worker_w_end == w_end

    # Call without w_start, w_end, or scheduled_risk_window
    _ensure_worker_initialized(
        prices_by_symbol=prices,
        events=[],
        opps_by_wc=opps,
        w_start=None,
        w_end=None,
        scheduled_risk_window=None,
    )

    # Worker state must be preserved
    assert gsd._worker_scheduled_risk_window is risk_cfg
    assert gsd._worker_w_end == w_end


# -----------------------------------------------------------------------------
# 3. P1: Candidate Immutability & Scenario Order Independence
# -----------------------------------------------------------------------------
def test_candidate_immutability_and_scenario_independence() -> None:
    """Verify that Candidate8D instances are not mutated in-place by selectors,

    and that running scenarios in different order yields independent objects.
    """
    base_cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
        net_pnl=10.0,
        mdd=5.0,
        calmar=2.0,
        compounding_score=0.1,
        compounding_mdd=0.05,
        compounding_ui=0.01,
        terminal_compounded_equity=1100.0,
        n_trades=150,
        peak_margin=100.0,
        compounding_peak_margin=120.0,
        stability=0.8,
    )

    cand_list = [base_cand]

    t0 = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=15)
    opp = _make_opp("opp1", t0, t1, 100.0, 105.0)
    prices = {"BTCUSDT": ([t0.timestamp(), t1.timestamp()], [100.0, 105.0])}
    opps_by_wc = {(2, 1): [opp]}

    # With prices and events, selectors return verified candidate copies
    res_pnl = select_pnl_max(
        cand_list,
        margin_cap=280.0,
        prices_by_symbol=prices,
        events=[opp],
        opps_by_wc=opps_by_wc,
        w_start=t0,
        w_end=t1,
    )
    res_comp = select_compounding(
        cand_list,
        max_mdd=0.15,
        margin_cap=280.0,
        prices_by_symbol=prices,
        events=[opp],
        opps_by_wc=opps_by_wc,
        w_start=t0,
        w_end=t1,
    )

    assert res_pnl is not None
    assert res_comp is not None

    # Crucial assertion: returned candidates must NOT be the exact same object in memory
    assert res_pnl is not base_cand
    assert res_comp is not base_cand
    assert res_pnl is not res_comp

    # The original base_cand in cand_list must remain untouched
    assert base_cand.net_pnl == 10.0
    assert base_cand.terminal_compounded_equity == 1100.0


# -----------------------------------------------------------------------------
# 4. P2: SimulationLedger Chronological Opportunity Ordering
# -----------------------------------------------------------------------------
def test_simulation_ledger_strict_chronological_ordering() -> None:
    """Counterexample from report:

    Events with minute offsets [0, 2, 1, 3] on same symbol with 2 concurrency slots.
    Previous flawed logic: if typed_opps[0] > typed_opps[-1] (0 > 3 is False),
    it skipped sorting and processed [0, 2, 1, 3], admitting 0 and 2.
    Correct logic sorts to [0, 1, 2, 3] and admits 0 and 1.
    """
    ledger = SimulationLedger(
        initial_cash=1000.0,
        notional_usdt=100.0,
        leverage=5.0,
    )

    t0 = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    t_exit = datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC)

    # Construct opportunities at minutes 0, 2, 1, 3
    minute_offsets = [0, 2, 1, 3]
    opps: list[RawOpportunity] = []
    for m in minute_offsets:
        opps.append(
            _make_opp(
                opportunity_id=f"opp_min_{m}",
                entry_eligible_at=t0 + timedelta(minutes=m),
                exit_time=t_exit,
                entry_price=100.0,
                exit_price=105.0,
            )
        )

    res, _ = ledger.simulate_window(
        opportunities=opps,
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=t0,
        window_end=t0 + timedelta(hours=1),
        max_concurrency=2,
        fast_eval=True,
    )

    admitted_ids = [t.trade_id for t in res.admitted_trades]
    # Correct order: minute 0 and minute 1 are admitted.
    # Minute 2 and 3 are blocked by slots=2.
    assert admitted_ids == ["opp_min_0", "opp_min_1"], (
        f"Expected ['opp_min_0', 'opp_min_1'] but got {admitted_ids}"
    )


# -----------------------------------------------------------------------------
# 5. verify_contenders_batch Memo Cache Decouples max_mdd
# -----------------------------------------------------------------------------
def test_verify_contenders_batch_memo_cache_decouples_max_mdd() -> None:
    """Verify that a candidate evaluated under a strict max_mdd is cached with its

    actual metrics, and a subsequent call with looser max_mdd accepts it from cache.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 1, 0, 0, tzinfo=UTC)

    # 15s price grid with a 12% drawdown during the trade
    epochs = [
        datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 0, 20, 0, tzinfo=UTC).timestamp(),
        datetime(2026, 9, 1, 0, 40, 0, tzinfo=UTC).timestamp(),
    ]
    # Price 100 -> entry at 100 -> drops to 88 (12% DD) -> exits at 105 (+5% PnL)
    prices = [100.0, 100.0, 88.0, 105.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    opp = _make_opp(
        opportunity_id="opp_mdd_01",
        entry_eligible_at=datetime(2026, 9, 1, 0, 10, 0, tzinfo=UTC),
        exit_time=datetime(2026, 9, 1, 0, 40, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_price=105.0,
    )

    opps_by_wc = {(2, 1): [opp]}
    _init_verification_worker(
        prices=prices_by_symbol,
        opps_by_wc=opps_by_wc,
        w_start=w_start,
        w_end=w_end,
    )

    cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
            "max_open_positions": 1,
        },
        net_pnl=5.0,
        mdd=12.0,
        calmar=0.4,
        compounding_score=0.01,
        compounding_mdd=0.12,
        compounding_ui=0.02,
        terminal_compounded_equity=1050.0,
        n_trades=1,
        peak_margin=20.0,
    )

    memo_cache: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}

    # Call 1: strict max_mdd = 0.005 (0.5%). The candidate has ~1.2% DD,
    # so it must NOT be admitted.
    res_strict = verify_contenders_batch(
        [cand],
        margin_cap=None,
        compounding_scale=True,
        max_mdd=0.005,
        pool=None,
        memo_cache=memo_cache,
    )
    assert len(res_strict) == 0

    # Call 2: looser max_mdd = 0.05 (5%) using the same memo_cache.
    # It must reuse cache and admit the candidate!
    res_loose = verify_contenders_batch(
        [cand],
        margin_cap=None,
        compounding_scale=True,
        max_mdd=0.05,
        pool=None,
        memo_cache=memo_cache,
    )
    assert len(res_loose) == 1
    assert res_loose[0][0] is cand


# -----------------------------------------------------------------------------
# 6. Worker Reinitialization on Differing Opportunities/Events
# -----------------------------------------------------------------------------
def test_worker_reinitializes_when_opps_or_events_differ() -> None:
    """Verify that when the same price series is reused with a different opportunity

    pool or event list, _ensure_worker_initialized does NOT silently keep the
    stale pool.
    """
    prices: dict[str, Any] = {"BTCUSDT": ([0.0, 100.0], [100.0, 100.0])}
    opp_a = _make_opp(
        "opp_a",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 5, tzinfo=UTC),
    )
    opp_b = _make_opp(
        "opp_b",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 5, tzinfo=UTC),
    )

    opps_pool_1 = {(2, 1): [opp_a]}
    opps_pool_2 = {(2, 1): [opp_b]}

    # Init with pool 1
    _init_verification_worker(
        prices=prices,
        opps_by_wc=opps_pool_1,
        w_start=datetime(2026, 9, 1, tzinfo=UTC),
        w_end=datetime(2026, 9, 2, tzinfo=UTC),
    )
    assert gsd._worker_opps_by_wc is opps_pool_1

    # Call ensure with the exact same price object, but new opportunity pool
    _ensure_worker_initialized(
        prices_by_symbol=prices,
        events=None,
        opps_by_wc=opps_pool_2,
    )
    # Must NOT keep opps_pool_1!
    assert gsd._worker_opps_by_wc is opps_pool_2

    # Now call ensure with new events
    events_c = [opp_a, opp_b]
    _ensure_worker_initialized(
        prices_by_symbol=prices,
        events=events_c,
        opps_by_wc=None,
    )
    assert gsd._worker_events is events_c
    assert len(gsd._worker_opps_by_wc[(2, 1)]) == 2


# -----------------------------------------------------------------------------
# 7. Exact 1-Day Window Compounding Day Count
# -----------------------------------------------------------------------------
def test_exact_one_day_window_compounding_not_halved() -> None:
    """Verify that an exact 1-day window [2026-09-01 00:00, 2026-09-02 00:00)

    is evaluated as exactly 1 day (not 2 days), and growth rate is not halved.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 2, 0, 0, 0, tzinfo=UTC)

    # 1 trade entirely within 2026-09-01 with +100U profit
    tr = TradeRecord(
        trade_id="T_day_1",
        symbol="BTCUSDT",
        entry_time=datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_time=datetime(2026, 9, 1, 16, 0, 0, tzinfo=UTC),
        exit_price=200.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
    )

    epochs = [w_start.timestamp(), w_end.timestamp()]
    prices = [100.0, 100.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    day_scales, score, mdd, ui, term_eq, peak_m = compute_daily_compounding_scales(
        admitted_events=[tr],
        initial_equity=1000.0,
        prices_by_symbol=prices_by_symbol,
        w_start=w_start,
        w_end=w_end,
    )

    # Must only contain 1 date ("2026-09-01"), NOT 2 dates
    assert list(day_scales.keys()) == ["2026-09-01"], (
        f"Expected ['2026-09-01'], got {list(day_scales.keys())}"
    )

    # Theoretical 1-day growth rate: ln(term_eq / 1000) / 1.0
    import math

    expected_log_growth = math.log(term_eq / 1000.0) / 1.0
    # score = log_growth - 2 * ui
    assert score == pytest.approx(expected_log_growth - 2.0 * ui, rel=1e-5)
    # If n_days had been 2, score would be ~ half of expected_log_growth
    assert score > (expected_log_growth * 0.8)


# -----------------------------------------------------------------------------
# Round 2 Regressions (2026-09-24 Round 2 Review)
# -----------------------------------------------------------------------------
def test_r2_01_worker_rejects_insolvent_contender_with_negative_equity() -> None:
    """R2-01: Verify that a contender whose MTM equity drops <= 0 during the window

    is marked infeasible (metrics.is_feasible=False), rejected by the worker,
    and not selected by select_pnl_max even if terminal PnL is positive.
    """
    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(minutes=15)
    opps = [
        _make_opp(
            opportunity_id=f"b{i}",
            entry_eligible_at=start + timedelta(minutes=1),
            exit_time=start + timedelta(minutes=10),
            entry_price=100.0,
            exit_price=110.0,
            symbol=f"S{i}",
        )
        for i in range(11)
    ]
    # Price crashes to 1 at minute 2, producing negative equity (-89.77U),
    # then recovers to 110 at minute 10
    stress_prices = {
        item.symbol: (
            [
                start.timestamp(),
                (start + timedelta(minutes=2)).timestamp(),
                (start + timedelta(minutes=10)).timestamp(),
            ],
            [100.0, 1.0, 110.0],
        )
        for item in opps
    }
    pool = {(2, 1): opps}
    _init_verification_worker(stress_prices, pool, start, end)

    idx, verified = _verify_single_contender((0, {}, 280, False, None))
    assert verified is None, (
        "Insolvent contender with negative equity must be rejected by worker"
    )

    cand = Candidate8D(
        params={},
        net_pnl=100.0,
        mdd=1.0,
        calmar=100.0,
        compounding_score=0.0,
        compounding_mdd=0.0,
        compounding_ui=0.0,
        terminal_compounded_equity=1100.0,
        n_trades=11,
        peak_margin=220.0,
    )
    selected = select_pnl_max(
        [cand],
        margin_cap=280,
        prices_by_symbol=stress_prices,
        events=opps,
        opps_by_wc=pool,
        w_start=start,
        w_end=end,
    )
    assert selected is None, (
        "Insolvent candidate must not be selected by select_pnl_max"
    )


def test_r2_02_dashboard_view_respects_manifest_window_boundary() -> None:
    """R2-02: Verify that compute_six_scenarios_view respects manifest.watermark_end

    and does not expand curve or leak post-manifest exit PnL into dashboard metrics.
    """
    from types import SimpleNamespace

    import pandas as pd

    from local_optimization.generate_six_scenarios_dashboard import (
        compute_six_scenarios_view,
    )

    start = datetime(2026, 9, 1, tzinfo=UTC)
    cutoff = start + timedelta(minutes=5)

    first = _make_opp(
        "first",
        start + timedelta(minutes=1),
        start + timedelta(minutes=3),
        entry_price=100.0,
        exit_price=110.0,
    )
    second = _make_opp(
        "second",
        start + timedelta(minutes=4),
        start + timedelta(minutes=10),
        entry_price=100.0,
        exit_price=110.0,
    )
    view_prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                cutoff.timestamp(),
                (start + timedelta(minutes=10)).timestamp(),
            ],
            [100.0, 100.0, 110.0],
        )
    }
    params = dict(
        impulse_window_buckets=2,
        confirmation_buckets=1,
        min_return_pct=0.5,
        min_imbalance=0.3,
        min_intensity=1.5,
        min_volume_ratio=0.0,
        cooldown_buckets=0,
    )
    manifest = SimpleNamespace(watermark_start=start, watermark_end=cutoff)
    result = compute_six_scenarios_view(
        [first, second],
        pd.DataFrame([params]),
        view_prices,
        manifest,
        max_workers=1,
    )

    selected_cand = result[0]["m280_pnl_max"]
    assert selected_cand is not None
    displayed_metrics = result[2]["s_m280_pnl_max"]
    curve_points = result[4]["s_m280_pnl_max"]

    # Both selection and dashboard display must agree on the verified PnL
    # bounded by cutoff
    assert displayed_metrics["net_pnl"] == pytest.approx(
        selected_cand.net_pnl, rel=1e-4
    )
    assert (
        displayed_metrics["net_pnl"] < 15.0
    )  # Must NOT include the second trade's +10U profit at min 10
    assert curve_points[-1].timestamp == cutoff


def test_r2_03_dashboard_scheduled_flatten_uses_market_price() -> None:
    """R2-03: Verify that get_scenario_events passes price series to
    SimulationLedger so scheduled flatten uses market price (105) rather
    than falling back to entry price (100).
    """
    from local_optimization.generate_six_scenarios_dashboard import get_scenario_events

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(minutes=15)
    opp = _make_opp(
        "trade_risk",
        start + timedelta(minutes=1),
        start + timedelta(minutes=10),
        entry_price=100.0,
        exit_price=110.0,
    )
    prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=5)).timestamp(),
                end.timestamp(),
            ],
            [100.0, 105.0, 110.0],
        )
    }
    risk = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(0, 5),
        flatten_start_at=time(0, 5),
        flatten_deadline_at=time(0, 5, 15),
        verify_at=time(0, 5, 30),
        reopen_at=time(0, 6),
    )

    _init_verification_worker(prices, {(2, 1): [opp]}, start, end, risk)
    displayed = get_scenario_events(
        "s_m280_pnl_max",
        {},
        None,
        None,
        [opp],
        scheduled_risk_window=risk,
    )
    assert len(displayed) == 1
    assert displayed[0].exit_price == pytest.approx(105.0)
    assert (
        displayed[0].calculated_net_pnl > 4.0
    )  # Positive PnL (~4.86U), not entry price loss (-0.14U)


def test_r2_04_ledger_drawdown_uses_state_in_initial_equity() -> None:
    """R2-04: Verify that SimulationLedger.simulate_window passes
    state_in.total_equity_mtm to evaluate_equity_curve rather than
    defaulting to 1000U, avoiding phantom drawdown.
    """
    from local_optimization.equity import evaluate_equity_curve
    from local_optimization.simulation_ledger import PortfolioState, SimulationLedger

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(minutes=15)
    flat_opp = _make_opp(
        "flat_trade",
        start + timedelta(minutes=1),
        start + timedelta(minutes=5),
        entry_price=100.0,
        exit_price=100.0,
    )
    state = PortfolioState.create(start, 900.0, 900.0)
    prices = {"BTCUSDT": ([start.timestamp(), end.timestamp()], [100.0, 100.0])}

    result, _ = SimulationLedger().simulate_window(
        [flat_opp],
        {},
        start,
        end,
        state_in=state,
        price_series=prices,
    )
    correct = evaluate_equity_curve(result.equity_points, initial_equity=900.0)
    assert result.mdd_usdt == pytest.approx(correct.max_drawdown_usdt, abs=1e-3)
    assert (
        result.mdd_usdt < 1.0
    )  # Fee/slippage only (~0.14U), NOT 100.14U from 1000U baseline


def test_r2_05_dashboard_all_open_trades_does_not_crash() -> None:
    """R2-05: Verify that compute_six_scenarios_view does not raise ValueError

    when all trades in the window are open (exit_time is None).
    """
    from types import SimpleNamespace

    import pandas as pd

    from local_optimization.generate_six_scenarios_dashboard import (
        compute_six_scenarios_view,
    )

    start = datetime(2026, 9, 1, tzinfo=UTC)
    cutoff = start + timedelta(minutes=5)
    open_opp = _make_opp(
        "open_trade",
        start + timedelta(minutes=1),
        start + timedelta(minutes=10),
        entry_price=100.0,
        exit_price=110.0,
    )
    open_trade = replace(open_opp, exit_time=None, exit_price=None)
    view_prices = {
        "BTCUSDT": (
            [start.timestamp(), cutoff.timestamp()],
            [100.0, 100.0],
        )
    }
    params = dict(
        impulse_window_buckets=2,
        confirmation_buckets=1,
        min_return_pct=0.5,
        min_imbalance=0.3,
        min_intensity=1.5,
        min_volume_ratio=0.0,
        cooldown_buckets=0,
    )
    manifest = SimpleNamespace(watermark_start=start, watermark_end=cutoff)

    # Must complete successfully without raising
    # "ValueError: max() iterable argument is empty"
    result = compute_six_scenarios_view(
        [open_trade],
        pd.DataFrame([params]),
        view_prices,
        manifest,
        max_workers=1,
    )
    assert result is not None


# -----------------------------------------------------------------------------
# Follow-up Regressions (Non-midnight 24h Compounding, Infeasible View, etc.)
# -----------------------------------------------------------------------------
def test_compounding_non_midnight_24h_window_not_halved() -> None:
    """Verify that a 24-hour non-midnight window (e.g. 08:00 -> 08:00 next day)

    is evaluated as exactly 1.0 day of growth, and not halved due to spanning
    two calendar dates.
    """
    import math

    w_start = datetime(2026, 9, 1, 8, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 2, 8, 0, 0, tzinfo=UTC)

    tr = TradeRecord(
        trade_id="T_day_non_midnight",
        symbol="BTCUSDT",
        entry_time=datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC),
        entry_price=100.0,
        exit_time=datetime(2026, 9, 1, 16, 0, 0, tzinfo=UTC),
        exit_price=200.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
    )

    epochs = [w_start.timestamp(), w_end.timestamp()]
    prices = [100.0, 100.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    day_scales, score, mdd, ui, term_eq, peak_m = compute_daily_compounding_scales(
        admitted_events=[tr],
        initial_equity=1000.0,
        prices_by_symbol=prices_by_symbol,
        w_start=w_start,
        w_end=w_end,
    )

    # 24 hours elapsed -> n_days = 1.0
    expected_log_growth = math.log(term_eq / 1000.0) / 1.0
    assert score == pytest.approx(expected_log_growth - 2.0 * ui, rel=1e-5)
    # If n_days had been 2, score would be around half of expected_log_growth
    assert score > (expected_log_growth * 0.8)


def test_dashboard_infeasible_scenario_shows_zero_trades() -> None:
    """Verify that when a scenario has no feasible solution (cand is None),

    compute_six_scenarios_view does not simulate empty params and displays 0
    trades and 0.00 PnL.
    """
    from types import SimpleNamespace

    import pandas as pd

    from local_optimization.generate_six_scenarios_dashboard import (
        compute_six_scenarios_view,
    )

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(hours=1)

    # Event has impulse_return_pct = 0.1, so it fails the filter min_return_pct = 5.0
    opp = _make_opp(
        "failing_opp",
        start + timedelta(minutes=10),
        start + timedelta(minutes=20),
    )
    # Give candidate params that will reject all opportunities, producing no candidates
    grid_df = pd.DataFrame(
        [
            dict(
                impulse_window_buckets=2,
                confirmation_buckets=1,
                min_return_pct=99.0,  # Impossible threshold -> 0 trades admitted
                min_imbalance=0.9,
                min_intensity=10.0,
                min_volume_ratio=10.0,
                cooldown_buckets=0,
            )
        ]
    )
    manifest = SimpleNamespace(watermark_start=start, watermark_end=end)

    res = compute_six_scenarios_view(
        [opp],
        grid_df,
        prices_by_symbol={},
        manifest=manifest,
        max_workers=1,
    )
    scenarios, _, curves_meta, _, _, _ = res

    # m280_pnl_max should be None (infeasible)
    assert scenarios["m280_pnl_max"] is None

    # The dashboard meta for this scenario must show 0 trades and 0.0 PnL
    meta_s1 = curves_meta["s_m280_pnl_max"]
    assert meta_s1["is_feasible"] is False
    assert meta_s1["total_trades"] == 0
    assert meta_s1["net_pnl"] == 0.0
    assert meta_s1["param_str"] == "无可行解 (NO_FEASIBLE_SOLUTION)"


def test_dashboard_empty_events_does_not_raise_index_error() -> None:
    """Verify that compute_six_scenarios_view does not raise IndexError

    when events list is completely empty.
    """
    import pandas as pd

    from local_optimization.generate_six_scenarios_dashboard import (
        compute_six_scenarios_view,
    )

    res = compute_six_scenarios_view(
        events=[],
        full_grid_df=pd.DataFrame(),
        prices_by_symbol={},
        manifest=None,
        max_workers=1,
    )
    assert res is not None
    _, _, _, timeline_series, _, _ = res
    assert timeline_series == []


def test_fallback_selectors_reject_insolvent_negative_equity_candidates() -> None:
    """Verify that when price series is absent, the fallback selectors for
    pnl_max, balanced, and compounding reject candidates whose drawdown
    indicates insolvency (mdd >= INITIAL_EQUITY) or exceed margin_cap.
    """
    # Insolvent candidate: +100U terminal PnL, but MDD = 1200U (account went negative)
    insolvent_cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
        net_pnl=100.0,
        mdd=1200.0,  # >= 1000.0 (INITIAL_EQUITY) -> Insolvent!
        calmar=0.08,
        compounding_score=0.1,
        compounding_mdd=0.05,
        compounding_ui=0.01,
        terminal_compounded_equity=1100.0,
        n_trades=150,
        peak_margin=50.0,
        compounding_peak_margin=50.0,
        stability=0.8,
    )

    # Candidate exceeding margin_cap = 280U
    high_margin_cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
        net_pnl=100.0,
        mdd=50.0,
        calmar=2.0,
        compounding_score=0.1,
        compounding_mdd=0.05,
        compounding_ui=0.01,
        terminal_compounded_equity=1100.0,
        n_trades=150,
        peak_margin=350.0,  # > 280.0
        compounding_peak_margin=350.0,
        stability=0.8,
    )

    # select_pnl_max fallback
    assert (
        select_pnl_max([insolvent_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )
    assert (
        select_pnl_max([high_margin_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )

    # select_balanced fallback
    assert (
        select_balanced([insolvent_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )
    assert (
        select_balanced([high_margin_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )

    # select_compounding fallback
    assert (
        select_compounding([insolvent_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )
    assert (
        select_compounding([high_margin_cand], margin_cap=280.0, prices_by_symbol=None)
        is None
    )
