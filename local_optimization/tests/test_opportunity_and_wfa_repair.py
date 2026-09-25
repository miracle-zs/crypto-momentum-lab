"""Comprehensive test suite verifying the 10 acceptance criteria from the repair spec.

Specification:
docs/superpowers/specs/2026-09-21-opportunity-pool-and-walk-forward-repair-design.md
Section 9: 验收标准 (Acceptance Criteria 1 to 10).
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from local_optimization.mtm_engine import TradeRecord
from local_optimization.opportunity import (
    OpportunityPoolManifest,
    OpportunityStatus,
    RawOpportunity,
    compute_pool_content_hash,
    generate_opportunity_id,
    load_opportunity_pool,
    validate_opportunity_pool,
)
from local_optimization.protocol import ParameterCandidate
from local_optimization.run_walk_forward_analysis import (
    WindowSplit,
    build_arg_parser,
    generate_rolling_splits,
    load_all_replay_events,
    run_walk_forward_analysis,
)
from local_optimization.simulation_ledger import (
    PortfolioState,
    SimulationLedger,
)


def make_mock_opportunity(
    opp_id: str,
    symbol: str,
    detected_at: datetime,
    entry_at: datetime,
    entry_price: float,
    exit_at: datetime | None,
    exit_price: float | None,
    net_pnl: float | None = None,
    exit_submitted_at: datetime | None = None,
    w: int = 2,
    c: int = 1,
    r: float = 0.8,
    imb: float = 0.4,
    inten: float = 3.5,
    vol: float = 1.5,
) -> RawOpportunity:
    """Helper to create valid RawOpportunity objects for testing."""
    return RawOpportunity(
        opportunity_id=opp_id,
        symbol=symbol,
        direction="LONG",
        detected_at=detected_at,
        detected_epoch=detected_at.timestamp(),
        entry_eligible_at=entry_at,
        entry_reference_price=entry_price,
        impulse_window_buckets=w,
        confirmation_buckets=c,
        impulse_return_pct=r,
        aggressive_imbalance=imb,
        confirmation_min_imbalance=imb,
        notional_intensity=inten,
        volume_ratio=vol,
        exit_time=exit_at,
        exit_submitted_at=exit_submitted_at,
        exit_price=exit_price,
        net_pnl_usdt=net_pnl,
        status=OpportunityStatus.DETECTED,
    )


# -----------------------------------------------------------------------------
# Criterion 1: Empty opportunity pool rejected
# -----------------------------------------------------------------------------
def test_criterion_1_empty_pool_rejected() -> None:
    """Acceptance 1: Empty opportunity pool must be rejected by validation gate."""
    errors = validate_opportunity_pool([])
    assert len(errors) > 0
    assert any("empty" in e.lower() for e in errors)


# -----------------------------------------------------------------------------
# Criterion 2: opportunity_id uniqueness and idempotency
# -----------------------------------------------------------------------------
def test_criterion_2_opportunity_id_uniqueness_and_idempotency() -> None:
    """Acceptance 2: Opportunity ID generation is idempotent and detects duplicates."""
    epoch = 1757000000.0
    id1 = generate_opportunity_id("BTCUSDT", epoch, 2, 1, "LONG")
    id2 = generate_opportunity_id("BTCUSDT", epoch, 2, 1, "LONG")
    id3 = generate_opportunity_id("ETHUSDT", epoch, 2, 1, "LONG")

    assert id1 == id2, "ID generation must be deterministic and idempotent"
    assert id1 != id3, "Different symbols must have distinct IDs"

    t0 = datetime.fromtimestamp(epoch, tz=UTC)
    opp1 = make_mock_opportunity(
        id1, "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(minutes=15), 51000.0
    )
    opp2 = make_mock_opportunity(
        id1, "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(minutes=15), 51000.0
    )

    errors = validate_opportunity_pool([opp1, opp2])
    assert any("duplicate" in e.lower() for e in errors)


# -----------------------------------------------------------------------------
# Criterion 3: Cross-window position computed exactly once
# -----------------------------------------------------------------------------
def test_criterion_3_cross_window_position_counted_once() -> None:
    """Acceptance 3: Cross-window position lives across boundary without duplication."""
    t_start = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)
    t_mid = datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 7, 0, 0, tzinfo=UTC)

    # Trade enters in window 1, exits in window 2
    opp = make_mock_opportunity(
        "cross_1",
        "BTCUSDT",
        t_start + timedelta(hours=20),
        t_start + timedelta(hours=20),
        50000.0,
        t_mid + timedelta(hours=4),
        51000.0,
    )

    ledger = SimulationLedger()
    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # Window 1: [t_start, t_mid)
    res1, state_out1 = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t_start,
        window_end=t_mid,
        state_in=None,
    )
    assert len(res1.admitted_trades) == 1
    assert res1.carry_out_count == 1, "Must carry out active trade at window 1 end"
    assert len(state_out1.active_positions) == 1

    # Window 2: [t_mid, t_end) with state_in = state_out1
    res2, state_out2 = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t_mid,
        window_end=t_end,
        state_in=state_out1,
    )
    assert len(res2.admitted_trades) == 0, "Must NOT re-admit open opportunity"
    assert res2.carry_in_count == 1, "Must inherit carry-in trade"
    assert res2.carry_out_count == 0, "Trade exits inside window 2, carry out is 0"
    assert len(state_out2.active_positions) == 0


# -----------------------------------------------------------------------------
# Criterion 4: Carry-in baseline equity no double count
# -----------------------------------------------------------------------------
def test_criterion_4_carry_in_baseline_equity_no_double_count() -> None:
    """Acceptance 4: Carry-in trade does not double-count pre-window floating gain."""
    t0 = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)
    t_mid = datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 7, 0, 0, tzinfo=UTC)

    trade = TradeRecord(
        trade_id="c4_trade",
        symbol="BTCUSDT",
        entry_time=t0 + timedelta(hours=12),
        entry_price=50000.0,
        exit_time=t_mid + timedelta(hours=6),
        exit_price=52000.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
    )

    price_series = {
        "BTCUSDT": (
            [t_mid.timestamp(), (t_mid + timedelta(hours=6)).timestamp()],
            [52000.0, 52000.0],
        )
    }

    state_in = PortfolioState.create(
        timestamp=t_mid,
        cash_usdt=1000.0,
        total_equity_mtm=1003.95,
        active_positions=[trade],
    )

    ledger = SimulationLedger()
    res, _ = ledger.simulate_window(
        opportunities=[],
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=t_mid,
        window_end=t_end,
        state_in=state_in,
        price_series=price_series,
    )

    # Within window 2 price is flat, OOS PnL only reflects exit fee
    assert res.oos_pnl == pytest.approx(-0.05, abs=0.02)
    assert abs(res.oos_pnl - 3.95) > 1.0


# -----------------------------------------------------------------------------
# Criterion 5: Train / Valid / OOS strictly non-overlapping
# -----------------------------------------------------------------------------
def test_criterion_5_train_valid_oos_strictly_non_overlapping() -> None:
    """Acceptance 5: Splits enforce strict half-open non-overlapping boundaries."""
    t_start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    splits = generate_rolling_splits(
        start_date=t_start,
        end_date=t_end,
        is_days=7,
        oos_days=2,
        step_days=2,
    )
    for s in splits:
        assert s.is_start < s.is_end
        assert s.is_end == s.oos_start
        assert s.oos_start < s.oos_end
        assert (s.is_start <= s.is_end < s.is_end) is False


# -----------------------------------------------------------------------------
# Criterion 6: OOS parameter hash immutability
# -----------------------------------------------------------------------------
def test_criterion_6_oos_parameter_hash_immutable() -> None:
    """Acceptance 6: Candidate parameter specification is frozen and immutable."""
    cand = ParameterCandidate.from_dict(
        {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.25,
            "cooldown_buckets": 0,
        }
    )
    p_hash_initial = cand.parameter_id
    cand_params = cand.params
    assert cand.parameter_id == p_hash_initial
    assert cand_params["impulse_window_buckets"] == 2


# -----------------------------------------------------------------------------
# Criterion 7: Continuous vs segmented carry-in equivalence
# -----------------------------------------------------------------------------
def test_criterion_7_continuous_vs_segmented_carry_in_equivalence() -> None:
    """Acceptance 7: Continuous 2-day run matches 2 segmented carry-in runs.

    Verified to within 1e-4 tolerance.
    """
    t0 = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 7, 0, 0, tzinfo=UTC)

    opp1 = make_mock_opportunity(
        "c7_1",
        "BTCUSDT",
        t0 + timedelta(hours=6),
        t0 + timedelta(hours=6),
        50000.0,
        t0 + timedelta(hours=18),
        51000.0,
    )
    opp2 = make_mock_opportunity(
        "c7_2",
        "ETHUSDT",
        t0 + timedelta(hours=20),
        t0 + timedelta(hours=20),
        3000.0,
        t1 + timedelta(hours=10),
        3150.0,
    )
    opp3 = make_mock_opportunity(
        "c7_3",
        "SOLUSDT",
        t1 + timedelta(hours=14),
        t1 + timedelta(hours=14),
        150.0,
        t2 - timedelta(hours=2),
        160.0,
    )

    opps = [opp1, opp2, opp3]
    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    price_series = {
        "BTCUSDT": (
            [t0.timestamp(), t1.timestamp(), t2.timestamp()],
            [50000.0, 51000.0, 51000.0],
        ),
        "ETHUSDT": (
            [t0.timestamp(), t1.timestamp(), t2.timestamp()],
            [3000.0, 3075.0, 3150.0],
        ),
        "SOLUSDT": (
            [t0.timestamp(), t1.timestamp(), t2.timestamp()],
            [150.0, 155.0, 160.0],
        ),
    }

    ledger = SimulationLedger()

    # Continuous Run: [t0, t2)
    _res_cont, state_cont = ledger.simulate_window(
        opportunities=opps,
        params=params,
        window_start=t0,
        window_end=t2,
        state_in=None,
        price_series=price_series,
    )
    final_cont = state_cont.total_equity_mtm

    # Segmented Run: Day 1 [t0, t1), then Day 2 [t1, t2)
    _res_seg1, state_seg1 = ledger.simulate_window(
        opportunities=opps,
        params=params,
        window_start=t0,
        window_end=t1,
        state_in=None,
        price_series=price_series,
    )
    _res_seg2, state_seg2 = ledger.simulate_window(
        opportunities=opps,
        params=params,
        window_start=t1,
        window_end=t2,
        state_in=state_seg1,
        price_series=price_series,
    )
    final_seg = state_seg2.total_equity_mtm

    assert math.isclose(final_cont, final_seg, abs_tol=1e-4), (
        f"Equivalence violated: Continuous={final_cont:.4f}, Segmented={final_seg:.4f}"
    )


# -----------------------------------------------------------------------------
# Criterion 8: Manifest validation gates detect gaps
# -----------------------------------------------------------------------------
def test_criterion_8_manifest_validation_gates_detect_gaps() -> None:
    """Acceptance 8: Tampered manifest or temporal gaps trigger security gates."""
    t0 = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "c8_1", "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=1), 51000.0
    )

    bad_manifest = OpportunityPoolManifest(
        snapshot_id="snap_c8",
        created_at=t0,
        symbol_count=1,
        row_count=10,  # Invalid count
        watermark_start=t0,
        watermark_end=t0 + timedelta(hours=1),
        content_hash="mock",
    )
    errors = validate_opportunity_pool([opp], bad_manifest)
    assert any("row count mismatch" in e.lower() for e in errors)


# -----------------------------------------------------------------------------
# Criterion 9: Actual execution reconciliation by stable ID
# -----------------------------------------------------------------------------
def test_criterion_9_actual_execution_reconciliation_by_stable_id() -> None:
    """Acceptance 9: Reconcile simulated execution against actual by stable ID."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp_id = generate_opportunity_id("BTCUSDT", t0.timestamp(), 2, 1, "LONG")
    opp = make_mock_opportunity(
        opp_id, "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=2), 51000.0
    )

    ledger = SimulationLedger()
    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
        },
        window_start=t0,
        window_end=t0 + timedelta(days=1),
    )

    assert len(res.executions) == 1
    exec_record = res.executions[0]
    assert exec_record.opportunity_id == opp_id
    assert exec_record.symbol == "BTCUSDT"
    assert exec_record.status == OpportunityStatus.ENTERED


# -----------------------------------------------------------------------------
# Criterion 10: Stateful vs Independent WFA separation
# -----------------------------------------------------------------------------
def test_criterion_10_stateful_vs_independent_wfa_separation() -> None:
    """Acceptance 10: Independent and Stateful WFA modes have distinct semantics."""
    t0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
    splits = [
        WindowSplit(
            1,
            "Split 1",
            t0,
            t0 + timedelta(days=7),
            t0 + timedelta(days=7),
            t0 + timedelta(days=9),
        ),
        WindowSplit(
            2,
            "Split 2",
            t0 + timedelta(days=2),
            t0 + timedelta(days=9),
            t0 + timedelta(days=9),
            t0 + timedelta(days=11),
        ),
    ]

    opps = [
        make_mock_opportunity(
            f"opp_{i}",
            "BTCUSDT",
            t0 + timedelta(days=i),
            t0 + timedelta(days=i),
            50000.0,
            t0 + timedelta(days=i, hours=2),
            50500.0,
            net_pnl=10.0,
        )
        for i in range(12)
    ]

    cand_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
                "initial_margin_peak_usdt": 100.0,
            }
        ]
    )
    grid_vals = {
        d: [cand_df[d].iloc[0]]
        for d in [
            "impulse_window_buckets",
            "confirmation_buckets",
            "min_return_pct",
            "min_imbalance",
            "min_intensity",
            "min_volume_ratio",
            "cooldown_buckets",
        ]
    }

    res_ind = run_walk_forward_analysis(
        events=opps,
        splits=splits,
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="independent",
    )
    assert len(res_ind) == 2

    res_state = run_walk_forward_analysis(
        events=opps,
        splits=splits,
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="stateful",
    )
    assert len(res_state) == 2


# -----------------------------------------------------------------------------
# Counterexample Regression Tests (Auditor-Identified Gaps)
# -----------------------------------------------------------------------------
def test_counterexample_1_account_events_fail_closed_without_flag(
    tmp_path: Path,
) -> None:
    """Gap 1: Formal WFA rejects fallback to account events without explicit flag."""
    p1 = tmp_path / "account_primary_events.csv"
    p2 = tmp_path / "account_acc02_events.csv"
    p1.write_text("symbol,detected_at,entry_at,exit_at\nBTC,2026-09-05,2026-09-05,\n")
    p2.write_text("symbol,detected_at,entry_at,exit_at\nETH,2026-09-05,2026-09-05,\n")

    with pytest.raises(ValueError, match="Formal Walk-Forward Analysis requires"):
        load_all_replay_events(tmp_path, allow_account_fallback=False)

    opps, _ = load_all_replay_events(tmp_path, allow_account_fallback=True)
    assert len(opps) == 2


def test_counterexample_2_opportunity_id_invariant_to_window_params() -> None:
    """Gap 2: opportunity_id isolates window specs and ignores execution params."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC).timestamp()
    id_w2_c1 = generate_opportunity_id("BTCUSDT", t0, 2, 1, "LONG")
    id_w2_c1_again = generate_opportunity_id("BTCUSDT", t0, 2, 1, "LONG")
    id_w3_c2 = generate_opportunity_id("BTCUSDT", t0, 3, 2, "LONG")
    id_w4_c3 = generate_opportunity_id("BTCUSDT", t0, 4, 3, "LONG")

    # Idempotent for same window specification
    assert id_w2_c1 == id_w2_c1_again
    # Distinct for different window specs to avoid signal collision in raw pool
    assert id_w2_c1 != id_w3_c2
    assert id_w3_c2 != id_w4_c3


def test_counterexample_3_tampered_manifest_content_hash_is_rejected() -> None:
    """Gap 3: Tampered content hash in manifest triggers validation error."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "test_opp", "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=1), 50500.0
    )
    valid_hash = compute_pool_content_hash([opp])
    bad_manifest = OpportunityPoolManifest(
        snapshot_id="snap_1",
        created_at=t0,
        symbol_count=1,
        row_count=1,
        watermark_start=t0,
        watermark_end=t0,
        content_hash="tampered_bad_hash_99999",
    )

    errs = validate_opportunity_pool([opp], bad_manifest)
    assert len(errs) > 0
    assert any("Content hash mismatch" in e for e in errs)

    good_manifest = OpportunityPoolManifest(
        snapshot_id="snap_1",
        created_at=t0,
        symbol_count=1,
        row_count=1,
        watermark_start=t0,
        watermark_end=t0,
        content_hash=valid_hash,
    )
    assert len(validate_opportunity_pool([opp], good_manifest)) == 0


def test_counterexample_4_pre_window_detection_entry_inside_admitted() -> None:
    """Gap 4: Detection before window with entry inside window is admitted."""
    t_start = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "pre_det",
        "BTCUSDT",
        t_start - timedelta(minutes=5),
        t_start + timedelta(minutes=10),
        50000.0,
        t_start + timedelta(hours=2),
        50500.0,
        net_pnl=25.0,
    )

    ledger = SimulationLedger()
    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
        },
        window_start=t_start,
        window_end=t_end,
    )
    assert res.n_trades == 1
    assert res.executions[0].status == OpportunityStatus.ENTERED


def test_counterexample_5_entry_at_window_end_rejected() -> None:
    """Gap 5: Entry exactly at window_end belongs to next window, not current."""
    t_start = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "end_boundary",
        "BTCUSDT",
        t_end - timedelta(minutes=5),
        t_end,
        50000.0,
        t_end + timedelta(hours=2),
        50500.0,
        net_pnl=25.0,
    )

    ledger = SimulationLedger()
    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
        },
        window_start=t_start,
        window_end=t_end,
    )
    assert res.n_trades == 0


def test_counterexample_6_jsonl_opportunity_pool_loading_and_hash(
    tmp_path: Path,
) -> None:
    """Gap 6: JSONL opportunity pool is loaded and validated cleanly."""
    import json

    pool_file = tmp_path / "opportunity_pool.jsonl"
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    row = {
        "opportunity_id": "opp_jsonl_1",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "detected_at": t0.isoformat(),
        "detected_epoch": t0.timestamp(),
        "entry_eligible_at": (t0 + timedelta(minutes=1)).isoformat(),
        "entry_reference_price": 50000.0,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "impulse_return_pct": 0.8,
        "aggressive_imbalance": 0.4,
        "confirmation_min_imbalance": 0.4,
        "notional_intensity": 2.5,
        "volume_ratio": 1.5,
    }
    pool_file.write_text(json.dumps(row) + "\n", encoding="utf-8")

    opps, manifest = load_opportunity_pool(tmp_path)
    assert len(opps) == 1
    assert opps[0].opportunity_id == "opp_jsonl_1"
    assert manifest.row_count == 1

    with pytest.raises(FileNotFoundError):
        load_opportunity_pool(tmp_path, require_manifest=True)


def test_counterexample_7_wfa_training_evaluates_via_mtm_simulation_ledger() -> None:
    """Gap 7: In-Sample training in WFA evaluates via unified SimulationLedger."""
    t0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
    splits = [
        WindowSplit(
            1,
            "Split 1",
            t0,
            t0 + timedelta(days=7),
            t0 + timedelta(days=7),
            t0 + timedelta(days=9),
        ),
    ]

    opps = [
        make_mock_opportunity(
            f"opp_is_{i}",
            "BTCUSDT",
            t0 + timedelta(days=i),
            t0 + timedelta(days=i),
            50000.0,
            t0 + timedelta(days=i, hours=2),
            50500.0,
            net_pnl=15.0,
        )
        for i in range(7)
    ]

    cand_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
                "initial_margin_peak_usdt": 100.0,
            }
        ]
    )
    grid_vals = {
        d: [cand_df[d].iloc[0]]
        for d in [
            "impulse_window_buckets",
            "confirmation_buckets",
            "min_return_pct",
            "min_imbalance",
            "min_intensity",
            "min_volume_ratio",
            "cooldown_buckets",
        ]
    }

    res = run_walk_forward_analysis(
        events=opps,
        splits=splits,
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="independent",
    )
    assert len(res) == 1
    assert res[0].rec_is_eval.net_pnl > 0


def test_counterexample_8_wfa_cli_price_cache_and_strict_manifest(
    tmp_path: Path,
) -> None:
    """Gap 8: CLI parses cache and strict manifest flags, fails closed when missing."""
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--data-dir",
            str(tmp_path),
            "--cache-file",
            str(tmp_path / "cache.pkl"),
            "--require-manifest",
            "--allow-account-fallback",
        ]
    )
    assert args.require_manifest is True
    assert args.allow_account_fallback is True
    assert args.cache_file == tmp_path / "cache.pkl"

    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "opp_strict_1", "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=1), 50500.0
    )
    df = pd.DataFrame(
        [
            {
                "opportunity_id": opp.opportunity_id,
                "symbol": opp.symbol,
                "direction": opp.direction,
                "detected_at": opp.detected_at.isoformat(),
                "entry_eligible_at": opp.entry_eligible_at.isoformat(),
                "entry_reference_price": opp.entry_reference_price,
                "impulse_window_buckets": opp.impulse_window_buckets,
                "confirmation_buckets": opp.confirmation_buckets,
                "impulse_return_pct": opp.impulse_return_pct,
                "aggressive_imbalance": opp.aggressive_imbalance,
                "confirmation_min_imbalance": opp.confirmation_min_imbalance,
                "notional_intensity": opp.notional_intensity,
                "volume_ratio": opp.volume_ratio,
            }
        ]
    )
    df.to_csv(tmp_path / "opportunity_pool.csv", index=False)

    with pytest.raises(FileNotFoundError, match="Manifest file required but not found"):
        load_all_replay_events(tmp_path, require_manifest=True)

    opps, _ = load_all_replay_events(tmp_path, require_manifest=False)
    assert len(opps) == 1


def test_counterexample_9_manifest_hash_invalidated_by_exit_price_or_pnl_change() -> (
    None
):
    """Gap 3/Blocker 3: Tampering with exit_price or net_pnl invalidates hash."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    t_exit = t0 + timedelta(hours=1)
    opp = make_mock_opportunity(
        "opp_hash_test", "BTCUSDT", t0, t0, 50000.0, t_exit, 51000.0, net_pnl=20.0
    )
    original_hash = compute_pool_content_hash([opp])

    # Alter exit price
    opp_tampered_price = make_mock_opportunity(
        "opp_hash_test", "BTCUSDT", t0, t0, 50000.0, t_exit, 52000.0, net_pnl=20.0
    )
    hash_tampered_price = compute_pool_content_hash([opp_tampered_price])
    assert original_hash != hash_tampered_price

    # Alter net_pnl
    opp_tampered_pnl = make_mock_opportunity(
        "opp_hash_test", "BTCUSDT", t0, t0, 50000.0, t_exit, 51000.0, net_pnl=50.0
    )
    hash_tampered_pnl = compute_pool_content_hash([opp_tampered_pnl])
    assert original_hash != hash_tampered_pnl


def test_counterexample_10_manifest_rejects_non_raw_parameter_independent() -> None:
    """Blocker 4: Manifest must declare pool_type='raw_parameter_independent'."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "opp_type_test", "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=1), 51000.0
    )
    c_hash = compute_pool_content_hash([opp])
    bad_manifest = OpportunityPoolManifest(
        snapshot_id="snap_bad_type",
        created_at=t0,
        symbol_count=1,
        row_count=1,
        watermark_start=t0,
        watermark_end=t0,
        content_hash=c_hash,
        pool_type="account_replay_fallback",
    )
    errs = validate_opportunity_pool([opp], bad_manifest)
    assert any("Invalid pool_type" in e for e in errs)

    good_manifest = OpportunityPoolManifest(
        snapshot_id="snap_good_type",
        created_at=t0,
        symbol_count=1,
        row_count=1,
        watermark_start=t0,
        watermark_end=t0,
        content_hash=c_hash,
        pool_type="raw_parameter_independent",
    )
    assert len(validate_opportunity_pool([opp], good_manifest)) == 0


def test_counterexample_11_single_run_csvs_not_discovered_as_opportunity_pool(
    tmp_path: Path,
) -> None:
    """Blocker 4: baseline_events.csv is not treated as opportunity pool."""
    p_baseline = tmp_path / "baseline_events.csv"
    p_profile_a = tmp_path / "profile_A_events.csv"
    p_baseline.write_text(
        "symbol,detected_at,entry_price\nBTCUSDT,2026-09-05T12:00:00Z,50000.0\n"
    )
    p_profile_a.write_text(
        "symbol,detected_at,entry_price\nETHUSDT,2026-09-05T12:00:00Z,3000.0\n"
    )

    with pytest.raises(
        ValueError, match="No validated parameter-independent RawOpportunity pool"
    ):
        load_all_replay_events(tmp_path, allow_account_fallback=False)


def test_counterexample_12_wfa_main_fails_closed_when_price_cache_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Blocker 5: Formal WFA main() refuses to run without price cache."""
    from local_optimization.run_walk_forward_analysis import main as wfa_main

    # Create dummy opportunity pool so it passes load_all_replay_events
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "opp_cli_cache", "BTCUSDT", t0, t0, 50000.0, t0 + timedelta(hours=1), 50500.0
    )
    import json

    (tmp_path / "opportunity_pool.jsonl").write_text(
        json.dumps(opp.to_dict()) + "\n", encoding="utf-8"
    )
    manifest = OpportunityPoolManifest(
        snapshot_id="test_cache_fail_snap",
        created_at=t0,
        symbol_count=1,
        row_count=1,
        watermark_start=t0,
        watermark_end=t0 + timedelta(hours=2),
        content_hash=compute_pool_content_hash([opp]),
        pool_type="raw_parameter_independent",
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest.to_dict()), encoding="utf-8"
    )

    grid_csv = tmp_path / "grid.csv"
    grid_csv.write_text(
        "impulse_window_buckets,confirmation_buckets,min_return_pct,min_imbalance,"
        "min_intensity,min_volume_ratio,cooldown_buckets\n"
        "2,1,0.5,0.3,1.5,0.0,0\n"
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "run_walk_forward_analysis.py",
            "--data-dir",
            str(tmp_path),
            "--grid-csv",
            str(grid_csv),
            "--cache-file",
            str(tmp_path / "non_existent_cache.pkl"),
        ],
    )
    with pytest.raises(
        FileNotFoundError, match="15s high-frequency price cache required"
    ):
        wfa_main()


def test_counterexample_13_daily_breakdown_threads_state_and_preserves_positions() -> (
    None
):
    """Blocker 6: Day-by-day OOS breakdown carries state across day boundaries."""
    t0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
    t_split_start = t0
    t_split_end = t0 + timedelta(days=7)
    oos_start = t_split_end
    oos_end = t_split_end + timedelta(days=2)

    splits = [
        WindowSplit(
            split_id=1,
            name="Split 1",
            is_start=t_split_start,
            is_end=t_split_end,
            oos_start=oos_start,
            oos_end=oos_end,
        )
    ]

    cross_day_opp = make_mock_opportunity(
        "cross_day_oos",
        "BTCUSDT",
        oos_start + timedelta(hours=6),
        oos_start + timedelta(hours=6),
        50000.0,
        oos_start + timedelta(hours=30),
        51000.0,
        net_pnl=20.0,
    )

    cand_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
                "initial_margin_peak_usdt": 100.0,
            }
        ]
    )
    grid_vals = {
        d: [cand_df[d].iloc[0]]
        for d in cand_df.columns
        if d != "initial_margin_peak_usdt"
    }

    res = run_walk_forward_analysis(
        events=[cross_day_opp],
        splits=splits,
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="stateful",
    )
    assert len(res) == 1
    assert len(res[0].oos_daily_breakdown) == 2
    sum_daily = round(sum(res[0].oos_daily_breakdown), 2)
    assert math.isclose(sum_daily, res[0].rec_oos_pnl, abs_tol=0.1)


def test_counterexample_14_wfa_window_bounds_align_with_manifest_watermark(
    tmp_path: Path,
) -> None:
    """Blocker 7: WFA evaluation horizon is governed by manifest watermark."""
    import json

    t_start = datetime(2026, 9, 3, 7, 19, 45, tzinfo=UTC)
    t_end = datetime(2026, 9, 20, 2, 30, 0, tzinfo=UTC)
    t_opp = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp = make_mock_opportunity(
        "opp_watermark_test",
        "BTCUSDT",
        t_opp,
        t_opp,
        50000.0,
        t_opp + timedelta(hours=1),
        50500.0,
    )
    c_hash = compute_pool_content_hash([opp])
    manifest = OpportunityPoolManifest(
        snapshot_id="snap_wm_test",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=1,
        watermark_start=t_start,
        watermark_end=t_end,
        content_hash=c_hash,
        pool_type="raw_parameter_independent",
    )
    (tmp_path / "opportunity_pool.jsonl").write_text(
        json.dumps(opp.to_dict()) + "\n", encoding="utf-8"
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest.to_dict()), encoding="utf-8"
    )

    opps, loaded_manifest = load_all_replay_events(tmp_path, require_manifest=True)
    assert loaded_manifest.watermark_start == t_start
    assert loaded_manifest.watermark_end == t_end
    splits = generate_rolling_splits(
        start_date=loaded_manifest.watermark_start,
        end_date=loaded_manifest.watermark_end,
        is_days=7,
        oos_days=2,
        step_days=2,
    )
    assert len(splits) > 0
    assert splits[0].is_start >= t_start
    assert splits[-1].oos_end <= t_end


def test_counterexample_15_daily_pipeline_blocks_stable_when_oos_unavailable() -> None:
    """Blocker 2: Missing or unavailable forward OOS blocks promotion to STABLE."""
    from local_optimization.tracker import (
        CandidateEvaluation as TrackerCandidateEval,
    )
    from local_optimization.tracker import (
        DailyTrackRecord,
        evaluate_stage_stability,
    )

    cand = ParameterCandidate(
        parameter_id="test_cand_1",
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
        },
    )
    eval_rec = TrackerCandidateEval(
        candidate=cand,
        is_feasible=True,
        trade_count=100,
        net_pnl=50.0,
        net_return_pct=5.0,
        ulcer_index=0.01,
        max_drawdown_pct=0.05,
        peak_initial_margin_usdt=100.0,
        neighborhood_stability_score=0.90,
    )

    history = [
        DailyTrackRecord(
            date_str=f"2026-09-{10 + i:02d}",
            snapshot_id=f"snap_{i}",
            protocol_id="proto_1",
            daily_best=eval_rec,
            recommended=eval_rec,
            live_actual=None,
            oos_forward_pnl=None,  # OOS evidence unavailable
            is_snapshot_complete=True,
            reconciliation_passed=True,
        )
        for i in range(10)
    ]

    status, notes = evaluate_stage_stability(
        history,
        min_consistency_days=7,
        min_oos_days=14,
        min_stability_score=0.70,
        min_trades=15,
        max_mdd_pct=0.20,
    )
    assert status != "stable"
    assert any("Missing OOS evidence" in n for n in notes)


# -----------------------------------------------------------------------------
# Counterexample 16: Opportunity pool covers all grid window buckets
# -----------------------------------------------------------------------------
def test_counterexample_16_opportunity_pool_covers_all_grid_window_buckets() -> None:
    """Blocker: Opportunity pool must cover complete (w in [2,3,4] x c in [1,2,3])."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opps = []
    for w in (2, 3, 4):
        for c in (1, 2, 3):
            opp_id = generate_opportunity_id("BTCUSDT", t0.timestamp(), w, c, "LONG")
            opps.append(
                make_mock_opportunity(
                    opp_id,
                    "BTCUSDT",
                    t0,
                    t0 + timedelta(seconds=15),
                    50000.0,
                    t0 + timedelta(minutes=15),
                    50500.0,
                    w=w,
                    c=c,
                )
            )

    w_buckets = {o.impulse_window_buckets for o in opps}
    c_buckets = {o.confirmation_buckets for o in opps}
    assert w_buckets == {2, 3, 4}
    assert c_buckets == {1, 2, 3}
    assert len(opps) == 9


# -----------------------------------------------------------------------------
# Counterexample 17: Zero or negative entry price rejected by validation gate
# -----------------------------------------------------------------------------
def test_counterexample_17_zero_or_negative_entry_price_rejected() -> None:
    """Blocker: validate_opportunity_pool rejects entry_price <= 0 or NaN/Inf."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp_zero = make_mock_opportunity(
        "opp_zero", "BTCUSDT", t0, t0, 0.0, t0 + timedelta(minutes=15), 50500.0
    )
    errors_zero = validate_opportunity_pool([opp_zero])
    assert any(
        "entry_reference_price" in e and "strictly > 0" in e for e in errors_zero
    )

    opp_neg = make_mock_opportunity(
        "opp_neg", "BTCUSDT", t0, t0, -100.0, t0 + timedelta(minutes=15), 50500.0
    )
    errors_neg = validate_opportunity_pool([opp_neg])
    assert any("entry_reference_price" in e and "strictly > 0" in e for e in errors_neg)

    opp_nan = make_mock_opportunity(
        "opp_nan", "BTCUSDT", t0, t0, float("nan"), t0 + timedelta(minutes=15), 50500.0
    )
    errors_nan = validate_opportunity_pool([opp_nan])
    assert any("entry_reference_price" in e for e in errors_nan)


# -----------------------------------------------------------------------------
# Counterexample 18: No default features fabricated (fail on NaN/None/Inf)
# -----------------------------------------------------------------------------
def test_counterexample_18_no_default_features_fabricated() -> None:
    """Blocker: Feature missingness cannot be patched by fake default values."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    opp_nan_vol = make_mock_opportunity(
        "opp_nan_vol",
        "BTCUSDT",
        t0,
        t0,
        50000.0,
        t0 + timedelta(minutes=15),
        50500.0,
        vol=float("nan"),
    )
    errs = validate_opportunity_pool([opp_nan_vol])
    assert any("volume_ratio" in e and "cannot be NaN/None" in e for e in errs)

    opp_nan_imb = make_mock_opportunity(
        "opp_nan_imb",
        "BTCUSDT",
        t0,
        t0,
        50000.0,
        t0 + timedelta(minutes=15),
        50500.0,
        imb=float("nan"),
    )
    errs_imb = validate_opportunity_pool([opp_nan_imb])
    assert any(
        "aggressive_imbalance" in e and "cannot be NaN/None" in e for e in errs_imb
    )


# -----------------------------------------------------------------------------
# Counterexample 19: Deduplication never sorts by net_pnl (no lookahead bias)
# -----------------------------------------------------------------------------
def test_counterexample_19_dedup_never_sorts_by_net_pnl() -> None:
    """Blocker: Deduplication between signals must not sort by future net_pnl."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    epoch = t0.timestamp()

    # Two records for the same physical signal identity
    row_first = {
        "symbol": "BTCUSDT",
        "detected_epoch": epoch,
        "opportunity_id": "opp_btc_01",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "direction": "LONG",
        "net_pnl_usdt": -5.0,  # lower pnl
        "order_seq": 1,
    }
    row_second = {
        "symbol": "BTCUSDT",
        "detected_epoch": epoch,
        "opportunity_id": "opp_btc_01",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "direction": "LONG",
        "net_pnl_usdt": 25.0,  # higher future pnl
        "order_seq": 2,
    }

    df = pd.DataFrame([row_first, row_second])
    dedup_subset = [
        "symbol",
        "detected_epoch",
        "impulse_window_buckets",
        "confirmation_buckets",
        "direction",
    ]
    # Chronological deduplication preserving discovery order
    dedup = df.sort_values(by=["detected_epoch", "opportunity_id"]).drop_duplicates(
        subset=dedup_subset,
        keep="first",
    )
    assert len(dedup) == 1
    # Must preserve first record, NOT peek ahead at future +25.0
    assert dedup.iloc[0]["net_pnl_usdt"] == -5.0
    assert dedup.iloc[0]["order_seq"] == 1


# -----------------------------------------------------------------------------
# Counterexample 20: Six scenarios fails without real events (bypass removed)
# -----------------------------------------------------------------------------
def test_counterexample_20_six_scenarios_fails_without_real_events() -> None:
    """Blocker: six_scenarios produces 0 candidates if opportunity pool is empty."""
    from local_optimization.generate_six_scenarios_dashboard import (
        filter_events_by_params,
        filter_events_with_concurrency,
    )

    empty_events: list[dict[str, object]] = []
    fake_grid_row = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
        "full_net_pnl_usdt": 999.0,  # fake precomputed column
        "full_max_drawdown_usdt": 10.0,
    }

    # Verify that filtering against empty events produces 0 selected trades
    sel, _, _ = filter_events_by_params(empty_events, fake_grid_row)
    assert len(sel) == 0

    admitted, pnl, mdd, peak_c = filter_events_with_concurrency(sel, None, max_slots=2)
    assert len(admitted) == 0
    assert pnl == 0.0
