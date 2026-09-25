"""Unit tests for 6-scenario optimizer and 8D MTM comparison dashboard."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    build_reconciliation_payload,
    compute_8d_neighborhood_stability,
    evaluate_daily_compounding,
    filter_events_with_concurrency,
    format_8d_param_str,
    to_trade_records,
)


def test_format_8d_param_str() -> None:
    """Verify 8D parameter string formatting."""
    params = {
        "impulse_window_buckets": 3,
        "confirmation_buckets": 1,
        "min_return_pct": 1.25,
        "min_imbalance": 0.35,
        "min_intensity": 2.5,
        "min_volume_ratio": 1.5,
        "cooldown_buckets": 4,
        "max_open_positions": 2,
    }
    s = format_8d_param_str(params)
    assert s == "3/1/1.25%/0.35/2.5/1.5x/cd=4/slots=2"


def test_filter_events_with_concurrency_slots() -> None:
    """Verify single-symbol slots and multi-symbol concurrency."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    t0_ep = t0.timestamp()

    # Part A: 3 overlapping events of the SAME symbol (BTCUSDT)
    ev1_btc = {
        "symbol": "BTCUSDT",
        "detected_at": t0,
        "detected_epoch": t0_ep,
        "entry_at": t0,
        "entry_epoch": t0_ep,
        "exit_epoch": t0_ep + 600,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "impulse_return_pct": 0.8,
        "min_imbalance": 0.35,
        "confirmation_min": 0.35,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "net_pnl_usdt": 5.0,
    }
    ev2_btc = {
        "symbol": "BTCUSDT",
        "detected_at": t0 + timedelta(minutes=2),
        "detected_epoch": t0_ep + 120,
        "entry_at": t0 + timedelta(minutes=2),
        "entry_epoch": t0_ep + 120,
        "exit_epoch": t0_ep + 720,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "impulse_return_pct": 0.8,
        "min_imbalance": 0.35,
        "confirmation_min": 0.35,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "net_pnl_usdt": 3.0,
    }
    ev3_btc = {
        "symbol": "BTCUSDT",
        "detected_at": t0 + timedelta(minutes=4),
        "detected_epoch": t0_ep + 240,
        "entry_at": t0 + timedelta(minutes=4),
        "entry_epoch": t0_ep + 240,
        "exit_epoch": t0_ep + 840,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "impulse_return_pct": 0.8,
        "min_imbalance": 0.35,
        "confirmation_min": 0.35,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "net_pnl_usdt": -10.0,
    }
    btc_events = [ev1_btc, ev2_btc, ev3_btc]
    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "cooldown_buckets": 0,
    }

    # Case 1: Max 1 slot per symbol -> only ev1_btc admitted
    adm1, pnl1, _, peak1 = filter_events_with_concurrency(
        btc_events, params, max_slots=1
    )
    assert len(adm1) == 1
    assert adm1[0]["symbol"] == "BTCUSDT"
    assert pnl1 == 5.0
    assert peak1 == 1

    # Case 2: Max 2 slots per symbol -> ev1_btc and ev2_btc admitted, ev3_btc rejected
    adm2, pnl2, _, peak2 = filter_events_with_concurrency(
        btc_events, params, max_slots=2
    )
    assert len(adm2) == 2
    assert pnl2 == 8.0  # Avoided ev3_btc's -10 loss
    assert peak2 == 2

    # Case 3: Max 3 slots per symbol -> all 3 admitted
    adm3, pnl3, _, peak3 = filter_events_with_concurrency(
        btc_events, params, max_slots=3
    )
    assert len(adm3) == 3
    assert pnl3 == -2.0
    assert peak3 == 3

    # Part B: Multi-symbol free concurrency (2 BTC + 2 ETH overlapping concurrently)
    ev1_eth = {**ev1_btc, "symbol": "ETHUSDT", "net_pnl_usdt": 4.0}
    ev2_eth = {**ev2_btc, "symbol": "ETHUSDT", "net_pnl_usdt": 6.0}
    multi_events = [ev1_btc, ev1_eth, ev2_btc, ev2_eth]

    # With slots=2 per symbol, both BTC and both ETH are admitted (total 4 admitted)
    adm_multi, pnl_multi, _, peak_multi = filter_events_with_concurrency(
        multi_events, params, max_slots=2
    )
    assert len(adm_multi) == 4
    assert pnl_multi == 5.0 + 4.0 + 3.0 + 6.0  # 18.0
    # Peak concurrency across all symbols in the account reaches 4
    assert peak_multi == 4


def test_evaluate_daily_compounding_metrics() -> None:
    """Verify daily compounding metrics calculation and UI penalty."""
    t0 = datetime(2026, 9, 3, 10, 0, tzinfo=UTC)
    events = [
        {
            "entry_at": t0,
            "exit_at": t0 + timedelta(minutes=15),
            "net_pnl_usdt": 10.0,
        },
        {
            "entry_at": t0 + timedelta(days=1),
            "exit_at": t0 + timedelta(days=1, minutes=15),
            "net_pnl_usdt": 15.0,
        },
        {
            "entry_at": t0 + timedelta(days=2),
            "exit_at": t0 + timedelta(days=2, minutes=15),
            "net_pnl_usdt": 5.0,
        },
    ]
    score, mdd, ui, end_eq, peak_margin = evaluate_daily_compounding(
        events, initial_equity=1000.0, f=0.10
    )
    assert end_eq > 1000.0
    assert mdd == 0.0  # strictly monotonic positive PnL
    assert ui == 0.0
    assert score > 0.0
    assert peak_margin > 0.0


def test_compute_8d_neighborhood_stability() -> None:
    """Verify 8-dimensional neighborhood stability computation."""
    dim_keys = [
        "impulse_window_buckets",
        "confirmation_buckets",
        "min_return_pct",
        "min_imbalance",
        "min_intensity",
        "min_volume_ratio",
        "cooldown_buckets",
        "max_open_positions",
    ]
    grid_vals = {
        "impulse_window_buckets": [2, 3],
        "confirmation_buckets": [1, 2],
        "min_return_pct": [0.5, 1.0],
        "min_imbalance": [0.3, 0.4],
        "min_intensity": [2.0, 4.0],
        "min_volume_ratio": [1.0, 1.5],
        "cooldown_buckets": [0, 4],
        "max_open_positions": [1, 2],
    }

    # Construct two adjacent candidates
    k1 = (2, 1, 0.5, 0.3, 2.0, 1.0, 0, 1)
    k2 = (2, 1, 0.5, 0.3, 2.0, 1.0, 0, 2)  # Neighbor differing only in slots

    c1 = Candidate8D(
        params=dict(zip(dim_keys, k1, strict=True)),
        net_pnl=100.0,
        mdd=10.0,
        calmar=10.0,
        compounding_score=0.05,
        compounding_mdd=0.02,
        compounding_ui=0.01,
        terminal_compounded_equity=1050.0,
        n_trades=25,
        peak_margin=20.0,
    )
    c2 = Candidate8D(
        params=dict(zip(dim_keys, k2, strict=True)),
        net_pnl=95.0,
        mdd=12.0,
        calmar=7.9,
        compounding_score=0.045,
        compounding_mdd=0.03,
        compounding_ui=0.015,
        terminal_compounded_equity=1045.0,
        n_trades=28,
        peak_margin=40.0,
    )

    cands_map = {k1: c1, k2: c2}
    compute_8d_neighborhood_stability(cands_map, grid_vals, dim_keys)

    # Both should have computed stability > 0
    assert c1.stability > 0.0
    assert c2.stability > 0.0


def test_to_trade_records_conversion() -> None:
    """Verify conversion to TradeRecord dataclasses."""
    t0 = datetime(2026, 9, 3, 10, 0, tzinfo=UTC)
    events: list[dict[str, Any]] = [
        {
            "symbol": "BTCUSDT",
            "entry_at": t0,
            "entry_epoch": t0.timestamp(),
            "exit_epoch": (t0 + timedelta(minutes=15)).timestamp(),
            "entry_price": 58000.0,
            "exit_price": 58500.0,
            "net_pnl_usdt": 2.5,
        }
    ]
    records = to_trade_records(events, compounding_scale=False)
    assert len(records) == 1
    assert records[0].symbol == "BTCUSDT"
    assert records[0].entry_price == 58000.0
    assert records[0].exit_price == 58500.0
    assert records[0].notional_usdt == 100.0


def test_build_reconciliation_payload(tmp_path: Any) -> None:
    """Verify build_reconciliation_payload handles both real data and fallback."""
    # Test with empty tmp_path (fallback path)
    res = build_reconciliation_payload(tmp_path, prices_by_symbol={})
    assert "accounts" in res
    assert "primary" in res["accounts"]
    assert "acc01" in res["accounts"]
    assert "acc02" in res["accounts"]
    status = res["accounts"]["primary"]["status_label"]
    assert status == "⚠️ 实盘数据缺失 (INSUFFICIENT_DATA)"
    assert res["accounts"]["primary"]["layers"]["L1"]["stat"] == "NA"
