"""Counterexample regression test suite addressing Astra Round 3 review (2026-09-20).

Validates all 6 P1/P2 issues identified in
docs/code-review-2026-09-20-local-optimization-round3.md:
1. MTM carry-in initial equity offset: no double-counting of pre-window floating gains.
2. Snapshot content validation: valid headers + garbage rows must fail.
3. Live reconciliation: garbage event files must yield INSUFFICIENT or FAIL.
4. Gate check: recon_passed must reject DIVERGED, FAIL, and INSUFFICIENT.
5. Ambiguous account identity tokens: records missing account IDs cannot falsely match.
6. Compounding selection: verified by 15s MTM, candidates exceeding 15% MDD rejected.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    build_reconciliation_payload,
    select_compounding,
)
from local_optimization.mtm_engine import (
    TradeRecord,
    reconstruct_mtm_equity,
)
from local_optimization.snapshot import inspect_snapshot_dir


def test_counterexample_mtm_carry_in_floating_gain_not_double_counted():
    """P1 #6: When initial_equity is 1010U with 10U carry-in unrealized gain,

    the first MTM equity point must be exactly 1010.0U, not 1020.0U.
    """
    t_req_start = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC).timestamp()
    entry_time = datetime(2026, 9, 9, 22, 0, 0, tzinfo=UTC)  # 2 hours before window
    exit_time = datetime(2026, 9, 10, 4, 0, 0, tzinfo=UTC)  # 4 hours after window

    # Trade: bought 1 unit of BTC at 100.0, notional 100.0
    carry_in_trade = TradeRecord(
        trade_id=101,
        symbol="BTCUSDT",
        entry_time=entry_time,
        entry_price=100.0,
        exit_time=exit_time,
        exit_price=115.0,
        notional_usdt=100.0,
        leverage=1.0,
        fee_rate=0.0,
        direction="LONG",
        is_open=False,
    )

    # Price series:
    # At t_req_start (00:00:00), price is 110.0 (+10U unrealized gain vs entry 100.0)
    # At 02:00:00, price is 112.0 (+12U relative to entry, so +2U vs window start)
    # At 04:00:00, price is 115.0 (+15U relative to entry, so +5U vs window start)
    timestamps = [
        t_req_start,
        t_req_start + 7200,
        t_req_start + 14400,
    ]
    prices = [110.0, 112.0, 115.0]
    prices_by_symbol = {"BTCUSDT": (timestamps, prices)}

    # Initial equity passed from live balance at t_req_start is 1010.0U
    # (which ALREADY includes the +10U floating gain on the account!)
    initial_equity = 1010.0

    points = reconstruct_mtm_equity(
        trades=[carry_in_trade],
        price_series=prices_by_symbol,
        initial_equity=initial_equity,
        grid_seconds=15,
        start_time=datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 9, 10, 4, 0, 0, tzinfo=UTC),
        is_total_equity=True,
    )

    assert len(points) > 0

    # FIRST POINT: Must be exactly 1010.0, NEVER 1019.95U or 1020.0U!
    assert points[0].equity == pytest.approx(1010.0, abs=1e-3)

    # MID POINT (price = 112.0, +2.0U gain inside window):
    mid_points = [
        p for p in points if abs(p.timestamp.timestamp() - (t_req_start + 7200)) < 15
    ]
    assert len(mid_points) > 0
    assert mid_points[0].equity == pytest.approx(1012.0, abs=1e-3)

    # FINAL POINT AFTER EXIT (price = 115.0, +5.0U gain inside window):
    assert points[-1].equity == pytest.approx(1015.0, abs=1e-3)


def test_counterexample_snapshot_valid_header_with_garbage_rows_fails(tmp_path: Path):
    """P1 #5: Snapshot directory with valid headers but garbage rows must be marked

    is_complete=False, missing the stream, and withhold ready tags.
    """
    # Create valid headers but garbage rows
    files_to_create = {
        "account_balance_usdt.csv": (
            "wallet_balance,observed_at\ngarbage_val,not_a_time\n"
        ),
        "live_strategy_signals.csv": (
            "symbol,direction,timestamp\nXYZ,INVALID_DIR,not_a_time\n"
        ),
        "account_fill_events.csv": (
            "order_id,symbol,price,qty,fill_time\n1,GARBAGE,-10,0,not_a_time\n"
        ),
        "exchange_orders.csv": (
            "order_id,symbol,status,side,created_at\n"
            "1,TRASH,INVALID_STATUS,FOO,not_a_time\n"
        ),
        "order_intents.csv": (
            "intent_id,symbol,decision,created_at\n1,BAD,INVALID_DECISION,not_a_time\n"
        ),
    }

    for fn, content in files_to_create.items():
        (tmp_path / fn).write_text(content, encoding="utf-8")

    manifest = inspect_snapshot_dir(
        snapshot_dir=tmp_path,
        target_cutoff=datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC),
    )

    assert manifest.is_complete is False
    assert len(manifest.missing_streams) == 5
    assert "decision_replay_ready" not in manifest.capability_tags
    assert "execution_audit_ready" not in manifest.capability_tags


def test_counterexample_live_recon_with_garbage_event_files_fails(tmp_path: Path):
    """P1 #4: Live reconciliation directory with balance but garbage event files

    must yield INSUFFICIENT_EVIDENCE or FAIL, never PASS.
    """
    primary_dir = tmp_path / "primary"
    primary_dir.mkdir(parents=True)

    # Valid balance rows
    balance_rows = [
        "wallet_balance,observed_at\n",
        "100.0,2026-09-20T00:00:00Z\n",
        "100.5,2026-09-20T01:00:00Z\n",
    ]
    (primary_dir / "account_balance_usdt.csv").write_text(
        "".join(balance_rows), encoding="utf-8"
    )

    # 4 garbage event files
    (primary_dir / "live_strategy_signals.csv").write_text(
        "symbol,direction,timestamp\ngarbage,BUY,not_a_date\n", encoding="utf-8"
    )
    (primary_dir / "account_fill_events.csv").write_text(
        "order_id,symbol,price,qty,fill_time\n1,trash,NaN,-1,not_a_date\n",
        encoding="utf-8",
    )
    (primary_dir / "exchange_orders.csv").write_text(
        "order_id,symbol,status,side,created_at\n1,dummy,NO_STATUS,NO_SIDE,not_a_date\n",
        encoding="utf-8",
    )
    (primary_dir / "order_intents.csv").write_text(
        "intent_id,symbol,decision,created_at\n1,bad,NO_DECISION,not_a_date\n",
        encoding="utf-8",
    )

    # Write account_primary_events.csv for primary replay trades
    trade_csv = (
        "trade_id,symbol,direction,entry_at,entry_price,exit_at,exit_price,notional_usdt,leverage,net_pnl_usdt\n"
        "1,BTCUSDT,LONG,2026-09-20T00:00:00Z,100.0,2026-09-20T01:00:00Z,101.0,100.0,1.0,1.0\n"
    )
    (tmp_path / "account_primary_events.csv").write_text(trade_csv, encoding="utf-8")

    payload = build_reconciliation_payload(
        data_dir=tmp_path,
        prices_by_symbol={"BTCUSDT": ([1789800000, 1789803600], [100.0, 101.0])},
        live_early_dir=tmp_path,
        live_latest_dir=tmp_path,
    )

    acc = payload["accounts"]["primary"]
    status = acc["status_label"]

    # Must NOT be PASS!
    assert "PASS" not in status
    assert "INSUFFICIENT" in status or "FAIL" in status


def test_counterexample_recon_passed_gate_logic():
    """P1 #4: Test gate check logic rejects DIVERGED, FAIL, and INSUFFICIENT."""
    # Test cases:
    cases = [
        (
            {"primary": {"status_label": "❌ 宇宙失配对账失败 (FAIL · 标的池不重合)"}},
            False,
        ),
        (
            {"primary": {"status_label": "⚠️ 人工干预漂移 (DIVERGED · 2笔手动平仓)"}},
            False,
        ),
        (
            {"primary": {"status_label": "⚠️ 实盘事件流缺失 (INSUFFICIENT_EVIDENCE)"}},
            False,
        ),
        ({"primary": {"status_label": "⚠️ 实盘数据缺失 (INSUFFICIENT_DATA)"}}, False),
        (
            {"primary": {"status_label": "❌ 执行证据失配 (FAIL · 关键对账指标不足)"}},
            False,
        ),
        ({"primary": {"status_label": "✅ 因果保真放行 (PASS · 紧密跟踪)"}}, True),
        (
            {
                "primary": {"status_label": "✅ 因果保真放行 (PASS · 紧密跟踪)"},
                "acc02": {"status_label": "⚠️ 人工干预漂移 (DIVERGED · 2笔手动平仓)"},
            },
            False,
        ),
        (
            {
                "primary": {"status_label": "✅ 因果保真放行 (PASS · 紧密跟踪)"},
                "acc02": {"status_label": "✅ 因果保真放行 (PASS · 紧密跟踪)"},
            },
            True,
        ),
    ]

    for accs, expected in cases:
        recon_passed = (
            all(
                "PASS" in str(a.get("status_label", ""))
                and "DIVERGED" not in str(a.get("status_label", ""))
                and "FAIL" not in str(a.get("status_label", ""))
                and "INSUFFICIENT" not in str(a.get("status_label", ""))
                for a in accs.values()
            )
            and len(accs) > 0
        )
        assert recon_passed == expected, (
            f"Failed for {accs}: expected {expected}, got {recon_passed}"
        )


def test_counterexample_compounding_15s_mtm_verification():
    """P1 #1: Compounding candidate with preliminary low realized MDD but true 15s MTM

    drawdown exceeding 15% must be rejected.
    """
    full_params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }
    cand = Candidate8D(
        params=full_params,
        net_pnl=50.0,
        mdd=0.10,
        calmar=5.0,
        compounding_score=0.25,
        compounding_mdd=0.08,  # preliminary says 8%
        compounding_ui=0.02,
        terminal_compounded_equity=1050.0,
        n_trades=2,
        peak_margin=100.0,
    )

    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC).timestamp()

    # Events admitting 2 concurrent trades
    events = [
        {
            "trade_id": 1,
            "symbol": "BTCUSDT",
            "entry_at": "2026-09-20T00:00:00Z",
            "exit_at": "2026-09-20T04:00:00Z",
            "detected_at": "2026-09-20T00:00:00Z",
            "entry_epoch": t0,
            "exit_epoch": t0 + 14400,
            "detected_epoch": t0,
            "entry_price": 100.0,
            "exit_price": 105.0,
            "notional_usdt": 100.0,
            "leverage": 5.0,
            "direction": "LONG",
            "net_pnl_usdt": 5.0,
            "fee_rate": 0.0,
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.8,
            "min_imbalance": 0.5,
            "confirmation_min": 0.5,
            "min_intensity": 2.0,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
        },
        {
            "trade_id": 2,
            "symbol": "ETHUSDT",
            "entry_at": "2026-09-20T00:00:00Z",
            "exit_at": "2026-09-20T04:00:00Z",
            "detected_at": "2026-09-20T00:00:00Z",
            "entry_epoch": t0,
            "exit_epoch": t0 + 14400,
            "detected_epoch": t0,
            "entry_price": 100.0,
            "exit_price": 105.0,
            "notional_usdt": 100.0,
            "leverage": 5.0,
            "direction": "LONG",
            "net_pnl_usdt": 5.0,
            "fee_rate": 0.0,
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.8,
            "min_imbalance": 0.5,
            "confirmation_min": 0.5,
            "min_intensity": 2.0,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
        },
    ]

    # Price plunges intraday to 10.0 (90% drawdown on 2x100U = 180U loss = 18% MDD)
    times = [t0, t0 + 3600, t0 + 7200, t0 + 14400]
    prices = [100.0, 10.0, 20.0, 105.0]
    prices_by_symbol = {
        "BTCUSDT": (times, prices),
        "ETHUSDT": (times, prices),
    }

    selected = select_compounding(
        [cand],
        max_mdd=0.15,
        events=events,
        prices_by_symbol=prices_by_symbol,
    )

    # Must be None because true 15s MTM drawdown exceeded 15%!
    assert selected is None


def test_counterexample_ambiguous_account_tokens_in_reconciliation():
    """P2 #2: When account identity is missing in both record and caller,

    distinct ambiguous tokens prevent accidental false matching across records.
    """
    from local_optimization.reconciliation import reconcile_signals_and_fills

    # Two signals with no account_id and no config_id
    sig1 = {"symbol": "BTCUSDT", "direction": "LONG", "timestamp_epoch": 1000.0}
    sig2 = {"symbol": "BTCUSDT", "direction": "LONG", "timestamp_epoch": 1000.0}

    report = reconcile_signals_and_fills(
        live_signals=[sig1],
        replay_signals=[sig2],
        live_fills=[],
        replay_fills=[],
        account_id="",  # No account identity provided
    )

    sig_summary = report.layers.get("signals")
    assert sig_summary is not None
    # Lacking account identity, they cannot falsely match across accounts
    assert sig_summary.matched_total == 0
