"""Unit tests for snapshot manifest and 6-layer reconciliation."""

from __future__ import annotations

import gzip
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from local_optimization.reconciliation import (
    reconcile_signals_and_fills,
)
from local_optimization.run_live_reconciliation import (
    ACCOUNT_REGISTRY,
    format_markdown_report,
    format_multi_account_markdown_report,
    run_reconciliation,
)
from local_optimization.snapshot import (
    inspect_snapshot_dir,
)


def test_reconciliation_exact_match() -> None:
    """When live and replay events match, audit passes with 100% precision/recall."""
    t0 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t0_epoch = t0.timestamp()

    signals = [
        {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "timestamp": t0.isoformat(),
            "timestamp_epoch": t0_epoch,
        }
    ]
    fills = [
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "price": 60000.0,
            "timestamp": t0.isoformat(),
            "time_epoch": t0_epoch + 1.0,
        }
    ]

    report = reconcile_signals_and_fills(
        live_signals=signals,
        replay_signals=signals,
        live_fills=fills,
        replay_fills=fills,
        account_id="test_acc",
    )

    assert report.is_audit_passed is True
    assert report.first_divergence is None
    assert report.layers["signals"].precision == 1.0
    assert report.layers["signals"].recall == 1.0
    assert report.layers["fills"].precision == 1.0
    assert report.layers["fills"].recall == 1.0


def test_reconciliation_first_divergence_detection() -> None:
    """Verify that the first chronological divergence is isolated as the root cause."""
    t1 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 14, 10, 30, tzinfo=UTC)

    live_signals = [
        {
            "symbol": "ETHUSDT",
            "direction": "LONG",
            "timestamp": t1.isoformat(),
            "timestamp_epoch": t1.timestamp(),
        }
    ]
    replay_signals = [
        {
            "symbol": "ETHUSDT",
            "direction": "LONG",
            "timestamp": t1.isoformat(),
            "timestamp_epoch": t1.timestamp(),
        }
    ]

    # Live had a fill, but replay missed the fill at t1 + 2s (root cause)
    live_fills = [
        {
            "symbol": "ETHUSDT",
            "side": "BUY",
            "price": 2400.0,
            "timestamp": t1.isoformat(),
            "time_epoch": t1.timestamp() + 2.0,
        },
        {
            "symbol": "SOLUSDT",
            "side": "BUY",
            "price": 140.0,
            "timestamp": t2.isoformat(),
            "time_epoch": t2.timestamp(),
        },
    ]
    replay_fills = []  # Replay missed both

    report = reconcile_signals_and_fills(
        live_signals=live_signals,
        replay_signals=replay_signals,
        live_fills=live_fills,
        replay_fills=replay_fills,
    )

    assert report.is_audit_passed is False
    assert report.first_divergence is not None
    assert report.first_divergence.symbol == "ETHUSDT"
    assert report.first_divergence.layer == "fills"
    assert report.first_divergence.is_root_cause is True


def test_snapshot_inspection() -> None:
    """Test inspecting a directory creates a complete SnapshotManifest."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        acc_dir = tmp_path / "primary"
        acc_dir.mkdir()

        # Create dummy stream files with valid column names
        (acc_dir / "account_balance_usdt.csv").write_text(
            "timestamp,balance\n2026-09-14T00:00:00Z,1000.0\n", encoding="utf-8"
        )
        (acc_dir / "account_fill_events.csv").write_text(
            "trade_at,symbol,price,qty,order_id\n2026-09-14T00:00:00Z,BTCUSDT,50000,1.0,ord1\n",
            encoding="utf-8",
        )
        (acc_dir / "exchange_orders.csv").write_text(
            "created_at,symbol,order_id,status,side\n2026-09-14T00:00:00Z,BTCUSDT,ord1,FILLED,BUY\n",
            encoding="utf-8",
        )
        (acc_dir / "live_strategy_signals.csv").write_text(
            "timestamp,symbol,direction\n2026-09-14T00:00:00Z,BTCUSDT,LONG\n",
            encoding="utf-8",
        )
        (acc_dir / "order_intents.csv").write_text(
            "timestamp,symbol,intent_id\n2026-09-14T00:00:00Z,BTCUSDT,int1\n",
            encoding="utf-8",
        )

        manifest = inspect_snapshot_dir(
            snapshot_dir=tmp_path,
            target_cutoff=datetime(2026, 9, 14, 0, 0, tzinfo=UTC),
        )

        assert manifest.is_complete is True
        assert "execution_audit_ready" in manifest.capability_tags
        assert "account_balance_usdt" in manifest.streams
        assert len(manifest.missing_streams) == 0


def test_run_reconciliation_e2e_mock() -> None:
    """Test full 6-layer run_reconciliation flow on mock data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        live_dir = tmp_path / "live"
        live_dir.mkdir()

        # Write gzipped live signals
        with gzip.open(
            live_dir / "live_strategy_signals.csv.gz", "wt", encoding="utf-8"
        ) as f:
            f.write(
                "signal_id,symbol,side,detected_at,strategy_name\n"
                "s1,BTCUSDT,BUY,2026-09-05T10:00:00Z,top10\n"
            )

        # Write gzipped intents
        with gzip.open(live_dir / "order_intents.csv.gz", "wt", encoding="utf-8") as f:
            f.write(
                "intent_id,candidate_id,symbol,state,approved_at\n"
                "i1,c1,BTCUSDT,APPROVED,2026-09-05T10:00:01Z\n"
            )

        # Write gzipped exchange orders
        with gzip.open(
            live_dir / "exchange_orders.csv.gz", "wt", encoding="utf-8"
        ) as f:
            f.write("exchange_order_id,symbol,status\no1,BTCUSDT,FILLED\n")

        # Write gzipped fills
        with gzip.open(
            live_dir / "account_fill_events.csv.gz", "wt", encoding="utf-8"
        ) as f:
            f.write(
                "trade_id,order_id,symbol,side,price,quantity,realized_pnl,fee,trade_at\n"
                "t1,o1,BTCUSDT,BUY,50000.0,0.1,0.0,0.05,2026-09-05T10:00:02Z\n"
                "t2,o2,BTCUSDT,SELL,51000.0,0.1,100.0,0.05,2026-09-05T12:00:00Z\n"
            )

        # Write baseline events CSV
        baseline_csv = tmp_path / "baseline_events.csv"
        baseline_csv.write_text(
            "symbol,direction,detected_at,fill_reason,entry_at,entry_price,"
            "exit_at,exit_price,closed,net_pnl_usdt,net_return_pct\n"
            "BTCUSDT,up,2026-09-05T10:00:00Z,filled,2026-09-05T10:00:02Z,50000.0,"
            "2026-09-05T12:00:00Z,51000.0,True,99.9,1.99\n",
            encoding="utf-8",
        )

        res = run_reconciliation(
            live_dir=live_dir,
            baseline_csv=baseline_csv,
            start_dt=datetime(2026, 9, 5, 0, 0, tzinfo=UTC),
            end_dt=datetime(2026, 9, 6, 0, 0, tzinfo=UTC),
        )

        assert res["layers"]["L1"]["common_symbols_count"] == 1
        assert res["layers"]["L2"]["matched_signals"] == 1
        assert res["layers"]["L3"]["live_intents_count"] == 1
        assert res["layers"]["L4"]["entry_fill_match_rate"] == 1.0
        assert res["layers"]["L5"]["live_exit_fills"] == 1
        assert res["layers"]["L6"]["live_net_pnl"] == 99.9

        md = format_markdown_report(res)
        assert "6-Layer Audit Hierarchy" in md
        assert "未发现因果分歧点" in md


def test_pair_round_trip_and_match_per_symbol_trades():
    from local_optimization.reconciliation import (
        match_per_symbol_trades,
        pair_round_trip_trades,
    )

    fills = [
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "price": 50000.0,
            "quantity": 0.1,
            "fee": 0.05,
            "trade_at": "2026-09-18T10:00:00Z",
        },
        {
            "symbol": "BTCUSDT",
            "side": "SELL",
            "price": 51000.0,
            "quantity": 0.1,
            "fee": 0.051,
            "realized_pnl": 100.0,
            "trade_at": "2026-09-18T10:30:00Z",
        },
        {
            "symbol": "ETHUSDT",
            "side": "BUY",
            "price": 3000.0,
            "quantity": 1.0,
            "fee": 0.03,
            "trade_at": "2026-09-18T11:00:00Z",
        },
    ]

    trades = pair_round_trip_trades(fills)
    assert len(trades) == 1
    tr = trades[0]
    assert tr["symbol"] == "BTCUSDT"
    assert tr["entry_price"] == 50000.0
    assert tr["exit_price"] == 51000.0
    assert tr["net_pnl"] == round(100.0 - 0.05 - 0.051, 4)

    replay_trades = [
        {
            "symbol": "BTCUSDT",
            "entry_at": "2026-09-18T10:00:05Z",
            "entry_price": 49990.0,
            "exit_at": "2026-09-18T10:30:00Z",
            "exit_price": 51000.0,
            "net_pnl_usdt": 100.2,
            "exit_reason": "candle_bearish",
        },
        {
            "symbol": "SOLUSDT",
            "entry_at": "2026-09-18T12:00:00Z",
            "entry_price": 150.0,
            "exit_at": "2026-09-18T13:00:00Z",
            "exit_price": 155.0,
            "net_pnl_usdt": 5.0,
            "exit_reason": "target_profit",
        },
    ]

    matched = match_per_symbol_trades(trades, replay_trades)
    assert matched["summary"]["total_live_trades"] == 1
    assert matched["summary"]["total_replay_trades"] == 2
    assert matched["summary"]["matched_count"] == 1
    assert matched["summary"]["replay_only_count"] == 1
    assert matched["summary"]["live_only_count"] == 0

    btc_rec = [r for r in matched["records"] if r["symbol"] == "BTCUSDT"][0]
    assert btc_rec["status"] == "MATCHED"
    assert btc_rec["entry_slippage_bps"] == round(
        (50000.0 - 49990.0) / 49990.0 * 10000.0, 1
    )


def test_format_multi_account_markdown_report() -> None:
    """Verify combined 4-account comparative audit markdown rendering."""
    assert "primary" in ACCOUNT_REGISTRY
    assert "acc01" in ACCOUNT_REGISTRY
    assert "acc02" in ACCOUNT_REGISTRY
    assert "acc03" in ACCOUNT_REGISTRY

    mock_acc_res = {
        "account_id": "primary",
        "start_time": "2026-09-04T00:00:00Z",
        "end_time": "2026-09-22T00:00:00Z",
        "layers": {
            "L1": {
                "live_symbols_count": 10,
                "replay_symbols_count": 10,
                "universe_jaccard": 1.0,
            },
            "L2": {
                "live_signals_count": 50,
                "replay_signals_count": 50,
                "matched_signals": 50,
                "signal_precision": 1.0,
                "signal_recall": 1.0,
                "live_intents_count": 50,
            },
            "L3": {
                "live_intents_count": 50,
                "total_exchange_orders": 50,
                "fills_with_valid_order": 50,
                "order_coverage_pct": 1.0,
            },
            "L4": {
                "live_buy_fills": 50,
                "replay_trades": 50,
                "entry_fill_match_rate": 1.0,
                "mean_slippage_bps": -2.5,
            },
            "L5": {
                "live_exit_fills": 50,
                "replay_closed_trades": 50,
                "exit_count_ratio": 1.0,
            },
            "L6": {
                "live_net_pnl": 100.0,
                "replay_net_pnl": 105.0,
                "equity_divergence_usdt": -5.0,
                "live_total_fees": 10.0,
            },
        },
        "first_divergence": None,
        "divergences": [],
    }

    results = {"primary": mock_acc_res}
    start_dt = datetime(2026, 9, 4, tzinfo=UTC)
    end_dt = datetime(2026, 9, 22, tzinfo=UTC)

    md = format_multi_account_markdown_report(results, start_dt, end_dt)
    assert "# 实盘 4 账户因果对账多维度对比研报" in md
    assert "primary" in md
    assert "Profile 1" in md
    assert "-2.50 bps" in md
    assert "✅ 无显著分歧" in md


def test_f07_financial_precision_and_invalid_values() -> None:
    """F07: Bad numbers raise ValueError instead of converting to 0.0; Decimal avoids drift."""
    import pytest
    from decimal import Decimal
    from local_optimization.run_live_reconciliation import to_decimal, to_float

    # 1. Invalid string must raise ValueError, not return 0.0
    with pytest.raises(ValueError, match="Invalid decimal value 'not-a-number'"):
        to_decimal("not-a-number", field_name="realized_pnl")

    with pytest.raises(ValueError, match="Invalid numeric value 'not-a-number'"):
        to_float("not-a-number", field_name="net_pnl")

    # 2. Decimal summation avoids float representation drift
    val1 = to_decimal("0.1", field_name="fee")
    val2 = to_decimal("0.2", field_name="fee")
    assert val1 + val2 == Decimal("0.3")
    assert str(val1 + val2) == "0.3"


def test_f08_short_round_trip_matching_and_direction() -> None:
    """F08: SHORT round trip (SELL open -> BUY close) pairs correctly with accurate net_pnl."""
    from local_optimization.reconciliation import pair_round_trip_trades

    # Two SHORT fills: Sell 1 @ 110, then Buy 1 @ 100 with realized_pnl=10, fee=0.1 on each leg
    fills = [
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "price": "110.0",
            "quantity": "1.0",
            "realized_pnl": "0.0",
            "fee": "0.1",
            "trade_at": "2026-09-18T10:00:00Z",
        },
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "price": "100.0",
            "quantity": "1.0",
            "realized_pnl": "10.0",
            "fee": "0.1",
            "trade_at": "2026-09-18T10:30:00Z",
        },
    ]

    trades = pair_round_trip_trades(fills)
    assert len(trades) == 1
    tr = trades[0]
    assert tr["account_id"] == "primary"
    assert tr["symbol"] == "BTCUSDT"
    assert tr["side"] == "SELL"
    assert tr["entry_price"] == 110.0
    assert tr["exit_price"] == 100.0
    assert tr["quantity"] == 1.0
    assert tr["realized_pnl"] == 10.0
    assert tr["fees"] == 0.2
    # 10.0 - 0.2 = 9.8 net pnl
    assert tr["net_pnl"] == 9.8
    assert tr["is_carry_in"] is False
    assert tr["entry_time"] != "N/A (Carry-In)"


def test_f08_hedge_mode_concurrent_long_and_short() -> None:
    """F08: Explicit position_side isolates concurrent LONG and SHORT positions on same symbol."""
    from local_optimization.reconciliation import pair_round_trip_trades

    fills = [
        # Long entry: BUY 1 @ 100
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "LONG",
            "side": "BUY",
            "price": 100.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 0.0,
            "trade_at": "2026-09-18T10:00:00Z",
        },
        # Short entry: SELL 1 @ 110
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "SHORT",
            "side": "SELL",
            "price": 110.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 0.0,
            "trade_at": "2026-09-18T10:05:00Z",
        },
        # Long exit: SELL 1 @ 105
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "LONG",
            "side": "SELL",
            "price": 105.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 5.0,
            "trade_at": "2026-09-18T10:30:00Z",
        },
        # Short exit: BUY 1 @ 102
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "SHORT",
            "side": "BUY",
            "price": 102.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 8.0,
            "trade_at": "2026-09-18T10:35:00Z",
        },
    ]

    trades = pair_round_trip_trades(fills)
    assert len(trades) == 2

    long_trade = [t for t in trades if t["side"] == "BUY"][0]
    assert long_trade["entry_price"] == 100.0
    assert long_trade["exit_price"] == 105.0
    assert long_trade["net_pnl"] == 4.8  # 5.0 - 0.2 fee
    assert long_trade["is_carry_in"] is False

    short_trade = [t for t in trades if t["side"] == "SELL"][0]
    assert short_trade["entry_price"] == 110.0
    assert short_trade["exit_price"] == 102.0
    assert short_trade["net_pnl"] == 7.8  # 8.0 - 0.2 fee
    assert short_trade["is_carry_in"] is False


def test_to_decimal_rejects_invalid_strings() -> None:
    from decimal import Decimal
    import pytest
    from local_optimization.reconciliation import to_decimal

    # Valid values
    assert to_decimal(10) == Decimal("10")
    assert to_decimal("123.45") == Decimal("123.45")
    assert to_decimal(None) == Decimal("0")
    assert to_decimal("") == Decimal("0")
    assert to_decimal(None, default=Decimal("5")) == Decimal("5")

    # Invalid strings must raise ValueError
    with pytest.raises(ValueError, match="Invalid decimal value"):
        to_decimal("not-a-number")

    with pytest.raises(ValueError, match="Invalid decimal value"):
        to_decimal("abc")


def test_interleaved_short_exit_does_not_pair_with_long_entry() -> None:
    """Interleaved SHORT exit (BUY) must NOT match a pending LONG entry (BUY)."""
    from local_optimization.reconciliation import pair_round_trip_trades

    fills = [
        # Long entry: BUY 1 @ 100
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "LONG",
            "side": "BUY",
            "price": 100.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 0.0,
            "trade_at": "2026-09-18T10:00:00Z",
        },
        # Short exit (closing pre-existing short): BUY 1 @ 95
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "SHORT",
            "side": "BUY",
            "price": 95.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 5.0,
            "trade_at": "2026-09-18T10:15:00Z",
        },
        # Long exit: SELL 1 @ 105
        {
            "account_label": "primary",
            "symbol": "BTCUSDT",
            "position_side": "LONG",
            "side": "SELL",
            "price": 105.0,
            "quantity": 1.0,
            "fee": 0.1,
            "realized_pnl": 5.0,
            "trade_at": "2026-09-18T10:30:00Z",
        },
    ]

    trades = pair_round_trip_trades(fills)
    assert len(trades) == 2

    # Long trade: BUY @ 100, SELL @ 105
    long_trades = [t for t in trades if t["position_side"] == "LONG" or t["side"] == "BUY" and not t["is_carry_in"]]
    assert len(long_trades) == 1
    assert long_trades[0]["entry_price"] == 100.0
    assert long_trades[0]["exit_price"] == 105.0
    assert long_trades[0]["side"] == "BUY"

    # Short trade: carry-in short exit BUY @ 95 -> trade side is SELL!
    short_trades = [t for t in trades if t["position_side"] == "SHORT"]
    assert len(short_trades) == 1
    assert short_trades[0]["side"] == "SELL"
    assert short_trades[0]["exit_price"] == 95.0
    assert short_trades[0]["is_carry_in"] is True


