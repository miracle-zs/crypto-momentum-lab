from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from crypto_momentum_lab.operator_dashboard.queries import (
    _aggregate_account_fills,
    _downsample_equity_snapshots,
    _is_dashboard_paper_run,
    _live_account_equity_point,
    _paper_exit_label,
    _split_exchange_orders,
)
from crypto_momentum_lab.persistence.postgres.models import PaperEquitySnapshotRow


def test_downsample_equity_snapshots_keeps_latest_row_in_each_utc_bucket() -> None:
    start = datetime(2026, 7, 28, tzinfo=UTC)
    rows = [
        _snapshot("first", start + timedelta(seconds=10), "1000"),
        _snapshot("latest-in-bucket", start + timedelta(minutes=5, seconds=59), "1001"),
        _snapshot("next-bucket", start + timedelta(minutes=6), "1002"),
    ]

    sampled = _downsample_equity_snapshots(rows)

    assert [row.snapshot_id for row in sampled] == [
        "latest-in-bucket",
        "next-bucket",
    ]


def test_downsample_equity_snapshots_caps_result_to_latest_240_buckets() -> None:
    start = datetime(2026, 7, 27, tzinfo=UTC)
    rows = [
        _snapshot(str(index), start + timedelta(minutes=6 * index), str(1000 + index))
        for index in range(242)
    ]

    sampled = _downsample_equity_snapshots(rows)

    assert len(sampled) == 240
    assert sampled[0].snapshot_id == "2"
    assert sampled[-1].snapshot_id == "241"


def test_dashboard_uses_latest_checkpoint_without_append_only_events() -> None:
    source = Path(
        "src/crypto_momentum_lab/operator_dashboard/queries.py"
    ).read_text(encoding="utf-8")

    assert "StrategyRuntimeCheckpointRow" in source
    assert "StrategyRuntimeEventRow" not in source


def test_dashboard_separates_confirmed_open_orders_from_uncertain_orders() -> None:
    rows = [
        SimpleNamespace(state="acknowledged"),
        SimpleNamespace(state="partially_filled"),
        SimpleNamespace(state="submitting"),
        SimpleNamespace(state="mystery"),
        SimpleNamespace(state="filled"),
    ]

    pending, ambiguous = _split_exchange_orders(rows)

    assert [row.state for row in pending] == [
        "acknowledged",
        "partially_filled",
    ]
    assert [row.state for row in ambiguous] == ["submitting", "mystery"]


def test_candle_exit_label_includes_entry_filter_variants() -> None:
    portfolio = {
        "exit_mode": "candle_15m",
        "candle_confirmation_count": 1,
        "candle_minimum_holding_buckets": 0,
    }

    assert _paper_exit_label(
        "candle_15m",
        portfolio,
        {"allow_long": True, "allow_short": False},
    ) == "15M 收线退出 · 仅多头"
    assert _paper_exit_label(
        "candle_15m",
        portfolio,
        {
            "allow_long": True,
            "allow_short": False,
            "max_abs_aggressive_imbalance": "0.7113",
        },
    ) == "15M 收线退出 · 仅多头 · 主动不平衡 ≤ 71.13%"


def test_candle_exit_label_includes_grace_recovery_threshold() -> None:
    assert _paper_exit_label(
        "candle_15m",
        {
            "candle_grace_bars": 8,
            "candle_grace_profit_pct": "0.0058",
        },
    ) == "反向后宽限 8 根 15M · 回收 +0.58%"


def test_dashboard_excludes_fixed_exit_paper_accounts() -> None:
    assert not _is_dashboard_paper_run(
        SimpleNamespace(execution_config={"portfolio": {"exit_mode": "fixed"}})
    )
    assert _is_dashboard_paper_run(
        SimpleNamespace(execution_config={"portfolio": {"exit_mode": "candle_15m"}})
    )


def test_live_account_balance_becomes_equity_point() -> None:
    point = _live_account_equity_point(
        SimpleNamespace(
            observed_at=datetime(2026, 8, 16, 2, 0, tzinfo=UTC),
            wallet_balance=Decimal("282.28"),
            unrealized_pnl=Decimal("-5.48"),
        )
    )

    assert point["equity"] == "276.80"
    assert point["balance"] == "282.28"
    assert point["unrealized_pnl"] == "-5.48"


def test_account_fills_are_aggregated_to_one_row_per_order() -> None:
    rows = [
        SimpleNamespace(
            order_id="order-1",
            symbol="TUTUSDT",
            side="SELL",
            fee_asset="BNB",
            price=Decimal("0.03158"),
            quantity=Decimal("736"),
            realized_pnl=Decimal("-1.27"),
            fee=Decimal("0.0116"),
            trade_at=datetime(2026, 8, 16, 1, 40, 13, tzinfo=UTC),
        ),
        SimpleNamespace(
            order_id="order-1",
            symbol="TUTUSDT",
            side="SELL",
            fee_asset="USDT",
            price=Decimal("0.03159"),
            quantity=Decimal("163"),
            realized_pnl=Decimal("-0.28"),
            fee=Decimal("0.0026"),
            trade_at=datetime(2026, 8, 16, 1, 40, 13, tzinfo=UTC),
        ),
    ]

    aggregated = _aggregate_account_fills(
        rows,
        {"order-1": "orderflow_impulse"},
    )

    assert len(aggregated) == 1
    assert aggregated[0]["order_id"] == "order-1"
    assert aggregated[0]["quantity"] == "899"
    assert aggregated[0]["realized_pnl"] == "-1.55"
    assert aggregated[0]["fee"] == "0.0142"
    assert aggregated[0]["fee_asset"] == "BNB / USDT"
    assert aggregated[0]["fill_count"] == 2
    assert aggregated[0]["strategy_name"] == "orderflow_impulse"
    assert aggregated[0]["reduce_only"] is False
    assert aggregated[0]["close_reason"] is None
    assert (
        Decimal(str(aggregated[0]["price"])).quantize(Decimal("0.00001"))
        == Decimal("0.03158")
    )


def test_account_fills_preserve_live_exit_reason_metadata() -> None:
    row = SimpleNamespace(
        order_id="exit-order-1",
        symbol="ONUSDT",
        side="SELL",
        fee_asset="USDT",
        price=Decimal("0.05000"),
        quantity=Decimal("10"),
        realized_pnl=Decimal("1.20"),
        fee=Decimal("0.001"),
        trade_at=datetime(2026, 8, 16, 2, 0, tzinfo=UTC),
    )

    aggregated = _aggregate_account_fills(
        [row],
        {"exit-order-1": "orderflow_impulse"},
        {
            "exit-order-1": {
                "reduce_only": True,
                "close_reason": "candle_15m_grace_timeout_1",
            }
        },
    )

    assert aggregated[0]["reduce_only"] is True
    assert aggregated[0]["close_reason"] == "candle_15m_grace_timeout_1"


def _snapshot(
    snapshot_id: str,
    observed_at: datetime,
    equity: str,
) -> PaperEquitySnapshotRow:
    value = Decimal(equity)
    return PaperEquitySnapshotRow(
        snapshot_id=snapshot_id,
        run_id="paper-account-test",
        observed_at=observed_at,
        balance=value,
        equity=value,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        total_fees=Decimal("0"),
        open_position_count=0,
    )
