from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from crypto_momentum_lab.operator_dashboard.queries import (
    _aggregate_account_fills,
    _downsample_equity_snapshots,
    _paper_exit_label,
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
    assert (
        Decimal(str(aggregated[0]["price"])).quantize(Decimal("0.00001"))
        == Decimal("0.03158")
    )


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
