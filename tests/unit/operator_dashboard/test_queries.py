from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.operator_dashboard.queries import (
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
