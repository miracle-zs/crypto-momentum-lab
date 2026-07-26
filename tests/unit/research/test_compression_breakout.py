import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.research.compression_breakout import (
    run_compression_breakout_event_study,
    write_compression_breakout_report,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)


def test_run_compression_breakout_event_study_builds_report() -> None:
    report = run_compression_breakout_event_study(
        states=_states(),
        config=_config(),
        source_paths=(Path("data/derived/market_states_15s"),),
        signal_interval_seconds=15,
    )

    assert report.schema_version == 1
    assert report.config == _config()
    assert report.source_paths == ("data/derived/market_states_15s",)
    assert report.summary.total_count == 1
    assert report.events[0].symbol == "BTCUSDT"


def test_write_compression_breakout_report_serializes_json(
    tmp_path: Path,
) -> None:
    report = run_compression_breakout_event_study(
        states=_states(),
        config=_config(),
        source_paths=(Path("states.parquet"),),
        signal_interval_seconds=15,
    )
    output_path = tmp_path / "reports" / "compression-breakout.json"

    write_compression_breakout_report(report, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["config"]["compression_window_buckets"] == 4
    assert payload["signal_interval_seconds"] == 15
    assert payload["source_paths"] == ["states.parquet"]
    assert payload["summary"]["total_count"] == 1
    assert payload["events"][0]["direction"] == "up"
    assert payload["events"][0]["forward_returns"]["1"] == (
        "0.004975124378109452736318407960"
    )


def _config() -> CompressionBreakoutConfig:
    return CompressionBreakoutConfig(
        compression_window_buckets=4,
        max_range_width_pct=Decimal("0.01"),
        min_breakout_pct=Decimal("0.001"),
        acceptance_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1, 2),
    )


def _states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.10")),
        _state(2, Decimal("99.95")),
        _state(3, Decimal("100.20")),
        _state(4, Decimal("100.50")),
        _state(5, Decimal("101.00")),
        _state(6, Decimal("100.25")),
    )


def _state(bucket_index: int, close_price: Decimal) -> MarketState15s:
    bucket_start = datetime(2026, 6, 19, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        trade_count=3,
        trade_notional=Decimal("300"),
        aggressive_buy_notional=Decimal("200"),
        aggressive_sell_notional=Decimal("100"),
        last_bid_price=close_price - Decimal("0.01"),
        last_ask_price=close_price + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=3,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
