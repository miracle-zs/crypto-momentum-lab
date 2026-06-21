import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.research.liquidation_cascade import (
    run_liquidation_cascade_event_study,
    write_liquidation_cascade_report,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
)


def test_run_liquidation_cascade_event_study_builds_report() -> None:
    report = run_liquidation_cascade_event_study(
        states=_states(),
        config=_config(),
        source_paths=(Path("data/derived/market_states_15s"),),
    )

    assert report.schema_version == 1
    assert report.config == _config()
    assert report.source_paths == ("data/derived/market_states_15s",)
    assert report.summary.total_count == 1
    assert report.events[0].symbol == "BTCUSDT"


def test_write_liquidation_cascade_report_serializes_json(tmp_path: Path) -> None:
    report = run_liquidation_cascade_event_study(
        states=_states(),
        config=_config(),
        source_paths=(Path("states.parquet"),),
    )
    output_path = tmp_path / "reports" / "liquidation-cascade.json"

    write_liquidation_cascade_report(report, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["config"]["liquidation_window_buckets"] == 2
    assert payload["source_paths"] == ["states.parquet"]
    assert payload["summary"]["total_count"] == 1
    assert payload["events"][0]["direction"] == "up"
    assert payload["events"][0]["liquidation_notional"] == "600"
    assert payload["events"][0]["forward_returns"]["1"] == (
        "0.009803921568627450980392156863"
    )


def _config() -> LiquidationCascadeConfig:
    return LiquidationCascadeConfig(
        liquidation_window_buckets=2,
        breakout_window_buckets=4,
        min_liquidation_count=1,
        min_liquidation_notional=Decimal("500"),
        min_price_move_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1, 2),
    )


def _states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(4, Decimal("100.00"), liquidation_notional=Decimal("300")),
        _state(5, Decimal("102.00"), liquidation_notional=Decimal("300")),
        _state(6, Decimal("103.00")),
        _state(7, Decimal("101.50")),
    )


def _state(
    bucket_index: int,
    close_price: Decimal,
    *,
    liquidation_notional: Decimal = Decimal("0"),
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 20, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    liquidation_count = 1 if liquidation_notional > 0 else 0
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
        trade_count=10,
        trade_notional=Decimal("300"),
        aggressive_buy_notional=Decimal("250"),
        aggressive_sell_notional=Decimal("50"),
        last_bid_price=close_price - Decimal("0.01"),
        last_ask_price=close_price + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=liquidation_count,
        liquidation_notional=liquidation_notional,
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
