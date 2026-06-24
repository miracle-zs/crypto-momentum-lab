import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.persistence.parquet import write_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    ReplayConfig,
    ReplayError,
    ReplayExecutionConfig,
    SimulatedFillStatus,
    build_strategy_replay_report,
    run_strategy_replay,
    write_strategy_replay_report,
)

_DEFAULT_EXECUTION = ReplayExecutionConfig()


def test_run_strategy_replay_emits_report_from_in_memory_states() -> None:
    report = run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=ReplayConfig(
            strategy_name="compression_breakout",
            run_id="run-1",
            code_commit="unknown",
            generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
            compression_breakout=CompressionBreakoutConfig(
                compression_window_buckets=3,
                max_range_width_pct=Decimal("0.01"),
                min_breakout_pct=Decimal("0.001"),
                acceptance_buckets=2,
                cooldown_buckets=3,
                forward_horizon_buckets=(1,),
            ),
            candidate_notional=Decimal("100"),
            candidate_ttl_buckets=2,
        ),
    )

    assert report.schema_version == 2
    assert report.run.strategy_name == "compression_breakout"
    assert report.input_state_count == len(_breakout_states())
    assert report.processed_symbol_count == 1
    assert len(report.signals) == 1
    assert len(report.candidates) == 1
    assert report.signals[0].side is StrategySide.LONG
    assert report.candidates[0].signal_id == report.signals[0].signal_id
    assert len(report.simulated_fills) == 1
    assert report.simulated_fills[0].status is SimulatedFillStatus.EXPIRED
    assert report.summary_counts["signals_by_side"] == {"long": 1}
    assert report.summary_counts["signals_by_symbol"] == {"BTCUSDT": 1}
    assert report.fill_summary["fills_by_status"] == {"expired": 1}


def test_run_strategy_replay_simulates_latency_fee_and_spread_fill() -> None:
    report = run_strategy_replay(
        states=_breakout_states() + (_state(5, close=Decimal("101.4")),),
        source_paths=("memory",),
        config=_replay_config(
            execution=ReplayExecutionConfig(
                latency_buckets=1,
                taker_fee_rate=Decimal("0.0004"),
                slippage_bps=Decimal("1"),
            )
        ),
    )

    assert len(report.simulated_fills) == 1
    fill = report.simulated_fills[0]
    assert fill.status is SimulatedFillStatus.FILLED
    assert fill.candidate_id == report.candidates[0].candidate_id
    assert fill.signal_id == report.signals[0].signal_id
    assert fill.symbol == "BTCUSDT"
    assert fill.side is StrategySide.LONG
    assert fill.target_fill_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.filled_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.reference_midpoint == Decimal("101.4")
    assert fill.spread == Decimal("0.02")
    assert fill.fill_price == Decimal("101.420141")
    assert fill.requested_notional == Decimal("100")
    assert fill.filled_notional == Decimal("100")
    assert fill.fee == Decimal("0.0400")
    assert fill.total_cost > fill.fee
    assert fill.cost_bps > Decimal("4")
    assert report.fill_summary["fills_by_status"] == {"filled": 1}
    assert report.fill_summary["filled_notional_by_symbol"] == {"BTCUSDT": 100}


def test_run_strategy_replay_records_missing_simulated_fill_price() -> None:
    missing_price_state = _unsafe_replace_state(
        _state(5, close=Decimal("101.4")),
        close_price=None,
        last_bid_price=None,
        last_ask_price=None,
        spread=None,
        midpoint=None,
        mark_price=None,
    )

    report = run_strategy_replay(
        states=_breakout_states() + (missing_price_state,),
        source_paths=("memory",),
        config=_replay_config(),
    )

    assert len(report.simulated_fills) == 1
    fill = report.simulated_fills[0]
    assert fill.status is SimulatedFillStatus.REJECTED
    assert fill.reason == "missing_fill_price"
    assert fill.fill_price is None
    assert report.fill_summary["fills_by_status"] == {"rejected": 1}


def test_run_strategy_replay_can_disable_simulated_fills() -> None:
    report = run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=_replay_config(execution=None),
    )

    assert report.execution_config is None
    assert report.simulated_fills == ()
    assert report.fill_summary["fills_by_status"] == {}


def test_build_strategy_replay_report_reads_parquet_states(tmp_path: Path) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw")
    derived_root = tmp_path / "derived"
    write_market_states_15s_dataset(
        root=derived_root,
        states=_breakout_states(),
        input_paths=(input_path,),
    )

    report = build_strategy_replay_report(
        state_paths=(derived_root / "market_states_15s",),
        config=ReplayConfig(
            strategy_name="compression_breakout",
            run_id="run-parquet",
            code_commit="unknown",
            generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
            compression_breakout=CompressionBreakoutConfig(
                compression_window_buckets=3,
                max_range_width_pct=Decimal("0.01"),
                min_breakout_pct=Decimal("0.001"),
                acceptance_buckets=2,
                cooldown_buckets=3,
                forward_horizon_buckets=(1,),
            ),
            candidate_notional=Decimal("100"),
            candidate_ttl_buckets=2,
        ),
    )

    assert report.input_state_count == len(_breakout_states())
    assert len(report.signals) == 1
    assert report.source_paths == ((derived_root / "market_states_15s").as_posix(),)


def test_write_strategy_replay_report_serializes_decimal_and_timestamps(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "report.json"
    report = run_strategy_replay(
        states=_breakout_states(),
        source_paths=("memory",),
        config=_replay_config(),
    )

    write_strategy_replay_report(report, output_path)

    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == 2
    assert payload["run"]["created_at"] == "2026-06-22T00:00:00+00:00"
    assert payload["execution_config"]["latency_buckets"] == 1
    assert payload["signals"][0]["features"]["breakout_price"] == "101.2"
    assert payload["candidates"][0]["desired_notional"] == "100"
    assert payload["simulated_fills"][0]["status"] == "expired"
    assert payload["final_checkpoint"]["payload"]["buffer_sizes"] == {"BTCUSDT": 5}


def test_replay_rejects_unknown_strategy() -> None:
    with pytest.raises(ReplayError, match="unsupported strategy"):
        run_strategy_replay(
            states=_breakout_states(),
            source_paths=("memory",),
            config=ReplayConfig(
                strategy_name="order_flow_impulse",
                run_id="run-1",
                code_commit="unknown",
                generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
                compression_breakout=_replay_config().compression_breakout,
                candidate_notional=Decimal("100"),
                candidate_ttl_buckets=2,
            ),
        )


def test_replay_rejects_empty_input() -> None:
    with pytest.raises(ReplayError, match="no market states"):
        run_strategy_replay(
            states=(),
            source_paths=("memory",),
            config=_replay_config(),
        )


def test_replay_rejects_naive_state_timestamp() -> None:
    state = _state(0, close=Decimal("100"))
    naive = _unsafe_replace_state(
        state,
        bucket_start=datetime(2026, 6, 22, 0, 0),
    )

    with pytest.raises(ReplayError, match="bucket_start must be timezone-aware"):
        run_strategy_replay(
            states=(naive,),
            source_paths=("memory",),
            config=_replay_config(),
        )


def _replay_config(
    *,
    execution: ReplayExecutionConfig | None = _DEFAULT_EXECUTION,
) -> ReplayConfig:
    return ReplayConfig(
        strategy_name="compression_breakout",
        run_id="run-1",
        code_commit="unknown",
        generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=3,
            max_range_width_pct=Decimal("0.01"),
            min_breakout_pct=Decimal("0.001"),
            acceptance_buckets=2,
            cooldown_buckets=3,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
        execution=execution,
    )


def _breakout_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )


def _state(
    bucket_index: int,
    *,
    close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, tzinfo=UTC) + timedelta(
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
        open_price=close,
        high_price=high if high is not None else close,
        low_price=low if low is not None else close,
        close_price=close,
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=close - Decimal("0.01"),
        last_ask_price=close + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )


def _unsafe_replace_state(
    state: MarketState15s,
    **changes: object,
) -> MarketState15s:
    replacement = object.__new__(MarketState15s)
    for field in fields(MarketState15s):
        object.__setattr__(
            replacement,
            field.name,
            changes.get(field.name, getattr(state, field.name)),
        )
    return replacement
