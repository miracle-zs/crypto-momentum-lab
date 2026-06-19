import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutEvent,
    CompressionBreakoutSummary,
    find_compression_breakouts,
    summarize_compression_breakouts,
)


@dataclass(frozen=True, slots=True)
class CompressionBreakoutReport:
    schema_version: int
    generated_at: datetime
    config: CompressionBreakoutConfig
    source_paths: tuple[str, ...]
    events: tuple[CompressionBreakoutEvent, ...]
    summary: CompressionBreakoutSummary


def run_compression_breakout_event_study(
    *,
    states: tuple[MarketState15s, ...],
    config: CompressionBreakoutConfig,
    source_paths: tuple[Path, ...],
) -> CompressionBreakoutReport:
    events = find_compression_breakouts(states, config)
    return CompressionBreakoutReport(
        schema_version=1,
        generated_at=datetime.now(UTC),
        config=config,
        source_paths=tuple(path.as_posix() for path in source_paths),
        events=events,
        summary=summarize_compression_breakouts(
            events,
            horizons=config.forward_horizon_buckets,
        ),
    )


def build_compression_breakout_report(
    *,
    state_paths: tuple[Path, ...],
    output_path: Path,
    config: CompressionBreakoutConfig,
) -> CompressionBreakoutReport:
    report = run_compression_breakout_event_study(
        states=read_market_states_15s_dataset(state_paths),
        config=config,
        source_paths=state_paths,
    )
    write_compression_breakout_report(report, output_path)
    return report


def write_compression_breakout_report(
    report: CompressionBreakoutReport,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    temporary_path.write_text(
        json.dumps(
            _report_payload(report),
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_path, output_path)


def _report_payload(report: CompressionBreakoutReport) -> dict[str, object]:
    return {
        "schema_version": report.schema_version,
        "generated_at": report.generated_at.isoformat(),
        "config": _config_payload(report.config),
        "source_paths": list(report.source_paths),
        "events": [_event_payload(event) for event in report.events],
        "summary": _summary_payload(report.summary),
    }


def _config_payload(config: CompressionBreakoutConfig) -> dict[str, object]:
    return {
        "compression_window_buckets": config.compression_window_buckets,
        "max_range_width_pct": str(config.max_range_width_pct),
        "min_breakout_pct": str(config.min_breakout_pct),
        "acceptance_buckets": config.acceptance_buckets,
        "cooldown_buckets": config.cooldown_buckets,
        "forward_horizon_buckets": list(config.forward_horizon_buckets),
    }


def _event_payload(event: CompressionBreakoutEvent) -> dict[str, object]:
    return {
        "symbol": event.symbol,
        "direction": event.direction.value,
        "detected_at": event.detected_at.isoformat(),
        "range_start": event.range_start.isoformat(),
        "range_end": event.range_end.isoformat(),
        "range_high": str(event.range_high),
        "range_low": str(event.range_low),
        "range_midpoint": str(event.range_midpoint),
        "range_width_pct": str(event.range_width_pct),
        "breakout_price": str(event.breakout_price),
        "breakout_distance_pct": str(event.breakout_distance_pct),
        "trade_count": event.trade_count,
        "trade_notional": str(event.trade_notional),
        "aggressive_buy_notional": str(event.aggressive_buy_notional),
        "aggressive_sell_notional": str(event.aggressive_sell_notional),
        "aggressive_imbalance": str(event.aggressive_imbalance),
        "spread": _optional_decimal(event.spread),
        "midpoint": _optional_decimal(event.midpoint),
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
        "forward_returns": {
            str(horizon): _optional_decimal(value)
            for horizon, value in event.forward_returns.items()
        },
        "max_favorable_return": _optional_decimal(event.max_favorable_return),
        "max_adverse_return": _optional_decimal(event.max_adverse_return),
    }


def _summary_payload(summary: CompressionBreakoutSummary) -> dict[str, object]:
    return {
        "total_count": summary.total_count,
        "by_direction": {
            direction.value: {
                "count": direction_summary.count,
                "mean_forward_returns": {
                    str(horizon): _optional_decimal(value)
                    for horizon, value in (
                        direction_summary.mean_forward_returns.items()
                    )
                },
            }
            for direction, direction_summary in summary.by_direction.items()
        },
    }


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)
