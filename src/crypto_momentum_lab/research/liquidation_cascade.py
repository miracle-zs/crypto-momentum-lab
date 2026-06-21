import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
    LiquidationCascadeEvent,
    LiquidationCascadeSummary,
    find_liquidation_cascades,
    summarize_liquidation_cascades,
)


@dataclass(frozen=True, slots=True)
class LiquidationCascadeReport:
    schema_version: int
    generated_at: datetime
    config: LiquidationCascadeConfig
    source_paths: tuple[str, ...]
    events: tuple[LiquidationCascadeEvent, ...]
    summary: LiquidationCascadeSummary


def run_liquidation_cascade_event_study(
    *,
    states: tuple[MarketState15s, ...],
    config: LiquidationCascadeConfig,
    source_paths: tuple[Path, ...],
) -> LiquidationCascadeReport:
    events = find_liquidation_cascades(states, config)
    return LiquidationCascadeReport(
        schema_version=1,
        generated_at=datetime.now(UTC),
        config=config,
        source_paths=tuple(path.as_posix() for path in source_paths),
        events=events,
        summary=summarize_liquidation_cascades(
            events,
            horizons=config.forward_horizon_buckets,
        ),
    )


def build_liquidation_cascade_report(
    *,
    state_paths: tuple[Path, ...],
    output_path: Path,
    config: LiquidationCascadeConfig,
) -> LiquidationCascadeReport:
    report = run_liquidation_cascade_event_study(
        states=read_market_states_15s_dataset(state_paths),
        config=config,
        source_paths=state_paths,
    )
    write_liquidation_cascade_report(report, output_path)
    return report


def write_liquidation_cascade_report(
    report: LiquidationCascadeReport,
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


def _report_payload(report: LiquidationCascadeReport) -> dict[str, object]:
    return {
        "schema_version": report.schema_version,
        "generated_at": report.generated_at.isoformat(),
        "config": _config_payload(report.config),
        "source_paths": list(report.source_paths),
        "events": [_event_payload(event) for event in report.events],
        "summary": _summary_payload(report.summary),
    }


def _config_payload(config: LiquidationCascadeConfig) -> dict[str, object]:
    return {
        "liquidation_window_buckets": config.liquidation_window_buckets,
        "breakout_window_buckets": config.breakout_window_buckets,
        "min_liquidation_count": config.min_liquidation_count,
        "min_liquidation_notional": str(config.min_liquidation_notional),
        "min_price_move_pct": str(config.min_price_move_pct),
        "min_aggressive_imbalance": str(config.min_aggressive_imbalance),
        "confirmation_buckets": config.confirmation_buckets,
        "cooldown_buckets": config.cooldown_buckets,
        "forward_horizon_buckets": list(config.forward_horizon_buckets),
    }


def _event_payload(event: LiquidationCascadeEvent) -> dict[str, object]:
    return {
        "symbol": event.symbol,
        "direction": event.direction.value,
        "detected_at": event.detected_at.isoformat(),
        "cluster_start": event.cluster_start.isoformat(),
        "cluster_end": event.cluster_end.isoformat(),
        "cluster_start_price": str(event.cluster_start_price),
        "cluster_end_price": str(event.cluster_end_price),
        "cluster_move_pct": str(event.cluster_move_pct),
        "breakout_level": str(event.breakout_level),
        "breakout_distance_pct": str(event.breakout_distance_pct),
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
        "cluster_trade_count": event.cluster_trade_count,
        "cluster_trade_notional": str(event.cluster_trade_notional),
        "aggressive_buy_notional": str(event.aggressive_buy_notional),
        "aggressive_sell_notional": str(event.aggressive_sell_notional),
        "aggressive_imbalance": str(event.aggressive_imbalance),
        "spread": _optional_decimal(event.spread),
        "midpoint": _optional_decimal(event.midpoint),
        "mark_price": _optional_decimal(event.mark_price),
        "forward_returns": {
            str(horizon): _optional_decimal(value)
            for horizon, value in event.forward_returns.items()
        },
        "max_favorable_return": _optional_decimal(event.max_favorable_return),
        "max_adverse_return": _optional_decimal(event.max_adverse_return),
    }


def _summary_payload(summary: LiquidationCascadeSummary) -> dict[str, object]:
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
