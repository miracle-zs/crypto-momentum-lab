"""Build a replay candidate-event table for orderflow_impulse optimization.

The script intentionally evaluates a relaxed event envelope and stores the
features needed to apply stricter entry thresholds offline.  It does not
write to PostgreSQL.  It expects the official 15-minute kline archive to be
available at ``--klines`` and writes a gzipped CSV plus a JSON manifest.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import gzip
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    OrderFlowImpulseEvent,
    find_order_flow_impulses,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
BUCKET = timedelta(seconds=15)
CANDLE = timedelta(minutes=15)
MAX_LABEL_CANDLES = 8
MAX_CONFIRMATION_BUCKETS = 3

CURRENT_RETURN = Decimal("0.01")
CURRENT_IMBALANCE = Decimal("0.40")
CURRENT_INTENSITY = Decimal("2")


@dataclass(frozen=True, slots=True)
class Candle:
    start: datetime
    open_price: Decimal
    close_price: Decimal


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    snapshot_id: str
    observed_at: datetime


class UniverseAsOf:
    def __init__(
        self,
        snapshots: list[UniverseSnapshot],
        eligible: dict[str, dict[str, tuple[int, Decimal]]],
    ) -> None:
        self._snapshots = snapshots
        self._times = [item.observed_at for item in snapshots]
        self._eligible = eligible

    def lookup(
        self,
        observed_at: datetime,
        symbol: str,
    ) -> tuple[UniverseSnapshot, int, Decimal] | None:
        index = bisect.bisect_right(self._times, observed_at) - 1
        if index < 0:
            return None
        snapshot = self._snapshots[index]
        item = self._eligible.get(snapshot.snapshot_id, {}).get(symbol)
        if item is None:
            return None
        rank, utc_day_return = item
        return snapshot, rank, utc_day_return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        default="2026-08-31T06:00:00Z",
        help="effective start in UTC; 2026-08-31T06:00:00Z is 14:00 Shanghai",
    )
    parser.add_argument(
        "--end",
        default="2026-09-02T09:10:00Z",
        help="exclusive replay end in UTC",
    )
    parser.add_argument(
        "--klines",
        type=Path,
        default=Path("/tmp/cml-orderflow-15m-archive-20260831-20260902.csv.gz"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "/tmp/cml-orderflow-candidate-pool-20260831-20260902.csv.gz"
        ),
    )
    parser.add_argument("--universe-top", type=int, default=10)
    parser.add_argument(
        "--environment",
        default="research",
    )
    return parser.parse_args()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"datetime must be timezone-aware: {value}")
    return parsed.astimezone(UTC)


def decimal_or_none(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def state_price(state: MarketState15s) -> Decimal | None:
    return state.close_price or state.midpoint or state.mark_price


def state_imbalance(state: MarketState15s) -> Decimal:
    total = state.aggressive_buy_notional + state.aggressive_sell_notional
    if total <= 0:
        return Decimal("0")
    return (
        state.aggressive_buy_notional - state.aggressive_sell_notional
    ) / total


def relaxed_config() -> OrderFlowImpulseConfig:
    return OrderFlowImpulseConfig(
        impulse_window_buckets=3,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.000001"),
        min_aggressive_imbalance=Decimal("0"),
        min_notional_intensity=Decimal("0.000001"),
        confirmation_buckets=1,
        cooldown_buckets=0,
        forward_horizon_buckets=(1,),
    )


def state_from_row(row: object) -> MarketState15s:
    item = row  # SQLAlchemy mapping; keeping construction explicit is safer.
    return MarketState15s(
        schema_version=int(item["schema_version"]),
        exchange=str(item["exchange"]),
        environment=str(item["environment"]),
        symbol=str(item["symbol"]),
        bucket_start=item["bucket_start"],
        bucket_end=item["bucket_end"],
        open_price=decimal_or_none(item["open_price"]),
        high_price=decimal_or_none(item["high_price"]),
        low_price=decimal_or_none(item["low_price"]),
        close_price=decimal_or_none(item["close_price"]),
        trade_count=int(item["trade_count"]),
        trade_notional=Decimal(str(item["trade_notional"])),
        aggressive_buy_notional=Decimal(str(item["aggressive_buy_notional"])),
        aggressive_sell_notional=Decimal(str(item["aggressive_sell_notional"])),
        last_bid_price=decimal_or_none(item["last_bid_price"]),
        last_ask_price=decimal_or_none(item["last_ask_price"]),
        spread=decimal_or_none(item["spread"]),
        midpoint=decimal_or_none(item["midpoint"]),
        liquidation_count=int(item["liquidation_count"]),
        liquidation_notional=Decimal(str(item["liquidation_notional"])),
        mark_price=decimal_or_none(item["mark_price"]),
        closed_kline_count=int(item["closed_kline_count"]),
        source_event_count=int(item["source_event_count"]),
        first_received_at=item["first_received_at"],
        last_received_at=item["last_received_at"],
    )


def load_klines(path: Path) -> dict[str, dict[datetime, Candle]]:
    result: dict[str, dict[datetime, Candle]] = defaultdict(dict)
    with gzip.open(path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            start = parse_datetime(row["candle_start"])
            result[row["symbol"]][start] = Candle(
                start=start,
                open_price=Decimal(row["open_price"]),
                close_price=Decimal(row["close_price"]),
            )
    return dict(result)


async def load_universe(
    engine: AsyncEngine,
    *,
    end: datetime,
    top_count: int,
) -> UniverseAsOf:
    snapshot_query = text(
        """
        SELECT snapshot_id::text AS snapshot_id, observed_at
        FROM universe_snapshots
        WHERE activated = true AND observed_at <= :end
        ORDER BY observed_at
        """
    )
    entry_query = text(
        """
        SELECT s.snapshot_id::text AS snapshot_id, e.symbol,
               e.gainer_rank, e.utc_day_return
        FROM universe_snapshots AS s
        JOIN universe_entries AS e ON e.snapshot_id = s.snapshot_id
        WHERE s.activated = true
          AND s.observed_at <= :end
          AND e.gainer_rank BETWEEN 1 AND :top_count
          AND e.utc_day_return > 0
        """
    )
    async with engine.connect() as connection:
        snapshot_result = await connection.execute(
            snapshot_query,
            {"end": end},
        )
        snapshots = [
            UniverseSnapshot(
                snapshot_id=str(row["snapshot_id"]),
                observed_at=row["observed_at"],
            )
            for row in snapshot_result.mappings()
        ]
        entry_result = await connection.execute(
            entry_query,
            {"end": end, "top_count": top_count},
        )
        eligible: dict[str, dict[str, tuple[int, Decimal]]] = defaultdict(dict)
        for row in entry_result.mappings():
            eligible[str(row["snapshot_id"])][str(row["symbol"])] = (
                int(row["gainer_rank"]),
                Decimal(str(row["utc_day_return"])),
            )
    return UniverseAsOf(snapshots, dict(eligible))


def first_future_candle_start(signal_at: datetime) -> datetime:
    floor = signal_at.replace(
        minute=(signal_at.minute // 15) * 15,
        second=0,
        microsecond=0,
    )
    return floor if signal_at == floor else floor + CANDLE


def label_event(
    event: OrderFlowImpulseEvent,
    *,
    entry_price: Decimal,
    candles: dict[datetime, Candle],
) -> dict[str, object]:
    signal_at = event.detected_at + BUCKET
    first_start = first_future_candle_start(signal_at)
    future = [candles.get(first_start + CANDLE * i) for i in range(MAX_LABEL_CANDLES)]
    available: list[Candle] = []
    for candle in future:
        if candle is None:
            break
        available.append(candle)
    labeled = len(available) == MAX_LABEL_CANDLES

    green_run = 0
    first_bearish_offset: int | None = None
    for index, candle in enumerate(available):
        if candle.close_price > candle.open_price and first_bearish_offset is None:
            green_run += 1
        elif (
            candle.close_price < candle.open_price
            and first_bearish_offset is None
        ):
            first_bearish_offset = index
            break
        else:
            break

    first_bearish_return: Decimal | None = None
    if first_bearish_offset is not None:
        first_bearish_return = (
            available[first_bearish_offset].close_price / entry_price - 1
        )
    green_run_return: Decimal | None = None
    if green_run > 0:
        green_run_return = available[green_run - 1].close_price / entry_price - 1

    def close_return(index: int) -> Decimal | None:
        if len(available) <= index:
            return None
        return available[index].close_price / entry_price - 1

    return {
        "label_status": "labeled" if labeled else "pending_or_unlabeled",
        "first_future_candle_start": first_start.isoformat(),
        "green_run_from_first": green_run if available else None,
        "first_bearish_offset_15m": first_bearish_offset,
        "decision_exit_return_pct": first_bearish_return,
        "green_run_from_first_return_pct": green_run_return,
        "close_return_15m_pct": close_return(0),
        "close_return_30m_pct": close_return(1),
        "close_return_60m_pct": close_return(3),
        "close_return_120m_pct": close_return(7),
    }


def split_segments(states: list[MarketState15s]) -> list[list[MarketState15s]]:
    segments: list[list[MarketState15s]] = []
    current: list[MarketState15s] = []
    previous_processed_at: datetime | None = None
    for state in states:
        if (
            previous_processed_at is not None
            and state.bucket_start - previous_processed_at > timedelta(seconds=30)
        ):
            if current:
                segments.append(current)
            current = []
        previous_processed_at = state.bucket_start
        if state.close_price is not None:
            current.append(state)
    if current:
        segments.append(current)
    return segments


def event_row(
    event: OrderFlowImpulseEvent,
    *,
    states: list[MarketState15s],
    universe: UniverseAsOf,
    klines: dict[str, dict[datetime, Candle]],
) -> dict[str, object]:
    starts = [state.bucket_start for state in states]
    index = bisect.bisect_left(starts, event.detected_at)
    detection_state = states[index]
    entry_price = state_price(detection_state)
    if entry_price is None or entry_price <= 0:
        raise ValueError("event detection state has no positive price")

    signal_at = event.detected_at + BUCKET
    universe_item = universe.lookup(signal_at, event.symbol)
    snapshot, rank, utc_day_return = universe_item or (None, None, None)
    confirmation_values: dict[str, object] = {}
    for offset in range(MAX_CONFIRMATION_BUCKETS):
        confirmation_index = index + offset
        if confirmation_index >= len(states):
            confirmation_values[f"confirm_{offset + 1}_price"] = None
            confirmation_values[f"confirm_{offset + 1}_imbalance"] = None
            continue
        confirmation_state = states[confirmation_index]
        confirmation_values[f"confirm_{offset + 1}_price"] = state_price(
            confirmation_state
        )
        confirmation_values[f"confirm_{offset + 1}_imbalance"] = state_imbalance(
            confirmation_state
        )

    confirm_1_price = state_price(detection_state)
    confirm_1_pass = (
        confirm_1_price is not None
        and confirm_1_price > event.breakout_level
        and state_imbalance(detection_state) >= CURRENT_IMBALANCE
    )
    current_event_pass = (
        event.impulse_return_pct >= CURRENT_RETURN
        and event.aggressive_imbalance >= CURRENT_IMBALANCE
        and event.notional_intensity >= CURRENT_INTENSITY
        and confirm_1_pass
    )
    labels = label_event(
        event,
        entry_price=entry_price,
        candles=klines.get(event.symbol, {}),
    )
    row: dict[str, object] = {
        "event_id": f"{event.symbol}|{signal_at.isoformat()}|{event.direction.value}",
        "symbol": event.symbol,
        "direction": event.direction.value,
        "event_detected_at": event.detected_at.isoformat(),
        "signal_at": signal_at.isoformat(),
        "impulse_start": event.impulse_start.isoformat(),
        "impulse_end": event.impulse_end.isoformat(),
        "entry_price": entry_price,
        "impulse_start_price": event.impulse_start_price,
        "impulse_end_price": event.impulse_end_price,
        "impulse_return_pct": event.impulse_return_pct,
        "aggressive_imbalance": event.aggressive_imbalance,
        "notional_intensity": event.notional_intensity,
        "baseline_notional": event.baseline_notional,
        "breakout_level": event.breakout_level,
        "breakout_distance_pct": event.breakout_distance_pct,
        "impulse_trade_count": event.impulse_trade_count,
        "impulse_trade_notional": event.impulse_trade_notional,
        "aggressive_buy_notional": event.aggressive_buy_notional,
        "aggressive_sell_notional": event.aggressive_sell_notional,
        "spread": event.spread,
        "midpoint": event.midpoint,
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": event.liquidation_notional,
        "snapshot_at": None if snapshot is None else snapshot.observed_at.isoformat(),
        "gainer_rank": rank,
        "utc_day_return": utc_day_return,
        "entry_pool_pass": universe_item is not None,
        "current_event_pass": current_event_pass,
        "current_top10_baseline_pass": (
            universe_item is not None and current_event_pass
        ),
        "fee_round_trip_pct": Decimal("0.0008"),
    }
    row.update(confirmation_values)
    row.update(labels)
    if row["decision_exit_return_pct"] is not None:
        row["decision_exit_net_return_pct"] = (
            row["decision_exit_return_pct"] - row["fee_round_trip_pct"]
        )
    else:
        row["decision_exit_net_return_pct"] = None
    return row


def serialise(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


async def build(args: argparse.Namespace) -> dict[str, object]:
    start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    if end <= start:
        raise ValueError("--end must be after --start")
    if not args.klines.exists():
        raise FileNotFoundError(args.klines)

    klines = load_klines(args.klines)
    engine = create_async_engine(__import__("os").environ["CML_DATABASE_URL"])
    universe = await load_universe(
        engine,
        end=end,
        top_count=args.universe_top,
    )
    state_query = text(
        """
        SELECT schema_version, exchange, environment, symbol,
               bucket_start, bucket_end, open_price, high_price, low_price,
               close_price, trade_count, trade_notional,
               aggressive_buy_notional, aggressive_sell_notional,
               last_bid_price, last_ask_price, spread, midpoint,
               liquidation_count, liquidation_notional, mark_price,
               closed_kline_count, source_event_count,
               first_received_at, last_received_at
        FROM runtime_market_states_15s
        WHERE environment = :environment
          AND bucket_start >= :start
          AND bucket_start < :end
        ORDER BY symbol, bucket_start
        """
    )
    config = relaxed_config()
    fieldnames: list[str] | None = None
    rows_written = 0
    top10_candidates = 0
    current_baseline = 0
    labeled_current_baseline = 0
    pending_current_baseline = 0
    state_rows = 0
    symbols = 0
    gap_segments = 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.output, "wt", newline="") as handle:
        writer: csv.DictWriter[str] | None = None

        def write_symbol(symbol_states: list[MarketState15s]) -> None:
            nonlocal writer, rows_written, top10_candidates, current_baseline
            nonlocal labeled_current_baseline, pending_current_baseline, gap_segments
            for segment in split_segments(symbol_states):
                gap_segments += 1
                events = find_order_flow_impulses(segment, config)
                for event in events:
                    if event.direction is not OrderFlowDirection.UP:
                        continue
                    signal_at = event.detected_at + BUCKET
                    if not start <= signal_at < end:
                        continue
                    row = event_row(
                        event,
                        states=segment,
                        universe=universe,
                        klines=klines,
                    )
                    if row["entry_pool_pass"]:
                        top10_candidates += 1
                    if row["current_top10_baseline_pass"]:
                        current_baseline += 1
                        if row["label_status"] == "labeled":
                            labeled_current_baseline += 1
                        else:
                            pending_current_baseline += 1
                    if writer is None:
                        fieldnames = list(row.keys())
                        writer = csv.DictWriter(handle, fieldnames=fieldnames)
                        writer.writeheader()
                    writer.writerow({key: serialise(value) for key, value in row.items()})
                    rows_written += 1

        async with engine.connect() as connection:
            result = await connection.stream(
                state_query,
                {
                    "environment": args.environment,
                    "start": start,
                    "end": end,
                },
            )
            current_symbol: str | None = None
            symbol_states: list[MarketState15s] = []
            async for raw_row in result.mappings():
                state_rows += 1
                symbol = str(raw_row["symbol"])
                if current_symbol is not None and symbol != current_symbol:
                    write_symbol(symbol_states)
                    symbols += 1
                    if symbols % 50 == 0:
                        print(
                            f"progress symbols={symbols} states={state_rows} "
                            f"events={rows_written}",
                            flush=True,
                        )
                    symbol_states = []
                current_symbol = symbol
                symbol_states.append(state_from_row(raw_row))
            if symbol_states:
                write_symbol(symbol_states)
                symbols += 1
    await engine.dispose()

    manifest = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "start_utc": start.isoformat(),
        "end_utc_exclusive": end.isoformat(),
        "start_shanghai": start.astimezone(SHANGHAI).isoformat(),
        "end_shanghai_exclusive": end.astimezone(SHANGHAI).isoformat(),
        "environment": args.environment,
        "state_table": "runtime_market_states_15s",
        "kline_source": str(args.klines),
        "universe_definition": f"activated positive gainer top {args.universe_top}, as-of signal_at",
        "event_envelope": {
            "impulse_window_buckets": config.impulse_window_buckets,
            "baseline_window_buckets": config.baseline_window_buckets,
            "breakout_window_buckets": config.breakout_window_buckets,
            "min_return_pct": str(config.min_return_pct),
            "min_aggressive_imbalance": str(config.min_aggressive_imbalance),
            "min_notional_intensity": str(config.min_notional_intensity),
            "confirmation_buckets": config.confirmation_buckets,
            "cooldown_buckets": config.cooldown_buckets,
        },
        "current_baseline": {
            "min_return_pct": str(CURRENT_RETURN),
            "min_aggressive_imbalance": str(CURRENT_IMBALANCE),
            "min_notional_intensity": str(CURRENT_INTENSITY),
            "confirmation_buckets": 1,
            "cooldown_buckets": 0,
        },
        "counts": {
            "state_rows": state_rows,
            "symbols": symbols,
            "gap_segments": gap_segments,
            "relaxed_up_events_written": rows_written,
            "top10_candidate_events": top10_candidates,
            "current_top10_baseline_events": current_baseline,
            "current_top10_baseline_labeled": labeled_current_baseline,
            "current_top10_baseline_pending": pending_current_baseline,
        },
        "output": str(args.output),
    }
    manifest_path = args.output.with_suffix(args.output.suffix + ".manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return manifest


if __name__ == "__main__":
    asyncio.run(build(parse_args()))
