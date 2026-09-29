#!/usr/bin/env python3
"""Inspect orderflow features behind realized live-primary winners.

This is a read-only account analysis.  It pairs live account BUY/SELL fills
FIFO, attributes each closed entry cohort to the nearest preceding persisted
orderflow_impulse signal, and prints only compact PnL/feature summaries.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


FEE_ZERO = Decimal("0")
LOCAL_OFFSET = timedelta(hours=8)


@dataclass(frozen=True, slots=True)
class Signal:
    symbol: str
    detected_at: datetime
    impulse_return: float | None
    imbalance: float | None
    intensity: float | None
    breakout_distance: float | None
    entry_price: float | None


@dataclass(slots=True)
class Lot:
    symbol: str
    opened_at: datetime
    quantity: Decimal
    price: Decimal
    fee: Decimal
    signal: Signal | None
    signal_delay_seconds: float | None


@dataclass(slots=True)
class Trade:
    symbol: str
    opened_at: datetime
    signal: Signal | None
    signal_delay_seconds: float | None
    entry_notional: Decimal = FEE_ZERO
    entry_fee: Decimal = FEE_ZERO
    gross_pnl: Decimal = FEE_ZERO
    exit_fee: Decimal = FEE_ZERO
    net_pnl: Decimal = FEE_ZERO
    closed_at: datetime | None = None

    @property
    def return_pct(self) -> Decimal | None:
        if self.entry_notional <= 0:
            return None
        return self.net_pnl / self.entry_notional * Decimal("100")


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(UTC)


def decimal_value(value: object) -> Decimal:
    return Decimal(str(value)) if value is not None else Decimal("0")


def feature_float(features: object, key: str) -> float | None:
    if not isinstance(features, dict):
        return None
    value = features.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_time(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(UTC).isoformat(timespec="seconds")


def signal_from_row(row: object) -> Signal:
    return Signal(
        symbol=str(row["symbol"]),
        detected_at=row["detected_at"],
        impulse_return=feature_float(row["features"], "impulse_return_pct"),
        imbalance=feature_float(row["features"], "aggressive_imbalance"),
        intensity=feature_float(row["features"], "notional_intensity"),
        breakout_distance=feature_float(
            row["features"], "breakout_distance_pct"
        ),
        entry_price=feature_float(row["features"], "entry_price"),
    )


def nearest_preceding_signal(
    signals_by_symbol: dict[str, list[Signal]],
    signal_times: dict[str, list[datetime]],
    *,
    symbol: str,
    opened_at: datetime,
) -> tuple[Signal | None, float | None]:
    signals = signals_by_symbol.get(symbol, [])
    times = signal_times.get(symbol, [])
    index = bisect.bisect_right(times, opened_at) - 1
    if index < 0:
        return None, None
    signal = signals[index]
    return signal, (opened_at - signal.detected_at).total_seconds()


async def load_rows(
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    database_url = __import__("os").environ["CML_DATABASE_URL"]
    engine = create_async_engine(database_url)
    fill_query = text(
        """
        SELECT symbol, side, price, quantity, realized_pnl, fee, trade_at
        FROM account_fill_events
        WHERE environment = 'live'
          AND account_label = 'primary'
          AND trade_at >= :start
          AND trade_at < :end
        ORDER BY symbol, trade_at, trade_id
        """
    )
    signal_query = text(
        """
        SELECT symbol, detected_at, features
        FROM live_strategy_signals
        WHERE account_label = 'primary'
          AND strategy_name = 'orderflow_impulse'
          AND signal_kind = 'strategy_signal'
          AND detected_at >= :start
          AND detected_at < :end
        ORDER BY symbol, detected_at
        """
    )
    async with engine.connect() as connection:
        fill_result = await connection.execute(
            fill_query, {"start": start, "end": end}
        )
        fills = [dict(row) for row in fill_result.mappings()]
        signal_result = await connection.execute(
            signal_query, {"start": start, "end": end}
        )
        signals = [dict(row) for row in signal_result.mappings()]
    await engine.dispose()
    return fills, signals


def pair_trades(
    fills: list[dict[str, object]],
    signals: list[dict[str, object]],
    *,
    cohort_start: datetime,
) -> tuple[list[Trade], int, int]:
    signals_by_symbol: dict[str, list[Signal]] = defaultdict(list)
    for row in signals:
        item = signal_from_row(row)
        signals_by_symbol[item.symbol].append(item)
    signal_times = {
        symbol: [item.detected_at for item in items]
        for symbol, items in signals_by_symbol.items()
    }

    fills_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)
    for fill in fills:
        fills_by_symbol[str(fill["symbol"])].append(fill)

    trades_by_key: dict[tuple[str, datetime], Trade] = {}
    unmatched_sell_quantity = 0
    matched_entry_lots = 0
    for symbol, symbol_fills in fills_by_symbol.items():
        lots: deque[Lot] = deque()
        for fill in sorted(symbol_fills, key=lambda item: item["trade_at"]):
            side = str(fill["side"]).upper()
            quantity = decimal_value(fill["quantity"])
            if quantity <= 0:
                continue
            price = decimal_value(fill["price"])
            fee = decimal_value(fill["fee"])
            trade_at = fill["trade_at"]
            if side == "BUY":
                signal, delay = nearest_preceding_signal(
                    signals_by_symbol,
                    signal_times,
                    symbol=symbol,
                    opened_at=trade_at,
                )
                lots.append(
                    Lot(
                        symbol=symbol,
                        opened_at=trade_at,
                        quantity=quantity,
                        price=price,
                        fee=fee,
                        signal=signal,
                        signal_delay_seconds=delay,
                    )
                )
                continue
            if side != "SELL":
                continue

            remaining = quantity
            while remaining > 0 and lots:
                lot = lots[0]
                consumed = min(remaining, lot.quantity)
                ratio = consumed / quantity
                entry_ratio = consumed / lot.quantity
                if lot.opened_at >= cohort_start:
                    signal_time = (
                        lot.signal.detected_at
                        if lot.signal is not None
                        else lot.opened_at
                    )
                    key = (symbol, signal_time)
                    trade = trades_by_key.get(key)
                    if trade is None:
                        trade = Trade(
                            symbol=symbol,
                            opened_at=lot.opened_at,
                            signal=lot.signal,
                            signal_delay_seconds=lot.signal_delay_seconds,
                        )
                        trades_by_key[key] = trade
                    trade.entry_notional += lot.price * consumed
                    trade.entry_fee += lot.fee * entry_ratio
                    trade.gross_pnl += decimal_value(fill["realized_pnl"]) * ratio
                    trade.exit_fee += fee * ratio
                    trade.net_pnl = (
                        trade.gross_pnl - trade.entry_fee - trade.exit_fee
                    )
                    trade.closed_at = trade_at
                    matched_entry_lots += 1
                lot.quantity -= consumed
                remaining -= consumed
                if lot.quantity <= 0:
                    lots.popleft()
            if remaining > 0:
                unmatched_sell_quantity += 1

    return list(trades_by_key.values()), matched_entry_lots, unmatched_sell_quantity


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def feature_stats(trades: list[Trade]) -> dict[str, dict[str, float | None]]:
    result: dict[str, dict[str, float | None]] = {}
    for label, getter in (
        ("min_return_pct", lambda item: item.signal.impulse_return if item.signal else None),
        ("min_aggressive_imbalance", lambda item: item.signal.imbalance if item.signal else None),
        ("min_notional_intensity", lambda item: item.signal.intensity if item.signal else None),
    ):
        values = [value for item in trades if (value := getter(item)) is not None]
        result[label] = {
            "count": float(len(values)),
            "p25": percentile(values, 0.25),
            "median": percentile(values, 0.50),
            "p75": percentile(values, 0.75),
            "min": min(values) if values else None,
            "max": max(values) if values else None,
        }
    return result


def print_trade(item: Trade) -> None:
    signal = item.signal
    print(
        "| {symbol} | {opened} | {closed} | {net:+.4f}U | {return_pct:+.3f}% | "
        "{ret} | {imb} | {intensity} | {delay}s |".format(
            symbol=item.symbol,
            opened=fmt_time(item.opened_at),
            closed=fmt_time(item.closed_at),
            net=float(item.net_pnl),
            return_pct=float(item.return_pct or 0),
            ret=(f"{signal.impulse_return:.5f}" if signal and signal.impulse_return is not None else "n/a"),
            imb=(f"{signal.imbalance:.3f}" if signal and signal.imbalance is not None else "n/a"),
            intensity=(f"{signal.intensity:.2f}" if signal and signal.intensity is not None else "n/a"),
            delay=(f"{item.signal_delay_seconds:.1f}" if item.signal_delay_seconds is not None else "n/a"),
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-08-31T06:00:00Z")
    parser.add_argument("--end", default="2026-09-03T00:00:00Z")
    parser.add_argument(
        "--signal-history-minutes",
        type=int,
        default=10,
        help="extra signal history before the entry cohort",
    )
    parser.add_argument("--top", type=int, default=12)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    cohort_start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    signal_start = cohort_start - timedelta(minutes=args.signal_history_minutes)
    fills, signals = await load_rows(signal_start, end)
    trades, matched_lots, unmatched_sells = pair_trades(
        fills,
        signals,
        cohort_start=cohort_start,
    )
    closed = [item for item in trades if item.closed_at is not None]
    winners = [item for item in closed if item.net_pnl > 0]
    losers = [item for item in closed if item.net_pnl <= 0]
    top_by_pnl = sorted(closed, key=lambda item: item.net_pnl, reverse=True)
    top_by_return = sorted(
        closed,
        key=lambda item: item.return_pct or Decimal("-inf"),
        reverse=True,
    )

    print(f"fills={len(fills)} signals={len(signals)}")
    print(
        f"closed_entry_cohorts={len(closed)} winners={len(winners)} "
        f"losers_or_breakeven={len(losers)} matched_entry_lots={matched_lots} "
        f"unmatched_sell_chunks={unmatched_sells}"
    )
    print("winner_feature_stats")
    for label, values in feature_stats(winners).items():
        print(label, values)
    print("loser_feature_stats")
    for label, values in feature_stats(losers).items():
        print(label, values)

    print("top_winners_by_net_pnl")
    print(
        "|symbol|entry_signal_or_fill_utc|last_exit_utc|net_pnl|net_return|"
        "impulse_return|imbalance|intensity|signal_delay|"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for item in top_by_pnl[: args.top]:
        print_trade(item)

    print("top_winners_by_net_return")
    print(
        "|symbol|entry_signal_or_fill_utc|last_exit_utc|net_pnl|net_return|"
        "impulse_return|imbalance|intensity|signal_delay|"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for item in top_by_return[: args.top]:
        print_trade(item)


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
