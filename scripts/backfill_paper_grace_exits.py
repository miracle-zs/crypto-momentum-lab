"""Replay and optionally persist the corrected B1/B8 candle-grace exits.

The replay keeps the already-filled entries from ``paper_positions`` and
re-runs only the portfolio marking path over the durable 15-second states.
Official Binance 15-minute candles are loaded for the same candle boundaries
used by the live daemon.  No signals, candidates, fills, or other paper runs
are changed.

The script is intentionally dry-run by default.  Use ``--apply`` only after
reviewing the printed summary.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from crypto_momentum_lab.persistence.postgres.models import (
    PaperEquitySnapshotRow,
    PaperPositionRow,
    RuntimeMarketState15sRow,
    StrategyRunRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    paper_position_from_row,
    paper_position_row,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    market_state_from_row,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    PaperExitConfig,
    PaperExitMode,
    PaperPosition,
    PaperPositionStatus,
    mark_positions,
)

RUN_GRACE_BARS = {
    "paper-account-12-orderflow-b1-long-candle15m-v1": 1,
    "paper-account-13-orderflow-b8-long-candle15m-v1": 8,
}
ENVIRONMENT = "research"
STATE_INTERVAL_SECONDS = 15
MAX_HOLDING_BUCKETS = 5760
INITIAL_BALANCE = Decimal("1000")
DEFAULT_FEE_RATE = Decimal("0.0004")
GRACE_PROFIT_PCT = Decimal("0.0058")
BINANCE_BASE_URL = "https://fapi.binance.com"
FIFTEEN_MINUTES = timedelta(minutes=15)


@dataclass(frozen=True, slots=True)
class OfficialCandle:
    symbol: str
    candle_start: datetime
    candle_end: datetime
    open_price: Decimal
    close_price: Decimal


@dataclass(frozen=True, slots=True)
class SnapshotValues:
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    entry_fees: Decimal
    exit_fees: Decimal
    open_position_count: int

    @property
    def balance(self) -> Decimal:
        return INITIAL_BALANCE + self.realized_pnl

    @property
    def equity(self) -> Decimal:
        return self.balance + self.unrealized_pnl

    @property
    def total_fees(self) -> Decimal:
        return self.entry_fees + self.exit_fees


@dataclass(frozen=True, slots=True)
class ReplayOutput:
    positions: dict[str, PaperPosition]
    snapshots: dict[str, dict[datetime, SnapshotValues]]
    state_count: int
    candle_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the replayed B1/B8 positions and equity snapshots.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Async PostgreSQL URL; defaults to CML_DATABASE_URL.",
    )
    parser.add_argument(
        "--binance-base-url",
        default=BINANCE_BASE_URL,
        help="Binance USD-M REST base URL.",
    )
    return parser.parse_args()


def utc_floor_15m(value: datetime) -> datetime:
    value = _aware(value).astimezone(UTC)
    return value.replace(
        minute=value.minute - value.minute % 15,
        second=0,
        microsecond=0,
    )


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value


async def load_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[
    list[PaperPositionRow],
    list[RuntimeMarketState15sRow],
    list[StrategyRunRow],
    list[PaperEquitySnapshotRow],
]:
    async with session_factory() as session:
        positions = (
            await session.scalars(
                select(PaperPositionRow)
                .where(PaperPositionRow.run_id.in_(tuple(RUN_GRACE_BARS)))
                .order_by(PaperPositionRow.opened_at, PaperPositionRow.position_id)
            )
        ).all()
        if not positions:
            raise RuntimeError("no B1/B8 paper positions found")

        symbols = tuple(sorted({row.symbol for row in positions}))
        minimum_open = min(row.opened_at for row in positions)
        states = (
            await session.scalars(
                select(RuntimeMarketState15sRow)
                .where(
                    RuntimeMarketState15sRow.environment == ENVIRONMENT,
                    RuntimeMarketState15sRow.symbol.in_(symbols),
                    RuntimeMarketState15sRow.bucket_start >= minimum_open,
                )
                .order_by(
                    RuntimeMarketState15sRow.bucket_start,
                    RuntimeMarketState15sRow.symbol,
                )
            )
        ).all()
        if not states:
            raise RuntimeError("no runtime market states cover B1/B8 entries")

        runs = (
            await session.scalars(
                select(StrategyRunRow).where(
                    StrategyRunRow.run_id.in_(tuple(RUN_GRACE_BARS))
                )
            )
        ).all()
        snapshots = (
            await session.scalars(
                select(PaperEquitySnapshotRow)
                .where(PaperEquitySnapshotRow.run_id.in_(tuple(RUN_GRACE_BARS)))
                .order_by(
                    PaperEquitySnapshotRow.run_id,
                    PaperEquitySnapshotRow.observed_at,
                )
            )
        ).all()
    return list(positions), list(states), list(runs), list(snapshots)


def fee_rates(runs: list[StrategyRunRow]) -> dict[str, Decimal]:
    rates: dict[str, Decimal] = {}
    for run in runs:
        config = run.execution_config
        fills = config.get("fills") if isinstance(config, dict) else None
        configured = fills.get("taker_fee_rate") if isinstance(fills, dict) else None
        rates[run.run_id] = (
            Decimal(str(configured)) if configured is not None else DEFAULT_FEE_RATE
        )
    missing = set(RUN_GRACE_BARS) - set(rates)
    if missing:
        raise RuntimeError(f"missing strategy run configuration: {sorted(missing)}")
    return rates


async def load_official_candles(
    *,
    symbols: tuple[str, ...],
    start: datetime,
    end: datetime,
    base_url: str,
) -> dict[tuple[str, datetime], OfficialCandle]:
    if end <= start:
        raise ValueError("official candle range must be non-empty")
    semaphore = asyncio.Semaphore(4)
    output: dict[tuple[str, datetime], OfficialCandle] = {}
    timeout = httpx.Timeout(20.0, connect=10.0)
    limits = httpx.Limits(max_connections=4, max_keepalive_connections=4)

    async with httpx.AsyncClient(
        base_url=base_url.rstrip("/"),
        timeout=timeout,
        limits=limits,
        trust_env=False,
    ) as client:

        async def one(symbol: str) -> tuple[str, dict[datetime, OfficialCandle]]:
            async with semaphore:
                cursor = start
                candles: dict[datetime, OfficialCandle] = {}
                while cursor < end:
                    limit = min(1500, max(1, int((end - cursor) / FIFTEEN_MINUTES)))
                    params: dict[str, str | int] = {
                        "symbol": symbol,
                        "interval": "15m",
                        "startTime": int(cursor.timestamp() * 1000),
                        "endTime": int(end.timestamp() * 1000) - 1,
                        "limit": limit,
                    }
                    page: list[Any] | None = None
                    last_error: Exception | None = None
                    for attempt in range(4):
                        try:
                            response = await client.get(
                                "/fapi/v1/klines", params=params
                            )
                            response.raise_for_status()
                            payload = response.json()
                            if not isinstance(payload, list):
                                raise ValueError("Binance kline page is not a list")
                            page = payload
                            last_error = None
                            break
                        except (
                            httpx.ConnectError,
                            httpx.ReadError,
                            httpx.RemoteProtocolError,
                            httpx.TimeoutException,
                            httpx.HTTPStatusError,
                        ) as error:
                            last_error = error
                            if attempt < 3:
                                await asyncio.sleep(0.5 * (attempt + 1))
                    if last_error is not None or page is None:
                        raise RuntimeError(
                            f"failed to load Binance candles for {symbol}"
                        ) from last_error
                    if not page:
                        raise RuntimeError(
                            "Binance returned no candles for "
                            f"{symbol} at {cursor.isoformat()}"
                        )
                    for row in page:
                        if not isinstance(row, list) or len(row) < 7:
                            raise RuntimeError(f"malformed Binance kline for {symbol}")
                        open_ms, close_ms = row[0], row[6]
                        if not isinstance(open_ms, int) or not isinstance(
                            close_ms, int
                        ):
                            raise RuntimeError(
                                f"invalid Binance timestamps for {symbol}"
                            )
                        candle_start = datetime.fromtimestamp(open_ms / 1000, tz=UTC)
                        candle_end = datetime.fromtimestamp(
                            (close_ms + 1) / 1000,
                            tz=UTC,
                        )
                        if start <= candle_start < end and candle_end <= end:
                            candles[candle_end] = OfficialCandle(
                                symbol=symbol,
                                candle_start=candle_start,
                                candle_end=candle_end,
                                open_price=Decimal(str(row[1])),
                                close_price=Decimal(str(row[4])),
                            )
                    next_start = (
                        max(
                            datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
                            for row in page
                            if isinstance(row, list) and row and isinstance(row[0], int)
                        )
                        + FIFTEEN_MINUTES
                    )
                    if next_start <= cursor:
                        raise RuntimeError(
                            f"Binance kline pagination stalled for {symbol}"
                        )
                    cursor = next_start
                    if len(page) < limit:
                        if cursor < end:
                            raise RuntimeError(
                                "Binance returned an incomplete candle range for "
                                f"{symbol}"
                            )
                        break
                return symbol, candles

        tasks = [asyncio.create_task(one(symbol)) for symbol in symbols]
        for task in asyncio.as_completed(tasks):
            symbol, candles = await task
            output.update(
                {(symbol, candle_end): candle for candle_end, candle in candles.items()}
            )
    return output


def reset_position(row: PaperPositionRow) -> PaperPosition:
    current = paper_position_from_row(row)
    return replace(
        current,
        status=PaperPositionStatus.OPEN,
        closed_at=None,
        exit_price=None,
        exit_fee=Decimal("0"),
        last_mark_price=current.entry_price,
        unrealized_pnl=-current.entry_fee,
        realized_pnl=None,
        return_pct=None,
        close_reason=None,
        grace_exit_started_at=None,
        grace_exit_deadline=None,
        updated_at=current.opened_at,
    )


def exit_config(grace_bars: int) -> PaperExitConfig:
    return PaperExitConfig(
        exit_mode=PaperExitMode.CANDLE_15M,
        max_holding_buckets=MAX_HOLDING_BUCKETS,
        state_interval_seconds=STATE_INTERVAL_SECONDS,
        initial_balance=INITIAL_BALANCE,
        require_executable_quote=True,
        candle_grace_bars=grace_bars,
        candle_grace_profit_pct=GRACE_PROFIT_PCT,
    )


def snapshot_values(
    *,
    run_id: str,
    observed_at: datetime,
    initial: dict[str, PaperPosition],
    current: dict[str, PaperPosition],
) -> SnapshotValues:
    realized = Decimal("0")
    unrealized = Decimal("0")
    entry_fees = Decimal("0")
    exit_fees = Decimal("0")
    open_count = 0
    for position_id, original in initial.items():
        if original.opened_at > observed_at:
            continue
        position = current[position_id]
        entry_fees += position.entry_fee
        exit_fees += position.exit_fee
        if position.status is PaperPositionStatus.CLOSED:
            realized += position.realized_pnl or Decimal("0")
        else:
            unrealized += position.unrealized_pnl
            open_count += 1
    return SnapshotValues(
        realized_pnl=realized,
        unrealized_pnl=unrealized,
        entry_fees=entry_fees,
        exit_fees=exit_fees,
        open_position_count=open_count,
    )


def replay(
    *,
    position_rows: list[PaperPositionRow],
    state_rows: list[RuntimeMarketState15sRow],
    snapshot_rows: list[PaperEquitySnapshotRow],
    rates: dict[str, Decimal],
    candles: dict[tuple[str, datetime], OfficialCandle],
) -> ReplayOutput:
    initial_by_run: dict[str, dict[str, PaperPosition]] = defaultdict(dict)
    current_by_run: dict[str, dict[str, PaperPosition]] = defaultdict(dict)
    for row in position_rows:
        position = reset_position(row)
        initial_by_run[row.run_id][position.position_id] = position
        current_by_run[row.run_id][position.position_id] = position

    snapshot_times_by_run: dict[str, set[datetime]] = defaultdict(set)
    for row in snapshot_rows:
        snapshot_times_by_run[row.run_id].add(row.observed_at)
    snapshot_series: dict[str, dict[datetime, SnapshotValues]] = defaultdict(dict)
    last_candle_end_by_run_symbol: dict[tuple[str, str], datetime] = {}

    grouped_states: dict[datetime, list[RuntimeMarketState15sRow]] = defaultdict(list)
    for row in state_rows:
        grouped_states[row.bucket_start].append(row)

    for bucket_start in sorted(grouped_states):
        for row in grouped_states[bucket_start]:
            state = market_state_from_row(row)
            for run_id, grace_bars in RUN_GRACE_BARS.items():
                active = tuple(
                    position
                    for position in current_by_run[run_id].values()
                    if position.status is PaperPositionStatus.OPEN
                )
                if not active:
                    continue
                candle_end = utc_floor_15m(state.bucket_start)
                has_matching_position = any(
                    position.symbol == state.symbol and position.opened_at < candle_end
                    for position in active
                )
                closed_candle = None
                last_candle_end = last_candle_end_by_run_symbol.get(
                    (run_id, state.symbol)
                )
                if (
                    has_matching_position
                    and candle_end <= state.bucket_start
                    and (last_candle_end is None or candle_end > last_candle_end)
                ):
                    official = candles.get((state.symbol, candle_end))
                    if official is None:
                        raise RuntimeError(
                            "missing official candle for "
                            f"{state.symbol} ending {candle_end.isoformat()}"
                        )
                    closed_candle = official
                    last_candle_end_by_run_symbol[(run_id, state.symbol)] = candle_end
                updates = mark_positions(
                    positions=active,
                    state=state,
                    config=exit_config(grace_bars),
                    taker_fee_rate=rates[run_id],
                    closed_candle=closed_candle,
                )
                for position in updates:
                    current_by_run[run_id][position.position_id] = position

        for run_id in RUN_GRACE_BARS:
            if bucket_start in snapshot_times_by_run[run_id]:
                snapshot_series[run_id][bucket_start] = snapshot_values(
                    run_id=run_id,
                    observed_at=bucket_start,
                    initial=initial_by_run[run_id],
                    current=current_by_run[run_id],
                )

    # Fill snapshots that do not land exactly on a selected-symbol state using
    # the last available replay boundary.  Normal server snapshots align with
    # 15-second states, but this keeps the migration deterministic if that
    # invariant changes later.
    replay_times = sorted(grouped_states)
    for run_id in RUN_GRACE_BARS:
        for observed_at in snapshot_times_by_run[run_id]:
            if observed_at in snapshot_series[run_id]:
                continue
            index = bisect.bisect_right(replay_times, observed_at) - 1
            if index < 0:
                current = initial_by_run[run_id]
            else:
                # The current map is at the final replay boundary.  No normal
                # snapshot falls before the first state; fail rather than
                # silently writing a future state into an old snapshot.
                raise RuntimeError(
                    f"snapshot boundary {observed_at.isoformat()} was not replayed"
                )
            snapshot_series[run_id][observed_at] = snapshot_values(
                run_id=run_id,
                observed_at=observed_at,
                initial=initial_by_run[run_id],
                current=current,
            )

    positions = {
        position_id: position
        for run_positions in current_by_run.values()
        for position_id, position in run_positions.items()
    }
    return ReplayOutput(
        positions=positions,
        snapshots=dict(snapshot_series),
        state_count=len(state_rows),
        candle_count=len(candles),
    )


def position_values(position: PaperPosition) -> dict[str, object]:
    return cast(dict[str, object], paper_position_row(position))


def snapshot_values_dict(values: SnapshotValues) -> dict[str, object]:
    return {
        "balance": values.balance,
        "equity": values.equity,
        "realized_pnl": values.realized_pnl,
        "unrealized_pnl": values.unrealized_pnl,
        "total_fees": values.total_fees,
        "open_position_count": values.open_position_count,
    }


def summary(
    position_rows: list[PaperPositionRow],
    output: ReplayOutput,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "state_count": output.state_count,
        "official_candle_count": output.candle_count,
        "runs": {},
    }
    for run_id in RUN_GRACE_BARS:
        before = [row for row in position_rows if row.run_id == run_id]
        after = [output.positions[row.position_id] for row in before]

        def stats(items: list[Any]) -> dict[str, Any]:
            closed = [
                item
                for item in items
                if getattr(item.status, "value", item.status) == "closed"
            ]
            wins = [item for item in closed if (item.realized_pnl or Decimal("0")) > 0]
            pnl = sum(
                (item.realized_pnl or Decimal("0") for item in closed),
                Decimal("0"),
            )
            return {
                "closed": len(closed),
                "open": len(items) - len(closed),
                "wins": len(wins),
                "win_rate": str(
                    (Decimal(len(wins)) / Decimal(len(closed)))
                    if closed
                    else Decimal("0")
                ),
                "realized_pnl": str(pnl),
            }

        result["runs"][run_id] = {
            "before": stats(before),
            "after": stats(after),
            "changed_positions": sum(
                any(
                    getattr(row, key) != value
                    for key, value in position_values(
                        output.positions[row.position_id]
                    ).items()
                    if key
                    not in {
                        "position_id",
                        "run_id",
                        "entry_fill_id",
                        "signal_id",
                        "symbol",
                        "side",
                        "entry_price",
                        "quantity",
                        "entry_notional",
                        "entry_fee",
                        "opened_at",
                    }
                )
                for row in before
            ),
            "snapshots": len(output.snapshots.get(run_id, {})),
        }
    return result


async def apply_output(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    position_rows: list[PaperPositionRow],
    snapshot_rows: list[PaperEquitySnapshotRow],
    output: ReplayOutput,
) -> None:
    async with session_factory() as session:
        async with session.begin():
            for row in position_rows:
                position_update_values = {
                    key: value
                    for key, value in position_values(
                        output.positions[row.position_id]
                    ).items()
                    if key
                    not in {
                        "position_id",
                        "run_id",
                        "entry_fill_id",
                        "signal_id",
                        "symbol",
                        "side",
                        "entry_price",
                        "quantity",
                        "entry_notional",
                        "entry_fee",
                        "opened_at",
                    }
                }
                await session.execute(
                    update(PaperPositionRow)
                    .where(
                        PaperPositionRow.position_id == row.position_id,
                        PaperPositionRow.run_id.in_(tuple(RUN_GRACE_BARS)),
                    )
                    .values(**position_update_values)
                )
            for row in snapshot_rows:
                snapshot_update = output.snapshots[row.run_id].get(row.observed_at)
                if snapshot_update is None:
                    raise RuntimeError(
                        f"missing replay snapshot for {row.run_id} at {row.observed_at}"
                    )
                await session.execute(
                    update(PaperEquitySnapshotRow)
                    .where(
                        PaperEquitySnapshotRow.snapshot_id == row.snapshot_id,
                        PaperEquitySnapshotRow.run_id.in_(tuple(RUN_GRACE_BARS)),
                    )
                    .values(**snapshot_values_dict(snapshot_update))
                )


async def main() -> None:
    args = parse_args()
    database_url = args.database_url
    if not database_url:
        import os

        database_url = os.environ.get("CML_DATABASE_URL")
    if not database_url:
        raise RuntimeError("CML_DATABASE_URL or --database-url is required")

    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        position_rows, state_rows, runs, snapshot_rows = await load_rows(
            session_factory
        )
        rates = fee_rates(runs)
        symbols = tuple(sorted({row.symbol for row in position_rows}))
        minimum_open = min(row.opened_at for row in position_rows)
        maximum_state = max(row.bucket_start for row in state_rows)
        candle_start = utc_floor_15m(minimum_open) - FIFTEEN_MINUTES
        candle_end = utc_floor_15m(maximum_state)
        candles = await load_official_candles(
            symbols=symbols,
            start=candle_start,
            end=candle_end,
            base_url=args.binance_base_url,
        )
        output = replay(
            position_rows=position_rows,
            state_rows=state_rows,
            snapshot_rows=snapshot_rows,
            rates=rates,
            candles=candles,
        )
        print(json.dumps(summary(position_rows, output), default=str, indent=2))
        if args.apply:
            await apply_output(
                session_factory=session_factory,
                position_rows=position_rows,
                snapshot_rows=snapshot_rows,
                output=output,
            )
            print("APPLIED B1/B8 position and equity snapshot backfill")
        else:
            print("DRY RUN: no database rows changed; rerun with --apply")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:  # noqa: BLE001
        print(f"backfill failed: {type(error).__name__}: {error}", file=sys.stderr)
        raise
