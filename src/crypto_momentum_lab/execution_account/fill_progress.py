"""Pure fill identities, counts and next polling cursor calculation."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
)
from crypto_momentum_lab.execution_account.sync_models import FillKey

_FILL_FETCH_OVERLAP_MS = 60_000
_NEW_POSITION_FILL_LOOKBACK = timedelta(minutes=30)
_HISTORICAL_FILL_LOOKBACK = timedelta(days=7)


@dataclass(frozen=True, slots=True)
class FillCursor:
    from_id: int | None = None
    start_time_ms: int | None = None


def account_fill_keys(fills: tuple[AccountFillEvent, ...]) -> set[FillKey]:
    return {(fill.symbol.strip().upper(), fill.trade_id.strip()) for fill in fills}


def fill_counts_by_symbol(
    fills: tuple[AccountFillEvent, ...],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for fill in fills:
        symbol = fill.symbol.strip().upper()
        counts[symbol] = counts.get(symbol, 0) + 1
    return tuple(sorted(counts.items()))


def advance_fill_cursors(
    previous: dict[str, FillCursor],
    symbols: tuple[str, ...],
    fills: tuple[AccountFillEvent, ...],
    *,
    observed_at: datetime,
) -> dict[str, FillCursor]:
    next_cursors = dict(previous)
    max_trade_id_by_symbol: dict[str, int] = {}
    for fill in fills:
        try:
            trade_id = int(fill.trade_id)
        except (TypeError, ValueError):
            continue
        symbol = fill.symbol.strip().upper()
        current = max_trade_id_by_symbol.get(symbol)
        if current is None or trade_id > current:
            max_trade_id_by_symbol[symbol] = trade_id

    observed_at_ms = int(observed_at.timestamp() * 1000)
    for symbol in symbols:
        max_trade_id = max_trade_id_by_symbol.get(symbol)
        if max_trade_id is not None:
            next_cursors[symbol] = FillCursor(from_id=max_trade_id + 1)
            continue
        cursor = previous.get(symbol)
        if cursor is None:
            next_cursors[symbol] = FillCursor(start_time_ms=observed_at_ms)
        elif cursor.from_id is None and cursor.start_time_ms is not None:
            next_cursors[symbol] = FillCursor(
                start_time_ms=max(0, observed_at_ms - _FILL_FETCH_OVERLAP_MS)
            )
    return next_cursors


def merge_fill_cursor(
    current: FillCursor | None,
    cursor: AccountFillReconciliationCursor,
) -> FillCursor | None:
    """Keep ID progress ahead of time progress and reject numeric regression."""
    if cursor.from_id is not None:
        new_from_id = (
            max(current.from_id, cursor.from_id)
            if current is not None and current.from_id is not None
            else cursor.from_id
        )
        new_start_time_ms = None
    elif cursor.start_time_ms is not None:
        if current is not None and current.from_id is not None:
            new_from_id = current.from_id
            new_start_time_ms = None
        else:
            new_from_id = None
            new_start_time_ms = (
                max(current.start_time_ms, cursor.start_time_ms)
                if current is not None and current.start_time_ms is not None
                else cursor.start_time_ms
            )
    else:
        return None

    return FillCursor(from_id=new_from_id, start_time_ms=new_start_time_ms)


def plan_fill_polling_ranges(
    previous_fill_cursors: Mapping[str, FillCursor],
    tracked_fill_symbols: tuple[str, ...],
    newly_active_symbols: set[str],
    *,
    observed_at: datetime,
) -> tuple[dict[str, int], dict[str, int]]:
    """Plan ID and bounded time cursors without issuing private requests."""
    start_time_by_symbol = {
        symbol: cursor.start_time_ms
        for symbol, cursor in previous_fill_cursors.items()
        if (cursor.from_id is None and cursor.start_time_ms is not None)
    }
    from_id_by_symbol = {
        symbol: cursor.from_id
        for symbol, cursor in previous_fill_cursors.items()
        if (cursor.from_id is not None and symbol in tracked_fill_symbols)
    }
    new_position_start_at = int(
        (observed_at - _NEW_POSITION_FILL_LOOKBACK).timestamp() * 1000
    )
    for symbol in newly_active_symbols:
        if symbol not in previous_fill_cursors:
            start_time_by_symbol[symbol] = max(0, new_position_start_at)
    # Any remaining tracked symbol still has no positional cursor.
    # Bound it explicitly so userTrades never falls back to "latest
    # 1000 fills of every prior episode" for that symbol.
    historical_start_at = int(
        (observed_at - _HISTORICAL_FILL_LOOKBACK).timestamp() * 1000
    )
    for symbol in tracked_fill_symbols:
        if symbol in from_id_by_symbol:
            continue
        if symbol not in start_time_by_symbol:
            start_time_by_symbol[symbol] = historical_start_at
    return from_id_by_symbol, start_time_by_symbol
