"""Bounded Postgres order-history reads used to rebuild live positions."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    ExchangeOrderRow,
    OrderIntentExecutionRow,
)

log = structlog.get_logger(__name__)

# Phase-1 scans only this far back when hunting for still-open lot anchors.
# Live timeout exits should close far sooner; a longer hold falls back to this
# bound rather than an unbounded order history.
_ORDER_ANCHOR_LOOKBACK = timedelta(days=7)
# Keep a short cushion before the earliest open lot so entry metadata written
# slightly before the fill timestamp is still visible to batch rebuild.
_ORDER_ANCHOR_BUFFER = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class _OrderAnchorEvent:
    symbol: str
    kind: str
    occurred_at: datetime
    quantity: Decimal


def _opening_anchors_from_events(
    events: Sequence[_OrderAnchorEvent],
    symbols: Sequence[str],
) -> dict[str, datetime]:
    """Return the earliest still-open lot time per symbol via a FIFO walk.

    Batch attribution can differ from pure FIFO under named bindings, so this
    window errs toward keeping more history rather than dropping a live lot.
    Callers still rely on the fill-vs-opened_at invariant for correctness.
    """
    by_symbol: dict[str, list[_OrderAnchorEvent]] = {
        symbol.strip().upper(): [] for symbol in symbols
    }
    for event in events:
        key = event.symbol.strip().upper()
        by_symbol.setdefault(key, []).append(event)

    anchors: dict[str, datetime] = {}
    for symbol, symbol_events in by_symbol.items():
        ordered = sorted(symbol_events, key=lambda item: item.occurred_at)
        open_lots: list[list[object]] = []
        for event in ordered:
            if event.quantity <= 0:
                continue
            if event.kind == "entry":
                open_lots.append([event.occurred_at, event.quantity])
                continue
            remaining = event.quantity
            for lot in open_lots:
                if remaining <= 0:
                    break
                lot_remaining = lot[1]
                if not isinstance(lot_remaining, Decimal) or lot_remaining <= 0:
                    continue
                take = min(lot_remaining, remaining)
                lot[1] = lot_remaining - take
                remaining -= take
            open_lots = [
                lot for lot in open_lots if isinstance(lot[1], Decimal) and lot[1] > 0
            ]
        if open_lots:
            opened_times = [lot[0] for lot in open_lots if isinstance(lot[0], datetime)]
            if opened_times:
                anchors[symbol] = min(opened_times)
    return anchors


_TERMINAL_ORDER_STATES = frozenset(
    {
        ExchangeOrderState.FILLED.value,
        ExchangeOrderState.CANCELED.value,
        ExchangeOrderState.ABSENT_RECONCILED.value,
        ExchangeOrderState.REJECTED.value,
        ExchangeOrderState.EXPIRED.value,
        ExchangeOrderState.SUPPRESSED.value,
    }
)


def _lookup_zero_at(
    zero_crossing_times: Mapping[Any, datetime] | None,
    symbol: str,
    position_side: str | None = None,
) -> datetime | None:
    if not zero_crossing_times:
        return None
    sym = symbol.strip().upper()
    pos_side = (position_side or "BOTH").strip().upper()
    if (sym, pos_side) in zero_crossing_times:
        return zero_crossing_times[(sym, pos_side)]
    val = zero_crossing_times.get(sym)
    if isinstance(val, datetime):
        return val
    return None


def _is_pre_zero_order(
    row: ExchangeOrderRow | object,
    zero_at: datetime | None,
) -> bool:
    if zero_at is None:
        return False
    state = getattr(row, "state", None)
    is_terminal = state in _TERMINAL_ORDER_STATES
    created_at = getattr(row, "created_at", None)
    updated_at = getattr(row, "updated_at", created_at) or created_at
    if created_at is None or updated_at is None:
        return False
    return bool(created_at < zero_at and updated_at < zero_at and is_terminal)


async def _load_order_anchor_events(
    session: AsyncSession,
    *,
    run_id: str,
    active_symbols: Sequence[str],
    lookback_start: datetime,
    zero_crossing_times: Mapping[Any, datetime] | None = None,
) -> tuple[tuple[_OrderAnchorEvent, ...], Mapping[str, datetime]]:
    rows = (
        await session.scalars(
            select(ExchangeOrderRow)
            .where(
                ExchangeOrderRow.run_id == run_id,
                ExchangeOrderRow.symbol.in_(active_symbols),
                ExchangeOrderRow.created_at >= lookback_start,
            )
            .order_by(ExchangeOrderRow.created_at.asc())
        )
    ).all()
    events: list[_OrderAnchorEvent] = []
    latest_entry_times: dict[str, datetime] = {}
    for row in rows:
        symbol_key = row.symbol.strip().upper()
        pos_side = getattr(row, "position_side", None) or "BOTH"
        zero_at = _lookup_zero_at(zero_crossing_times, symbol_key, pos_side)
        if _is_pre_zero_order(row, zero_at):
            continue
        executed = row.executed_quantity or Decimal("0")
        if row.reduce_only:
            quantity = executed
            if quantity <= 0 and row.state == ExchangeOrderState.FILLED.value:
                quantity = row.quantity
            if quantity > 0:
                events.append(
                    _OrderAnchorEvent(
                        symbol=row.symbol,
                        kind="exit",
                        occurred_at=row.created_at,
                        quantity=quantity,
                    )
                )
            continue
        if (
            symbol_key not in latest_entry_times
            or row.created_at > latest_entry_times[symbol_key]
        ):
            latest_entry_times[symbol_key] = row.created_at
        quantity = executed
        if quantity <= 0 and row.state == ExchangeOrderState.FILLED.value:
            quantity = row.quantity
        if quantity > 0:
            events.append(
                _OrderAnchorEvent(
                    symbol=row.symbol,
                    kind="entry",
                    occurred_at=row.created_at,
                    quantity=quantity,
                )
            )
    return tuple(events), latest_entry_times


async def load_position_orders_bounded(
    session: AsyncSession,
    *,
    run_id: str,
    active_symbols: Sequence[str],
    now: datetime | None = None,
    account_label: str | None = None,
) -> list[ExchangeOrderRow]:
    """Two-phase order load: anchors first, then a per-symbol time window."""
    if not active_symbols:
        return []
    observed_at = now or datetime.now(tz=UTC)
    lookback_start = observed_at - _ORDER_ANCHOR_LOOKBACK
    symbols = tuple(sorted({symbol.strip().upper() for symbol in active_symbols}))

    zero_crossing_times: dict[tuple[str, str], datetime] = {}
    if account_label:
        zero_rows = (
            await session.execute(
                select(
                    AccountPositionSnapshotRow.symbol,
                    AccountPositionSnapshotRow.position_side,
                    func.max(AccountPositionSnapshotRow.observed_at),
                )
                .where(
                    AccountPositionSnapshotRow.environment == "live",
                    AccountPositionSnapshotRow.account_label == account_label,
                    AccountPositionSnapshotRow.symbol.in_(symbols),
                    AccountPositionSnapshotRow.position_amt == 0,
                    AccountPositionSnapshotRow.observed_at <= observed_at,
                )
                .group_by(
                    AccountPositionSnapshotRow.symbol,
                    AccountPositionSnapshotRow.position_side,
                )
            )
        ).all()
        for sym, pos_side, zero_at in zero_rows:
            if zero_at is not None:
                zero_crossing_times[
                    (sym.strip().upper(), (pos_side or "BOTH").strip().upper())
                ] = zero_at

    events, latest_entry_times = await _load_order_anchor_events(
        session,
        run_id=run_id,
        active_symbols=symbols,
        lookback_start=lookback_start,
        zero_crossing_times=zero_crossing_times,
    )
    anchors = _opening_anchors_from_events(events, symbols)
    window_conditions = []
    for symbol in symbols:
        anchor = anchors.get(symbol)
        if anchor is not None:
            window_start = anchor - _ORDER_ANCHOR_BUFFER
        elif symbol in latest_entry_times:
            # All historical lots in events are fully exited.
            # Anchor to the current episode's entry instead of 7 days ago.
            window_start = latest_entry_times[symbol] - _ORDER_ANCHOR_BUFFER
        else:
            window_start = lookback_start

        window_conditions.append(
            and_(
                ExchangeOrderRow.symbol == symbol,
                or_(
                    ExchangeOrderRow.created_at >= window_start,
                    ExchangeOrderRow.updated_at >= window_start,
                    ExchangeOrderRow.state.not_in(_TERMINAL_ORDER_STATES),
                ),
            )
        )
    if not window_conditions:
        return []
    rows = (
        await session.scalars(
            select(ExchangeOrderRow)
            .where(
                ExchangeOrderRow.run_id == run_id,
                or_(*window_conditions),
            )
            .order_by(ExchangeOrderRow.updated_at.desc())
            .limit(1000)
        )
    ).all()

    row_list: list[ExchangeOrderRow] = []
    for row in rows:
        sym = row.symbol.strip().upper()
        pos_side = getattr(row, "position_side", None) or "BOTH"
        zero_at = _lookup_zero_at(zero_crossing_times, sym, pos_side)
        if _is_pre_zero_order(row, zero_at):
            continue
        row_list.append(row)

    loaded_client_ids = {row.client_order_id for row in row_list if row.client_order_id}
    exit_intent_ids = tuple(
        row.intent_id for row in row_list if row.reduce_only and row.intent_id
    )
    if exit_intent_ids:
        intent_rows = (
            await session.execute(
                select(
                    OrderIntentExecutionRow.intent_id,
                    OrderIntentExecutionRow.details,
                ).where(OrderIntentExecutionRow.intent_id.in_(exit_intent_ids))
            )
        ).all()
        missing_entry_client_ids: set[str] = set()
        for _intent_id, details in intent_rows:
            features = details.get("features", {}) if isinstance(details, dict) else {}
            batch_id = features.get("batch_id") if isinstance(features, dict) else None
            if isinstance(batch_id, str) and batch_id:
                target_client_id = batch_id.split(":")[-1]
                if target_client_id and target_client_id not in loaded_client_ids:
                    missing_entry_client_ids.add(target_client_id)
        if missing_entry_client_ids:
            missing_entry_rows = (
                await session.scalars(
                    select(ExchangeOrderRow).where(
                        ExchangeOrderRow.run_id == run_id,
                        ExchangeOrderRow.client_order_id.in_(
                            tuple(missing_entry_client_ids)
                        ),
                    )
                )
            ).all()
            for extra_row in missing_entry_rows:
                sym = extra_row.symbol.strip().upper()
                pos_side = getattr(extra_row, "position_side", None) or "BOTH"
                zero_at = _lookup_zero_at(zero_crossing_times, sym, pos_side)
                if not _is_pre_zero_order(extra_row, zero_at):
                    row_list.append(extra_row)
                    loaded_client_ids.add(extra_row.client_order_id)

    for symbol, anchor in anchors.items():
        log.debug(
            "position_order_window_bounded",
            symbol=symbol,
            opened_at=anchor.isoformat(),
            window_start=(anchor - _ORDER_ANCHOR_BUFFER).isoformat(),
            row_count=len(row_list),
        )
    return row_list
