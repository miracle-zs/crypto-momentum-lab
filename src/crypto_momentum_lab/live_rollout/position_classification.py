"""Classify live exchange positions into managed / pending / unmanaged."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    OrderObservation,
    PersistedExchangeOrder,
    PositionObservation,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_batches import PositionOrderFact
from crypto_momentum_lab.domain.execution.position_ledger_models import CoverageEvidence
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.order_facts_loader import (
    _resolve_symbol_fill_horizon,
)
from crypto_momentum_lab.live_rollout.position_batches import (
    _build_position_batches,
    _exit_fill_quantity,
    _is_entry_fill_observed,
    _order_entry_time,
    _position_order_key,
)

log = structlog.get_logger(__name__)

_PositionOrder = PositionOrderFact

_EXIT_SUBMITTED_STATES = frozenset(
    {
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.PARTIALLY_FILLED,
        ExchangeOrderState.FILLED,
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.ABSENT_RECONCILED,
        ExchangeOrderState.EXPIRED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }
)
_PENDING_ENTRY_STATES = frozenset(
    {
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }
)
_PENDING_POSITION_MAX_AGE_SECONDS = 60


def _classify_live_positions_detailed(
    positions: Sequence[AccountPositionSnapshot | PositionObservation],
    orders: list[OrderObservation],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
    *,
    environment: str,
    account_label: str,
    entry_fill_times: Mapping[str, datetime] | None = None,
    exit_batch_ids: Mapping[str, str] | None = None,
    account_fills: Sequence[AccountFillEvent] = (),
    since_time: datetime | None = None,
    coverage_by_symbol: Mapping[str, CoverageEvidence] | None = None,
    build_managed_positions: bool = True,
) -> tuple[
    tuple[ManagedLivePosition, ...],
    frozenset[str],
    frozenset[str],
]:
    resolved_since = since_time or _resolve_symbol_fill_horizon(orders, positions)
    fill_times = entry_fill_times or {}
    position_orders = _normalise_position_orders(orders, unresolved)
    if exit_batch_ids:
        position_orders = tuple(
            replace(
                order,
                exit_batch_id=(
                    None
                    if exit_batch_ids is None
                    else exit_batch_ids.get(order.client_order_id or "")
                ),
            )
            for order in position_orders
        )
    managed: list[ManagedLivePosition] = []
    pending: set[str] = set()
    unmanaged: set[str] = set()
    for position in positions:
        if position.position_amt == 0:
            continue
        try:
            position_side = FuturesPositionSide(position.position_side)
        except (TypeError, ValueError):
            unmanaged.add(position.symbol)
            continue
        side = _strategy_side(position, position_side)
        matching_orders = [
            order
            for order in position_orders
            if order.symbol == position.symbol and order.position_side is position_side
        ]
        opening_candidates = [
            order
            for order in matching_orders
            if not order.reduce_only
            and _opening_order_matches_side(order.side, side)
            and _is_entry_fill_observed(order, fill_times)
        ]
        opening = max(
            opening_candidates,
            key=lambda order: (
                _order_entry_time(order, fill_times),
                order.updated_at,
                order.created_at,
            ),
            default=None,
        )
        if opening is None or position.entry_price <= 0:
            if _has_recent_pending_entry_order(
                position,
                matching_orders,
                side=side,
            ):
                pending.add(position.symbol)
                continue
            unmanaged.add(position.symbol)
            continue
        opened_at = _order_entry_time(opening, fill_times)
        reduce_only_orders = [
            order
            for order in matching_orders
            if order.reduce_only and not _opening_order_matches_side(order.side, side)
        ]
        closing_filled_quantity = sum(
            (
                _exit_fill_quantity(order)
                for order in reduce_only_orders
                if order.created_at >= opened_at
            ),
            start=Decimal("0"),
        )
        closing_filled_strict = closing_filled_quantity == abs(position.position_amt)
        # Also handle residual snapshot lag during exit settlement (e.g. partial
        # snapshot updates arriving as the position drains from full quantity to zero).
        closing_filled_draining = False
        if not closing_filled_strict and (
            closing_filled_quantity > abs(position.position_amt)
        ):
            latest_closing_order = max(
                reduce_only_orders,
                key=lambda o: (o.updated_at, o.created_at),
                default=None,
            )
            observed_at = position.observed_at
            if isinstance(observed_at, datetime) and latest_closing_order is not None:
                # The snapshot was observed after the exit order was submitted
                # (allowing minor clock skew) and before settlement window expires.
                time_since_submission = (
                    observed_at - latest_closing_order.created_at
                ).total_seconds()
                time_since_fill = (
                    observed_at - latest_closing_order.updated_at
                ).total_seconds()
                if (
                    time_since_submission >= -1.0
                    and time_since_fill <= _PENDING_POSITION_MAX_AGE_SECONDS
                ):
                    closing_filled_draining = True
        closing_filled = closing_filled_strict or closing_filled_draining
        batches = (
            _build_position_batches(
                position=position,
                environment=environment,
                account_label=account_label,
                side=side,
                position_side=position_side,
                matching_orders=matching_orders,
                account_fills=account_fills,
                since_time=resolved_since,
                coverage_evidence=((coverage_by_symbol or {}).get(position.symbol)),
            )
            if build_managed_positions
            else ()
        )
        if not batches and not closing_filled:
            # The account snapshot can arrive before the new entry's order
            # state/fill metadata.  In that window the only confirmed batch
            # may be an older batch whose reduce-only exit already consumed
            # it.  Falling back to that accumulator would assign the current
            # position quantity and the old entry timestamp to the new lot,
            # which can trigger an immediate candle-timeout exit.  Keep the
            # symbol fail-closed until the next reconciliation observes the
            # new entry fill instead of inventing a batch boundary.
            if _has_recent_pending_entry_order(
                position,
                matching_orders,
                side=side,
            ):
                pending.add(position.symbol)
                continue
            unmanaged.add(position.symbol)
            continue
        aggregate_opened_at = max(
            (batch.opened_at for batch in batches),
            default=opened_at,
        )
        latest_recovery = max(
            (batch for batch in batches if batch.recovery_order_plan is not None),
            key=lambda batch: (
                batch.recovery_order_plan.created_at
                if batch.recovery_order_plan is not None
                else batch.opened_at
            ),
            default=None,
        )
        # Once multiple residual batches exist, the aggregate flag must stay
        # open so the exit manager can evaluate the batches independently.
        # A market order that is active for one batch is carried on that batch
        # view instead of suppressing every batch in the symbol aggregate.
        aggregate_closing_filled = closing_filled and not batches
        managed.append(
            ManagedLivePosition(
                account_label=account_label,
                symbol=position.symbol,
                side=side,
                position_side=position_side,
                quantity=abs(position.position_amt),
                entry_price=position.entry_price,
                opened_at=aggregate_opened_at,
                closing_order_filled=aggregate_closing_filled,
                recovery_order_client_id=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_client_id
                ),
                recovery_exit_started_at=(
                    None
                    if latest_recovery is None
                    else latest_recovery.exit_order_submitted_at
                ),
                recovery_order_created_at=(
                    None
                    if latest_recovery is None
                    or latest_recovery.recovery_order_plan is None
                    else latest_recovery.recovery_order_plan.created_at
                ),
                recovery_order_plan=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_plan
                ),
                recovery_order_remaining_quantity=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_remaining_quantity
                ),
                batch_id=(batches[0].batch_id if len(batches) == 1 else None),
                batches=batches,
                projection_version=(batches[0].projection_version if batches else None),
            )
        )
    return (
        tuple(sorted(managed, key=lambda item: (item.symbol, item.position_side))),
        frozenset(pending),
        frozenset(unmanaged),
    )


def _has_recent_pending_entry_order(
    position: AccountPositionSnapshot | PositionObservation,
    matching_orders: Sequence[_PositionOrder],
    *,
    side: StrategySide,
) -> bool:
    """Identify a bounded order-to-position visibility race.

    An account event can publish a new position before the order state or
    fill ledger transaction is visible to the strategy runtime. Only a
    recent, non-terminal entry order from this run qualifies as pending;
    unknown positions and stale orders remain fail-closed as unmanaged.
    """
    observed_at = position.observed_at
    for order in matching_orders:
        if (
            order.reduce_only
            or not _opening_order_matches_side(order.side, side)
            or order.state not in _PENDING_ENTRY_STATES
        ):
            continue
        pending_since = min(order.created_at, order.updated_at)
        age_seconds = (observed_at - pending_since).total_seconds()
        if 0 <= age_seconds <= _PENDING_POSITION_MAX_AGE_SECONDS:
            return True
    return False


def _normalise_position_orders(
    orders: Sequence[OrderObservation],
    unresolved: Sequence[PersistedExchangeOrder],
) -> tuple[_PositionOrder, ...]:
    unresolved_by_client_id = {item.plan.client_order_id: item for item in unresolved}
    normalised: list[_PositionOrder] = []
    seen_keys: set[str] = set()
    for row in orders:
        client_order_id = _optional_text(row.client_order_id)
        persisted = (
            None
            if client_order_id is None
            else unresolved_by_client_id.get(client_order_id)
        )
        order = _position_order_from_row(
            row,
            plan=None if persisted is None else persisted.plan,
            fallback_state=None if persisted is None else persisted.state,
            fallback_executed_quantity=(
                None if persisted is None else persisted.executed_quantity
            ),
        )
        if order is None:
            continue
        key = _position_order_key(order)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        normalised.append(order)
    for item in unresolved:
        order = _position_order_from_plan(item)
        key = _position_order_key(order)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        normalised.append(order)
    return tuple(normalised)


def _position_order_from_row(
    row: OrderObservation,
    *,
    plan: OrderExecutionPlan | None,
    fallback_state: ExchangeOrderState | None,
    fallback_executed_quantity: Decimal | None,
) -> _PositionOrder | None:
    try:
        position_side = FuturesPositionSide(row.position_side)
    except (TypeError, ValueError):
        return None
    if row.created_at is None or row.updated_at is None:
        return None
    state = _normalise_order_state(row.state, fallback=fallback_state)
    executed_quantity = _decimal_or_zero(row.executed_quantity)
    if fallback_executed_quantity is not None:
        executed_quantity = max(
            executed_quantity,
            _decimal_or_zero(fallback_executed_quantity),
        )
    quantity = max(_decimal_or_zero(row.quantity), executed_quantity)
    if quantity <= 0:
        return None
    price_value = (
        row.price if row.price is not None else None if plan is None else plan.price
    )
    return _PositionOrder(
        symbol=str(row.symbol),
        position_side=position_side,
        side=str(row.side).upper(),
        reduce_only=bool(row.reduce_only),
        order_type=str(row.order_type).upper(),
        quantity=quantity,
        executed_quantity=executed_quantity,
        state=state,
        client_order_id=_optional_text(row.client_order_id),
        exchange_order_id=_optional_text(row.exchange_order_id),
        created_at=row.created_at,
        updated_at=row.updated_at,
        price=None if price_value is None else _decimal_or_zero(price_value),
        plan=plan,
    )


def _position_order_from_plan(item: PersistedExchangeOrder) -> _PositionOrder:
    plan = item.plan
    return _PositionOrder(
        symbol=plan.symbol,
        position_side=FuturesPositionSide(plan.position_side),
        side=plan.side.upper(),
        reduce_only=plan.reduce_only,
        order_type=plan.order_type.upper(),
        quantity=plan.quantity,
        executed_quantity=max(Decimal("0"), item.executed_quantity),
        state=_normalise_order_state(item.state),
        client_order_id=plan.client_order_id,
        exchange_order_id=item.exchange_order_id,
        created_at=plan.created_at,
        updated_at=item.updated_at,
        price=plan.price,
        plan=plan,
    )


def _normalise_order_state(
    value: object,
    *,
    fallback: ExchangeOrderState | None = None,
) -> ExchangeOrderState:
    if isinstance(value, ExchangeOrderState):
        return value
    if value is not None:
        try:
            return ExchangeOrderState(str(value))
        except ValueError:
            pass
    return fallback or ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _decimal_or_zero(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _strategy_side(
    position: AccountPositionSnapshot | PositionObservation,
    position_side: FuturesPositionSide,
) -> StrategySide:
    if position_side is FuturesPositionSide.LONG:
        return StrategySide.LONG
    if position_side is FuturesPositionSide.SHORT:
        return StrategySide.SHORT
    return StrategySide.LONG if position.position_amt > 0 else StrategySide.SHORT


def _opening_order_matches_side(order_side: str, side: StrategySide) -> bool:
    return (order_side == "BUY") is (side is StrategySide.LONG)
