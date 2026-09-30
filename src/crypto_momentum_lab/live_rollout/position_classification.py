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
    OrderIdentityEvent,
    OrderObservation,
    PersistedExchangeOrder,
    PositionObservation,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_batches import PositionOrderFact
from crypto_momentum_lab.domain.execution.position_ledger_models import CoverageEvidence
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.order_facts_loader import (
    _resolve_symbol_fill_horizon,
)
from crypto_momentum_lab.live_rollout.order_identity import (
    _expand_legacy_order_row,
    _legacy_order_identity_is_ambiguous,
    _legacy_order_identity_is_reconstructible,
    _legacy_order_identity_is_zero_fill_terminal,
    _optional_text,
    _position_order_from_plan,
    _position_order_from_row,
)
from crypto_momentum_lab.live_rollout.position_batches import (
    _batch_id_for_entry,
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


def _classify_live_positions(
    positions: Sequence[AccountPositionSnapshot | PositionObservation],
    orders: list[OrderObservation],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
    *,
    entry_fill_times: Mapping[str, datetime] | None = None,
    entry_fill_prices: Mapping[str, Decimal] | None = None,
    exit_batch_ids: Mapping[str, str] | None = None,
    order_identity_events: Mapping[
        str,
        Sequence[OrderIdentityEvent],
    ]
    | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
    coverage_by_symbol: Mapping[str, CoverageEvidence] | None = None,
) -> tuple[tuple[ManagedLivePosition, ...], frozenset[str]]:
    """Keep the historical two-value classification API for callers/tests."""
    managed, _pending, unmanaged = _classify_live_positions_detailed(
        positions,
        orders,
        unresolved,
        entry_fill_times=entry_fill_times,
        entry_fill_prices=entry_fill_prices,
        exit_batch_ids=exit_batch_ids,
        order_identity_events=order_identity_events,
        account_fill_quantities=account_fill_quantities,
        coverage_by_symbol=coverage_by_symbol,
    )
    return managed, unmanaged


def _classify_live_positions_detailed(
    positions: Sequence[AccountPositionSnapshot | PositionObservation],
    orders: list[OrderObservation],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
    *,
    entry_fill_times: Mapping[str, datetime] | None = None,
    entry_fill_prices: Mapping[str, Decimal] | None = None,
    exit_batch_ids: Mapping[str, str] | None = None,
    order_identity_events: Mapping[
        str,
        Sequence[OrderIdentityEvent],
    ]
    | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
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
    fill_prices = entry_fill_prices or {}
    identity_events = order_identity_events or {}
    fill_quantities = account_fill_quantities or {}
    ambiguous_identity_ids = frozenset(
        client_order_id
        for client_order_id, events in identity_events.items()
        if _legacy_order_identity_is_ambiguous(events)
    )
    reconstructible_identity_ids = frozenset(
        client_order_id
        for client_order_id in ambiguous_identity_ids
        if _legacy_order_identity_is_reconstructible(
            identity_events[client_order_id],
            fill_quantities,
        )
    )
    zero_fill_terminal_identity_ids = frozenset(
        client_order_id
        for client_order_id in ambiguous_identity_ids
        if _legacy_order_identity_is_zero_fill_terminal(
            identity_events[client_order_id],
            fill_quantities,
        )
    )
    unresolved_identity_ids = frozenset(
        client_order_id
        for client_order_id in ambiguous_identity_ids
        if client_order_id
        not in reconstructible_identity_ids | zero_fill_terminal_identity_ids
    )
    if ambiguous_identity_ids:
        log.warning(
            "live_legacy_order_identity_conflict",
            ambiguous_client_order_ids=sorted(ambiguous_identity_ids),
            reconstructed_client_order_ids=sorted(reconstructible_identity_ids),
            zero_fill_terminal_client_order_ids=sorted(zero_fill_terminal_identity_ids),
            unresolved_client_order_ids=sorted(unresolved_identity_ids),
        )
    position_orders = _normalise_position_orders(
        orders,
        unresolved,
        order_identity_events=identity_events,
        account_fill_quantities=fill_quantities,
    )
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
    position_orders, binding_unresolved_identity_ids = (
        _repair_legacy_exit_batch_bindings(
            position_orders,
            identity_events=identity_events,
            account_fill_quantities=fill_quantities,
            fill_times=fill_times,
        )
    )
    blocked_identity_ids = unresolved_identity_ids | binding_unresolved_identity_ids
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
        if any(
            order.client_order_id in blocked_identity_ids for order in matching_orders
        ):
            log.critical(
                "live_position_batch_attribution_blocked",
                symbol=position.symbol,
                account_label=getattr(position, "account_label", None),
                ambiguous_client_order_ids=sorted(
                    {
                        order.client_order_id
                        for order in matching_orders
                        if order.client_order_id in blocked_identity_ids
                    }
                ),
                reason="legacy_order_identity_not_reconstructible",
            )
            unmanaged.add(position.symbol)
            continue
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
                fill_times,
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
            observed_at = getattr(position, "observed_at", None)
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
                side=side,
                position_side=position_side,
                matching_orders=matching_orders,
                fill_times=fill_times,
                fill_prices=fill_prices,
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
                fill_times,
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
    fill_times: Mapping[str, datetime],
    *,
    side: StrategySide,
) -> bool:
    """Identify a bounded order-to-position visibility race.

    An account event can publish a new position before the order state or
    fill ledger transaction is visible to the strategy runtime. Only a
    recent, non-terminal entry order from this run qualifies as pending;
    unknown positions and stale orders remain fail-closed as unmanaged.
    """
    observed_at = getattr(position, "observed_at", None)
    if not isinstance(observed_at, datetime):
        return False
    for order in matching_orders:
        if (
            order.reduce_only
            or not _opening_order_matches_side(order.side, side)
            or order.state not in _PENDING_ENTRY_STATES
        ):
            continue
        try:
            pending_since = min(order.created_at, order.updated_at)
            age_seconds = (observed_at - pending_since).total_seconds()
        except TypeError:
            return False
        if 0 <= age_seconds <= _PENDING_POSITION_MAX_AGE_SECONDS:
            return True
    return False


def _normalise_position_orders(
    orders: Sequence[OrderObservation],
    unresolved: Sequence[PersistedExchangeOrder],
    *,
    order_identity_events: Mapping[
        str,
        Sequence[OrderIdentityEvent],
    ]
    | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
) -> tuple[_PositionOrder, ...]:
    unresolved_by_client_id = {item.plan.client_order_id: item for item in unresolved}
    normalised: list[_PositionOrder] = []
    seen_keys: set[str] = set()
    for row in orders:
        client_order_id = _optional_text(getattr(row, "client_order_id", None))
        persisted = (
            None
            if client_order_id is None
            else unresolved_by_client_id.get(client_order_id)
        )
        legacy_events = (
            ()
            if client_order_id is None or order_identity_events is None
            else order_identity_events.get(client_order_id, ())
        )
        expanded_orders = _expand_legacy_order_row(
            row,
            plan=None if persisted is None else persisted.plan,
            fallback_state=None if persisted is None else persisted.state,
            fallback_executed_quantity=(
                None if persisted is None else persisted.executed_quantity
            ),
            events=legacy_events,
            account_fill_quantities=account_fill_quantities or {},
        )
        if expanded_orders is not None:
            for expanded in expanded_orders:
                key = _position_order_key(expanded)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                normalised.append(expanded)
            continue
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


def _repair_legacy_exit_batch_bindings(
    orders: Sequence[_PositionOrder],
    *,
    identity_events: Mapping[
        str,
        Sequence[OrderIdentityEvent],
    ],
    account_fill_quantities: Mapping[str, Decimal],
    fill_times: Mapping[str, datetime],
) -> tuple[tuple[_PositionOrder, ...], frozenset[str]]:
    """Replace stale legacy exit bindings with the nearest prior entry.

    A reused client ID can carry an old ``batch_id`` in
    ``order_intent_executions``.  Once the exchange attempts are split, the
    attempt timestamp gives us a stronger identity boundary than that stale
    metadata: a reduce-only SELL belongs to the latest filled LONG entry
    before that attempt (and vice versa).  If that boundary cannot be proven,
    fail closed for the affected client ID instead of retaining a wrong lot.
    """

    reconstructible_ids = frozenset(
        client_order_id
        for client_order_id, events in identity_events.items()
        if _legacy_order_identity_is_ambiguous(events)
        and _legacy_order_identity_is_reconstructible(
            events,
            account_fill_quantities,
        )
    )
    if not reconstructible_ids:
        return tuple(orders), frozenset()
    entry_orders = tuple(
        order
        for order in orders
        if not order.reduce_only and _is_entry_fill_observed(order, fill_times)
    )
    repaired: list[_PositionOrder] = []
    unresolved: set[str] = set()
    for order in orders:
        client_order_id = order.client_order_id
        if not order.reduce_only or client_order_id not in reconstructible_ids:
            repaired.append(order)
            continue
        exit_side = StrategySide.LONG if order.side == "SELL" else StrategySide.SHORT
        candidates = [
            entry
            for entry in entry_orders
            if entry.symbol == order.symbol
            and entry.position_side is order.position_side
            and _opening_order_matches_side(entry.side, exit_side)
            and _order_entry_time(entry, fill_times) <= order.created_at
        ]
        if not candidates:
            unresolved.add(client_order_id)
            repaired.append(order)
            continue
        target = max(
            candidates,
            key=lambda entry: (
                _order_entry_time(entry, fill_times),
                entry.updated_at,
                entry.created_at,
            ),
        )
        repaired.append(
            replace(
                order,
                exit_batch_id=_batch_id_for_entry(target),
            )
        )
    return tuple(repaired), frozenset(unresolved)


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


def _filled_order_quantity(order: object) -> Decimal:
    executed_quantity = getattr(order, "executed_quantity", None)
    if executed_quantity is not None:
        try:
            executed = Decimal(str(executed_quantity))
        except (ArithmeticError, TypeError, ValueError):
            executed = Decimal("0")
        if executed > 0:
            return executed

    # FILLED rows written before cumulative executed_quantity was persisted
    # still carry the planned quantity, which is the safest fallback for the
    # account-sync-lag suppression path.
    quantity = getattr(order, "quantity", None)
    if quantity is None:
        return Decimal("0")
    try:
        return max(Decimal("0"), Decimal(str(quantity)))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")
