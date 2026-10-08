"""Build managed live position batches from order facts and fills."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_batches import (
    ManagedLivePositionBatch,
    PositionObservation,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    CoverageEvidence,
    PositionKey,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.order_identity_adapter import (
    build_position_account_facts,
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


def _build_position_batches(
    *,
    position: AccountPositionSnapshot | PositionObservation,
    environment: str,
    account_label: str,
    side: StrategySide,
    position_side: FuturesPositionSide,
    matching_orders: Sequence[_PositionOrder],
    account_fills: Sequence[AccountFillEvent] = (),
    since_time: datetime | None = None,
    coverage_evidence: CoverageEvidence | None = None,
) -> tuple[ManagedLivePositionBatch, ...]:
    observation = PositionObservation(
        symbol=position.symbol,
        side=side,
        position_side=position_side,
        position_amt=position.position_amt,
        entry_price=position.entry_price,
        observed_at=position.observed_at,
    )
    # Authoritative PositionLedger projection: builds primary batches
    # with zero-gap reconciliation.
    try:
        position_key = PositionKey(
            environment=environment,
            account_label=account_label,
            symbol=position.symbol,
            position_side=position_side,
        )

        def _fill_matches_position(fill: AccountFillEvent) -> bool:
            if fill.symbol != position.symbol:
                return False
            payload = fill.raw_payload
            fill_ps = payload.get("positionSide") or payload.get("ps")
            if fill_ps:
                fill_ps_str = str(fill_ps).upper()
                pos_ps_str = position_side.value.upper()
                if fill_ps_str != "BOTH" and fill_ps_str != pos_ps_str:
                    return False
            return True

        matching_fills = tuple(
            fill for fill in account_fills if _fill_matches_position(fill)
        )
        coverage = None
        if since_time is not None and position.observed_at is not None:
            obs_dt = position.observed_at
            if obs_dt < since_time:
                obs_dt = since_time
            # Only cursor+checkpoint evidence can confirm completeness.
            # Non-empty attributes on the snapshot are not proof.
            coverage = compose_fact_coverage(
                coverage_evidence,
                start=since_time,
                end=obs_dt,
            )
        facts = build_position_account_facts(
            position_key=position_key,
            orders=matching_orders,
            fills=matching_fills,
            observation=observation,
            coverage=coverage,
        )
        ledger = PositionLedger(position_key)
        projection = ledger.project(facts)
        if projection.unallocated_quantity > 0:
            return ()

        active_limit_orders = [
            order
            for order in matching_orders
            if order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "LIMIT"
            and not order.state.terminal
        ]
        active_market_orders = any(
            order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "MARKET"
            and not order.state.terminal
            for order in matching_orders
        )
        recovery_order = max(
            active_limit_orders,
            key=lambda order: (order.created_at, order.updated_at),
            default=None,
        )
        recovery_remaining = None
        if recovery_order is not None and recovery_order.plan is not None:
            recovery_remaining = max(
                Decimal("0"),
                recovery_order.plan.quantity - recovery_order.executed_quantity,
            )

        ledger_batches_list: list[ManagedLivePositionBatch] = []
        if projection.active_batches:
            for ab in projection.active_batches:
                ledger_batches_list.append(
                    ManagedLivePositionBatch(
                        batch_id=ab.batch_id,
                        quantity=ab.quantity,
                        entry_price=ab.entry_price,
                        opened_at=ab.opened_at,
                        exit_order_submitted_at=(
                            ab.exit_order_submitted_at
                            if ab.exit_order_submitted_at is not None
                            else (
                                recovery_order.created_at
                                if recovery_order is not None
                                else None
                            )
                        ),
                        recovery_order_client_id=(
                            None
                            if recovery_order is None or recovery_order.plan is None
                            else recovery_order.plan.client_order_id
                        ),
                        recovery_order_plan=(
                            None if recovery_order is None else recovery_order.plan
                        ),
                        recovery_order_remaining_quantity=recovery_remaining,
                        closing_order_filled=active_market_orders,
                        entry_order_count=max(1, len(ab.entry_order_ids)),
                        entry_order_count_proven=bool(ab.entry_order_ids),
                        entry_client_order_ids=frozenset(ab.entry_client_order_ids),
                        entry_exchange_order_ids=frozenset(ab.entry_order_ids),
                        projection_version=projection.projection_version,
                    )
                )
        ledger_batches = tuple(ledger_batches_list)
        log.info(
            "position_ledger_primary_active",
            symbol=position.symbol,
            batch_count=len(ledger_batches),
            total_quantity=str(sum((b.quantity for b in ledger_batches), Decimal("0"))),
        )
        return ledger_batches
    except Exception as exc:
        log.error(
            "authoritative_position_ledger_failed",
            symbol=position.symbol,
            error=str(exc),
        )
        # Fail closed: do not synthesize batches after the authoritative
        # projection failed.
        err_msg = (
            f"Authoritative PositionLedger projection failed for "
            f"{position.symbol}: {exc}"
        )
        raise RuntimeError(err_msg) from exc


def _is_entry_fill_observed(
    order: _PositionOrder,
    fill_times: Mapping[str, datetime],
) -> bool:
    return (
        order.state
        in {
            ExchangeOrderState.PARTIALLY_FILLED,
            ExchangeOrderState.FILLED,
        }
        or _entry_fill_at(order, fill_times) is not None
        or order.executed_quantity > 0
    )


def _order_entry_time(
    order: _PositionOrder,
    fill_times: Mapping[str, datetime],
) -> datetime:
    return _entry_fill_at(order, fill_times) or order.updated_at


def _position_order_key(order: _PositionOrder) -> str:
    if order.exchange_order_id is not None:
        return f"exchange:{order.exchange_order_id}"
    if order.client_order_id is not None:
        return f"client:{order.client_order_id}"
    return (
        f"anonymous:{order.symbol}:{order.position_side.value}:"
        f"{order.side}:{int(order.reduce_only)}:{order.order_type}:"
        f"{order.created_at.isoformat()}:{order.quantity}"
    )


def _exit_fill_quantity(order: _PositionOrder) -> Decimal:
    if order.state not in _EXIT_SUBMITTED_STATES:
        return Decimal("0")
    if order.executed_quantity > 0:
        return order.executed_quantity
    return order.quantity if order.state is ExchangeOrderState.FILLED else Decimal("0")


def _entry_fill_at(
    order: object | None,
    fill_times: Mapping[str, datetime],
) -> datetime | None:
    if order is None:
        return None
    for identifier in (
        order.exchange_order_id,
        order.client_order_id,
    ):
        if identifier is not None:
            fill_at = fill_times.get(identifier)
            if fill_at is not None:
                return fill_at
    return None


def _record_earliest_fill(
    fill_times: dict[str, datetime],
    identifier: str | None,
    filled_at: datetime,
) -> None:
    if identifier is None:
        return
    previous = fill_times.get(identifier)
    if previous is None or filled_at < previous:
        fill_times[identifier] = filled_at
