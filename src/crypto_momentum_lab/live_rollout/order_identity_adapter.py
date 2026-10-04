"""Build current position facts without inventing exchange executions."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.position_batches import (
    _EXIT_SUBMITTED_STATES,
    PositionObservation,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
)


def build_position_account_facts(
    *,
    position_key: PositionKey,
    orders: Sequence[PositionOrderFact],
    fills: Sequence[AccountFillEvent] = (),
    observation: PositionObservation | None = None,
    coverage: FactCoverageInterval | None = None,
) -> AccountFacts:
    """Build position facts from actual fills, observations and exit submissions."""

    matching_order_ids = {
        str(oid)
        for order in orders
        for oid in (order.client_order_id, order.exchange_order_id)
        if oid
    }
    order_times = [order.created_at for order in orders if order.created_at]
    earliest_order_time = min(order_times) if order_times else None

    def _fill_matches_side(fill: AccountFillEvent) -> bool:
        if fill.symbol != position_key.symbol:
            return False
        payload = fill.raw_payload
        ps = payload.get("positionSide") or payload.get("ps")
        if ps:
            ps_str = str(ps).upper()
            if (
                ps_str != "BOTH"
                and ps_str != position_key.position_side.value.upper()
            ):
                return False
        if earliest_order_time is not None:
            if str(fill.order_id) in matching_order_ids:
                return True
            if fill.trade_at < earliest_order_time - timedelta(minutes=5):
                return False
        return True

    fill_list: list[AccountFillEvent] = [f for f in fills if _fill_matches_side(f)]
    exit_boundaries: list[ExitOrderSubmissionFact] = []
    for order in orders:
        if order.reduce_only and order.state in _EXIT_SUBMITTED_STATES:
            exit_boundaries.append(
                ExitOrderSubmissionFact(
                    order_id=order.exchange_order_id
                    or order.client_order_id
                    or f"exit_{order.created_at.timestamp()}",
                    submitted_at=order.created_at,
                    symbol=position_key.symbol,
                    position_side=position_key.position_side,
                    client_order_id=order.client_order_id,
                    target_batch_id=order.exit_batch_id,
                )
            )

    snapshots: list[AccountPositionSnapshot] = []
    if observation is not None and observation.observed_at is not None:
        snapshots.append(
            AccountPositionSnapshot(
                environment=position_key.environment,
                account_label=position_key.account_label,
                symbol=position_key.symbol,
                position_side=position_key.position_side.value,
                position_amt=observation.position_amt,
                entry_price=observation.entry_price,
                mark_price=observation.entry_price,
                unrealized_pnl=Decimal("0.0"),
                notional=observation.position_amt * observation.entry_price,
                leverage=None,
                margin_type=None,
                observed_at=observation.observed_at,
                raw_payload={},
            )
        )

    return AccountFacts(
        position_key=position_key,
        fills=tuple(fill_list),
        snapshots=tuple(snapshots),
        exit_boundaries=tuple(exit_boundaries),
        coverage=coverage,
    )
