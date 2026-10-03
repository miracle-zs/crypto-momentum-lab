"""Build current ledger facts without inferring synthetic exchange fills."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
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
    AccountFacts,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    PositionKey,
)


class PositionFactBuilder:
    """Build ledger facts from explicit exchange fills and observations."""

    @staticmethod
    def to_account_facts(
        *,
        position_key: PositionKey,
        orders: Sequence[PositionOrderFact],
        fills: Sequence[AccountFillEvent],
        observation: PositionObservation | None = None,
        coverage: FactCoverageInterval | None = None,
    ) -> AccountFacts:
        matching_order_ids = {
            str(oid)
            for order in orders
            for oid in (order.client_order_id, order.exchange_order_id)
            if oid
        }
        order_times = [
            order.created_at for order in orders if getattr(order, "created_at", None)
        ]
        earliest_order_time = min(order_times) if order_times else None

        def _fill_matches_side(fill: AccountFillEvent) -> bool:
            if fill.symbol != position_key.symbol:
                return False
            payload = fill.raw_payload or {}
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
        if observation is not None:
            now_dt = datetime.now(UTC)
            obs_time = (
                observation.observed_at
                if getattr(observation, "observed_at", None) is not None
                else (orders[-1].updated_at if orders else now_dt)
            )
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
                    observed_at=obs_time or datetime.now(UTC),
                    raw_payload={},
                )
            )

        return AccountFacts(
            position_key=position_key,
            fills=tuple(fill_list),
            snapshots=tuple(snapshots),
            exit_boundaries=tuple(exit_boundaries),
            coverage=coverage,
            has_synthetic_fills=False,
        )
