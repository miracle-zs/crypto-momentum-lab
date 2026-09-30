"""Legacy order identity adapters for PositionLedger.

Adapts legacy execution order facts and observations into AccountFacts
for authoritative PositionLedger projection.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
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


class LegacyOrderIdentityAdapter:
    """Adapts legacy execution order facts and observations into AccountFacts."""

    @staticmethod
    def to_account_facts(
        *,
        position_key: PositionKey,
        orders: Sequence[PositionOrderFact],
        fills: Sequence[AccountFillEvent] = (),
        observation: PositionObservation | None = None,
        coverage: FactCoverageInterval | None = None,
        fill_times: Mapping[str, datetime] | None = None,
        fill_prices: Mapping[str, Decimal] | None = None,
    ) -> AccountFacts:
        """Construct normalized AccountFacts from legacy inputs.

        If explicit fills are not available, synthetic fills are synthesized
        from filled PositionOrderFact entries to allow backward compatibility.
        """

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
        has_synthetic = False

        if not fill_list and orders:
            has_synthetic = True
            for ord_idx, order in enumerate(orders):
                oid = order.exchange_order_id or order.client_order_id or "unknown"
                fill_dt = (
                    (fill_times.get(oid) if fill_times else None)
                    or (
                        fill_times.get(order.exchange_order_id)
                        if fill_times and order.exchange_order_id
                        else None
                    )
                    or (
                        fill_times.get(order.client_order_id)
                        if fill_times and order.client_order_id
                        else None
                    )
                    or order.created_at
                )
                has_observed_fill = (
                    order.executed_quantity > 0
                    or (
                        getattr(order, "state", None)
                        in {
                            ExchangeOrderState.PARTIALLY_FILLED,
                            ExchangeOrderState.FILLED,
                        }
                    )
                    or (
                        fill_times is not None
                        and any(
                            k in fill_times
                            for k in (
                                oid,
                                order.exchange_order_id,
                                order.client_order_id,
                            )
                            if k
                        )
                    )
                )
                if has_observed_fill:
                    qty = (
                        order.executed_quantity
                        if order.executed_quantity > 0
                        else order.quantity
                    )
                    prc = (
                        order.price
                        or (fill_prices.get(oid) if fill_prices else None)
                        or (
                            fill_prices.get(order.exchange_order_id)
                            if fill_prices and order.exchange_order_id
                            else None
                        )
                        or (
                            fill_prices.get(order.client_order_id)
                            if fill_prices and order.client_order_id
                            else None
                        )
                        or (
                            observation.entry_price
                            if observation and observation.entry_price > 0
                            else Decimal("1.0")
                        )
                    )
                    pos_side_val = getattr(order, "position_side", None)
                    if pos_side_val is None:
                        pos_side_val = position_key.position_side.value
                    elif isinstance(pos_side_val, FuturesPositionSide):
                        pos_side_val = pos_side_val.value
                    else:
                        pos_side_val = str(pos_side_val)
                    fill_list.append(
                        AccountFillEvent(
                            environment=position_key.environment,
                            account_label=position_key.account_label,
                            symbol=position_key.symbol,
                            trade_id=f"syn_t_{ord_idx}_{oid}",
                            order_id=oid if oid != "unknown" else f"ord_{ord_idx}",
                            side=order.side,
                            price=prc,
                            quantity=qty,
                            realized_pnl=Decimal("0.0"),
                            fee=Decimal("0.0"),
                            fee_asset="USDT",
                            trade_at=fill_dt,
                            raw_payload={
                                "synthetic_from_order": True,
                                "is_system": True,
                                "client_order_id": order.client_order_id,
                                "reduce_only": order.reduce_only,
                                "positionSide": pos_side_val,
                            },
                        )
                    )

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
            has_synthetic_fills=has_synthetic,
        )
