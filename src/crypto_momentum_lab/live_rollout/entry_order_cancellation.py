"""Safe cancellation of scheduled live opening orders."""

from collections.abc import Collection
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.account import AccountOpenOrderSnapshot
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)


class OpenOrderExchange(Protocol):
    async def fetch_open_orders(self) -> Collection[AccountOpenOrderSnapshot]: ...


class EntryOrderCancellationRepository(Protocol):
    async def adopt_external_order_for_cancellation(
        self,
        plan: OrderExecutionPlan,
        *,
        exchange_order_id: str,
        observed_at: datetime,
    ) -> None: ...


def external_open_order_cancellation_plan(
    order: AccountOpenOrderSnapshot,
    *,
    run_id: str,
) -> OrderExecutionPlan:
    """Build a durable local plan for an exchange-visible orphan order."""

    remaining_quantity = order.original_quantity - order.executed_quantity
    if remaining_quantity <= 0:
        raise ValueError(
            "exchange-visible open order has no remaining quantity: "
            f"{order.client_order_id}"
        )
    raw_position_side = order.raw_payload.get("positionSide")
    if raw_position_side is None:
        position_side = FuturesPositionSide.BOTH
    else:
        try:
            position_side = FuturesPositionSide(str(raw_position_side).upper())
        except ValueError as error:
            raise ValueError(
                "exchange-visible open order has unsupported position side: "
                f"{order.client_order_id}"
            ) from error
    raw_time_in_force = order.raw_payload.get("timeInForce")
    time_in_force = None
    if order.order_type.upper() == "LIMIT" and isinstance(
        raw_time_in_force,
        str,
    ):
        normalized_time_in_force = raw_time_in_force.upper()
        if normalized_time_in_force in {"GTC", "IOC", "FOK", "GTX", "GTD", "RPI"}:
            time_in_force = normalized_time_in_force
    return OrderExecutionPlan(
        intent_id=f"orphan-cancel:{order.client_order_id}",
        run_id=run_id,
        client_order_id=order.client_order_id,
        symbol=order.symbol,
        side=order.side.upper(),
        order_type=order.order_type.upper(),
        quantity=remaining_quantity,
        price=order.price if order.price > 0 else None,
        reduce_only=False,
        created_at=order.observed_at,
        position_side=position_side,
        quantized=True,
        time_in_force=time_in_force,
    )


class LiveEntryOrderCanceller:
    """Cancel known and exchange-visible opening orders through the state machine."""

    def __init__(
        self,
        *,
        exchange: OpenOrderExchange,
        state_machine: OrderExecutionPort,
        repository: EntryOrderCancellationRepository,
        run_id: str,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._exchange = exchange
        self._state_machine = state_machine
        self._repository = repository
        self._run_id = run_id

    async def cancel(self, plans: tuple[OrderExecutionPlan, ...]) -> int:
        """Cancel known plans and adopt/cancel any exchange-visible orphan."""

        known_ids = {plan.client_order_id for plan in plans}
        cancelled_count = 0
        for plan in plans:
            result = await self._state_machine.cancel_order(plan)
            _require_confirmed_cancellation(
                result,
                f"known opening order cancellation was not confirmed: "
                f"{plan.client_order_id}",
            )
            cancelled_count += 1

        open_orders = await self._exchange.fetch_open_orders()
        for order in open_orders:
            if order.reduce_only or order.client_order_id in known_ids:
                continue
            orphan_plan = external_open_order_cancellation_plan(
                order,
                run_id=self._run_id,
            )
            await self._repository.adopt_external_order_for_cancellation(
                orphan_plan,
                exchange_order_id=order.order_id,
                observed_at=order.observed_at,
            )
            result = await self._state_machine.cancel_order(orphan_plan)
            _require_confirmed_cancellation(
                result,
                "exchange opening order cancellation was not confirmed: "
                f"{order.client_order_id}",
            )
            cancelled_count += 1
        return cancelled_count


def _require_confirmed_cancellation(
    result: OrderExecutionResult,
    message: str,
) -> None:
    if result.state is ExchangeOrderState.REJECTED or not result.state.terminal:
        raise RuntimeError(message)


__all__ = [
    "EntryOrderCancellationRepository",
    "LiveEntryOrderCanceller",
    "OpenOrderExchange",
    "external_open_order_cancellation_plan",
]
