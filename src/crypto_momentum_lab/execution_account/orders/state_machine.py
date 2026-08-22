import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.domain.market.models import JsonValue


class SubmitPolicy(StrEnum):
    SHADOW_SUPPRESS = "shadow_suppress"
    LIVE_SUBMIT = "live_submit"


class LiveSubmissionDisabledError(RuntimeError):
    pass


class ExchangeOrderRejectedError(RuntimeError):
    pass


class ExchangeSubmissionTimeoutError(TimeoutError):
    pass


class ExchangeOrderQueryUnknownError(RuntimeError):
    """Order lookup failed before the exchange state was known."""

    pass


class ExchangeCancellationUnknownError(RuntimeError):
    """The cancel request outcome is unknown and needs reconciliation."""

    pass


class OrderExchangeClient(Protocol):
    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        pass

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        pass

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        pass


class OrderStateRepository(Protocol):
    async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
        pass

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        pass

    async def save_fill(self, fill: ExchangeOrderFill) -> bool:
        pass

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        pass


@dataclass(frozen=True, slots=True)
class OrderExecutionResult:
    client_order_id: str
    state: ExchangeOrderState
    exchange_order_id: str | None
    suppressed: bool = False
    executed_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")


class OrderExecutionStateMachine:
    def __init__(
        self,
        *,
        exchange: OrderExchangeClient,
        repository: OrderStateRepository,
        submit_policy: SubmitPolicy,
        live_submit_enabled: bool,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._exchange = exchange
        self._repository = repository
        self._submit_policy = submit_policy
        self._live_submit_enabled = live_submit_enabled
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._lock = asyncio.Lock()

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        async with self._lock:
            return await self._execute_approved_intent(plan)

    async def _execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        if not plan.quantized:
            raise ValueError("order plan must be quantized before execution")
        if (
            self._submit_policy is SubmitPolicy.LIVE_SUBMIT
            and not self._live_submit_enabled
        ):
            raise LiveSubmissionDisabledError(
                "live_submit policy requires explicit live_submit_enabled"
            )
        await self._repository.save_planned_order(plan)
        if self._submit_policy is SubmitPolicy.SHADOW_SUPPRESS:
            await self._repository.save_shadow_suppression(
                ShadowSuppressionEvent(
                    order_plan_id=plan.client_order_id,
                    client_order_id=plan.client_order_id,
                    suppressed_at=self._now(),
                    reason="shadow_submit_policy",
                    order_payload={
                        "symbol": plan.symbol,
                        "side": plan.side,
                        "type": plan.order_type,
                        "quantity": str(plan.quantity),
                        "price": None if plan.price is None else str(plan.price),
                        "reduce_only": plan.reduce_only,
                    },
                )
            )
            await self._append_event(plan, ExchangeOrderState.SUPPRESSED)
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.SUPPRESSED,
                exchange_order_id=None,
                suppressed=True,
            )

        await self._append_event(plan, ExchangeOrderState.SUBMITTING)
        try:
            snapshot = await self._exchange.submit_order(plan)
        except ExchangeOrderRejectedError as exc:
            await self._append_event(
                plan,
                ExchangeOrderState.REJECTED,
                details={"reason": str(exc)},
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.REJECTED,
                None,
            )
        except ExchangeSubmissionTimeoutError:
            try:
                queried_snapshot = await self._exchange.query_order_by_client_id(
                    plan.symbol,
                    plan.client_order_id,
                )
            except ExchangeOrderQueryUnknownError as exc:
                await self._append_event(
                    plan,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    details={"reason": str(exc)},
                )
                return OrderExecutionResult(
                    plan.client_order_id,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    None,
                )
            if queried_snapshot is None:
                await self._append_event(
                    plan,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    details={"reason": "submit_timeout_order_not_found"},
                )
                return OrderExecutionResult(
                    plan.client_order_id,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    None,
                )
            snapshot = queried_snapshot
        return await self._apply_snapshot(plan, snapshot)

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        async with self._lock:
            return await self._reconcile_order(plan)

    async def _reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        snapshot = await self._exchange.query_order_by_client_id(
            plan.symbol,
            plan.client_order_id,
        )
        if snapshot is None:
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={"reason": "reconciliation_order_not_found"},
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
            )
        return await self._apply_snapshot(plan, snapshot)

    async def cancel_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        """Cancel a known resting order and persist the result.

        Cancellation is a normal part of the B1 grace-timeout flow, so it is
        intentionally separate from the operator-authorized emergency cancel
        control exposed by the Binance client.
        """
        async with self._lock:
            return await self._cancel_order(plan)

    async def _cancel_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        if not plan.quantized:
            raise ValueError("order plan must be quantized before cancellation")
        await self._append_event(plan, ExchangeOrderState.CANCELING)
        try:
            snapshot = await self._exchange.cancel_order_by_client_id(
                plan.symbol,
                plan.client_order_id,
            )
        except ExchangeCancellationUnknownError as exc:
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={"reason": str(exc)},
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
            )
        except ExchangeOrderRejectedError as exc:
            await self._append_event(
                plan,
                ExchangeOrderState.REJECTED,
                details={"reason": str(exc)},
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.REJECTED,
                None,
            )
        return await self._apply_snapshot(plan, snapshot)

    async def _apply_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        if snapshot.client_order_id != plan.client_order_id:
            raise ValueError("exchange response client order id mismatch")
        for fill in snapshot.fills:
            await self._repository.save_fill(fill)
        await self._append_event(
            plan,
            snapshot.state,
            exchange_order_id=snapshot.exchange_order_id,
            details={
                "executed_quantity": str(snapshot.executed_quantity),
                "average_price": str(snapshot.average_price),
                "entry_leverage": snapshot.entry_leverage,
            },
            occurred_at=snapshot.observed_at,
        )
        return OrderExecutionResult(
            plan.client_order_id,
            snapshot.state,
            snapshot.exchange_order_id,
            executed_quantity=snapshot.executed_quantity,
            average_price=snapshot.average_price,
        )

    async def _append_event(
        self,
        plan: OrderExecutionPlan,
        state: ExchangeOrderState,
        *,
        exchange_order_id: str | None = None,
        details: dict[str, JsonValue] | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        event_at = occurred_at or self._now()
        event_id = str(
            uuid5(
                NAMESPACE_URL,
                f"order-event:{plan.client_order_id}:{state.value}:"
                f"{event_at.isoformat()}",
            )
        )
        await self._repository.append_order_event(
            ExchangeOrderEvent(
                event_id=event_id,
                client_order_id=plan.client_order_id,
                state=state,
                occurred_at=event_at,
                exchange_order_id=exchange_order_id,
                details=details or {},
            )
        )

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return now
