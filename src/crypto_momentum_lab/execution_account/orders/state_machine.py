import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, TypeVar
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


class ExchangeOrderAlreadyAbsentError(RuntimeError):
    """The exchange explicitly confirmed that the target order is absent."""

    def __init__(
        self,
        message: str,
        *,
        exchange_code: int | None = None,
        exchange_message: str | None = None,
        http_status: int | None = None,
        open_orders_checked: bool = False,
    ) -> None:
        super().__init__(message)
        self.exchange_code = exchange_code
        self.exchange_message = exchange_message or message
        self.http_status = http_status
        self.open_orders_checked = open_orders_checked


class ExchangeSubmissionTimeoutError(TimeoutError):
    pass


class ExchangeOrderQueryUnknownError(RuntimeError):
    """Order lookup failed before the exchange state was known."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ExchangeCancellationUnknownError(RuntimeError):
    """The cancel request outcome is unknown and needs reconciliation."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


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


OrderEventCallback = Callable[
    [OrderExecutionPlan, ExchangeOrderEvent],
    Awaitable[None],
]
ExchangeBoundaryCallback = Callable[
    [OrderExecutionPlan, str, datetime],
    Awaitable[None],
]
ExchangeCallResult = TypeVar("ExchangeCallResult")


@dataclass(frozen=True, slots=True)
class OrderExecutionResult:
    client_order_id: str
    state: ExchangeOrderState
    exchange_order_id: str | None
    suppressed: bool = False
    executed_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class PreparedOrderSubmission:
    """Durable write-ahead journal returned by an atomic order preparation."""

    plan: OrderExecutionPlan
    submitting_event: ExchangeOrderEvent

    def __post_init__(self) -> None:
        if self.submitting_event.state is not ExchangeOrderState.SUBMITTING:
            raise ValueError("prepared submission must contain a SUBMITTING event")
        if self.submitting_event.client_order_id != self.plan.client_order_id:
            raise ValueError(
                "prepared submission event must reference the order plan"
            )


@dataclass(frozen=True, slots=True)
class _OrderQueryResult:
    snapshot: ExchangeOrderSnapshot | None
    reason: str | None
    attempts: int
    confirmed_absent: bool = False


class OrderExecutionStateMachine:
    def __init__(
        self,
        *,
        exchange: OrderExchangeClient,
        repository: OrderStateRepository,
        submit_policy: SubmitPolicy,
        live_submit_enabled: bool,
        clock: Callable[[], datetime] | None = None,
        on_event: OrderEventCallback | None = None,
        on_exchange_request: ExchangeBoundaryCallback | None = None,
        on_exchange_response: ExchangeBoundaryCallback | None = None,
        serialize_commands: bool = True,
        reconciliation_retry_delays: tuple[float, ...] = (
            1.0,
            2.0,
            4.0,
            8.0,
        ),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if any(delay < 0 for delay in reconciliation_retry_delays):
            raise ValueError("reconciliation retry delays must not be negative")
        self._exchange = exchange
        self._repository = repository
        self._submit_policy = submit_policy
        self._live_submit_enabled = live_submit_enabled
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._on_event = on_event
        self._on_exchange_request = on_exchange_request
        self._on_exchange_response = on_exchange_response
        self._reconciliation_retry_delays = tuple(reconciliation_retry_delays)
        self._sleep = sleep
        self._lock = asyncio.Lock() if serialize_commands else None

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
    ) -> OrderExecutionResult:
        if self._lock is None:
            return await self._execute_approved_intent(
                plan,
                prepared_submission=prepared_submission,
            )
        async with self._lock:
            return await self._execute_approved_intent(
                plan,
                prepared_submission=prepared_submission,
            )

    async def _execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
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
        if prepared_submission is not None:
            if prepared_submission.plan != plan:
                raise ValueError("prepared submission does not match order plan")
            if self._submit_policy is SubmitPolicy.SHADOW_SUPPRESS:
                raise ValueError(
                    "shadow submit cannot use a prepared live submission"
                )
        else:
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

        if prepared_submission is None:
            await self._append_event(plan, ExchangeOrderState.SUBMITTING)
        else:
            await self._notify_event(
                prepared_submission.plan,
                prepared_submission.submitting_event,
            )
        try:
            snapshot = await self._exchange_call(
                plan,
                operation="submit",
                call=lambda: self._exchange.submit_order(plan),
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
        except ExchangeSubmissionTimeoutError:
            query_result = await self._query_order_with_retry(
                plan,
                not_found_reason="submit_timeout_order_not_found",
            )
            if query_result.snapshot is None:
                await self._append_event(
                    plan,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    details={
                        "reason": query_result.reason
                        or "submit_timeout_order_not_found",
                        "reconciliation_attempts": query_result.attempts,
                    },
                )
                return OrderExecutionResult(
                    plan.client_order_id,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    None,
                )
            snapshot = query_result.snapshot
        return await self._apply_snapshot(plan, snapshot)

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        if self._lock is None:
            return await self._reconcile_order(plan)
        async with self._lock:
            return await self._reconcile_order(plan)

    async def _reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        query_result = await self._query_order_with_retry(
            plan,
            not_found_reason="reconciliation_order_not_found",
        )
        if query_result.snapshot is None:
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={
                    "reason": query_result.reason
                    or "reconciliation_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                },
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
            )
        return await self._apply_snapshot(plan, query_result.snapshot)

    async def cancel_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        """Cancel a known resting order and persist the result.

        Cancellation is a normal part of the B1 grace-timeout flow, so it is
        intentionally separate from the operator-authorized emergency cancel
        control exposed by the Binance client.
        """
        if self._lock is None:
            return await self._cancel_order(plan)
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
            snapshot = await self._exchange_call(
                plan,
                operation="cancel",
                call=lambda: self._exchange.cancel_order_by_client_id(
                    plan.symbol,
                    plan.client_order_id,
                ),
            )
        except ExchangeCancellationUnknownError as exc:
            if exc.retry_after_seconds is not None:
                await self._sleep(exc.retry_after_seconds)
            query_result = await self._query_order_with_retry(
                plan,
                not_found_reason="cancel_result_order_not_found",
            )
            if query_result.snapshot is not None:
                return await self._apply_snapshot(plan, query_result.snapshot)
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={
                    "reason": str(exc) or "cancel_outcome_unknown",
                    "reconciliation_reason": query_result.reason
                    or "cancel_result_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                },
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
            )
        except ExchangeOrderAlreadyAbsentError as exc:
            query_result = await self._query_order_with_retry(
                plan,
                not_found_reason="cancel_result_order_not_found",
            )
            if query_result.snapshot is not None:
                return await self._apply_snapshot(plan, query_result.snapshot)
            if query_result.confirmed_absent:
                details: dict[str, JsonValue] = {
                    "reason": str(exc) or "cancel_order_already_absent",
                    "reconciliation_reason": query_result.reason
                    or "cancel_result_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                    "confirmed_absent": True,
                }
                if exc.exchange_code is not None:
                    details["exchange_code"] = exc.exchange_code
                if exc.exchange_message is not None:
                    details["exchange_message"] = exc.exchange_message
                if exc.http_status is not None:
                    details["http_status"] = exc.http_status
                if exc.open_orders_checked:
                    details["open_orders_checked"] = True
                await self._append_event(
                    plan,
                    ExchangeOrderState.ABSENT_RECONCILED,
                    details=details,
                )
                return OrderExecutionResult(
                    plan.client_order_id,
                    ExchangeOrderState.ABSENT_RECONCILED,
                    None,
                )
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={
                    "reason": str(exc) or "cancel_outcome_unknown",
                    "exchange_code": exc.exchange_code,
                    "exchange_message": exc.exchange_message,
                    "http_status": exc.http_status,
                    "reconciliation_reason": query_result.reason
                    or "cancel_result_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                    "confirmed_absent": False,
                },
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

    async def _query_order_with_retry(
        self,
        plan: OrderExecutionPlan,
        *,
        not_found_reason: str,
    ) -> _OrderQueryResult:
        last_reason: str | None = None
        query_failed = False
        retry_delays = self._reconciliation_retry_delays
        for attempt in range(len(retry_delays) + 1):
            retry_after_seconds: float | None = None
            try:
                snapshot = await self._exchange_call(
                    plan,
                    operation="query",
                    call=lambda: self._exchange.query_order_by_client_id(
                        plan.symbol,
                        plan.client_order_id,
                    ),
                )
            except ExchangeOrderQueryUnknownError as exc:
                query_failed = True
                last_reason = str(exc) or "order_query_unknown"
                retry_after_seconds = exc.retry_after_seconds
            else:
                if snapshot is not None:
                    return _OrderQueryResult(snapshot, None, attempt + 1)
                last_reason = not_found_reason
            if attempt < len(retry_delays):
                delay = retry_delays[attempt]
                if retry_after_seconds is not None:
                    delay = max(delay, retry_after_seconds)
                await self._sleep(delay)
        return _OrderQueryResult(
            snapshot=None,
            reason=last_reason or not_found_reason,
            attempts=len(retry_delays) + 1,
            confirmed_absent=not query_failed,
        )

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
        event = ExchangeOrderEvent(
            event_id=event_id,
            client_order_id=plan.client_order_id,
            state=state,
            occurred_at=event_at,
            exchange_order_id=exchange_order_id,
            details=details or {},
        )
        await self._repository.append_order_event(event)
        await self._notify_event(plan, event)

    async def _notify_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        if self._on_event is not None:
            await self._on_event(plan, event)

    async def _exchange_call(
        self,
        plan: OrderExecutionPlan,
        *,
        operation: str,
        call: Callable[[], Awaitable[ExchangeCallResult]],
    ) -> ExchangeCallResult:
        await self._notify_exchange_boundary(
            plan,
            f"{operation}_request_started",
        )
        try:
            return await call()
        finally:
            await self._notify_exchange_boundary(
                plan,
                f"{operation}_response_received",
            )

    async def _notify_exchange_boundary(
        self,
        plan: OrderExecutionPlan,
        phase: str,
    ) -> None:
        callback = (
            self._on_exchange_request
            if phase.endswith("request_started")
            else self._on_exchange_response
        )
        if callback is None:
            return
        try:
            await callback(plan, phase, self._now())
        except Exception:
            # Telemetry is deliberately not allowed to block an exchange
            # command or change its failure semantics.
            return

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return now
