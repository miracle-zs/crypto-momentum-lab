import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol, TypeVar
from uuid import NAMESPACE_URL, uuid5

import structlog

from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeBoundaryCallback,
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeOrderQueryUnknownError,
    ExchangeOrderRejectedError,
    ExchangeSubmissionTimeoutError,
    LiveSubmissionDisabledError,
    OrderExchangeClient,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError as _OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    PreparedOrderSubmission as _PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.market.models import JsonValue

log = structlog.get_logger()


class OrderEventRepository(Protocol):
    async def record_order_observation(
        self,
        event: ExchangeOrderEvent,
        fills: tuple[ExchangeOrderFill, ...] = (),
    ) -> bool: ...


OrderEventCallback = Callable[
    [OrderExecutionPlan, ExchangeOrderEvent],
    Awaitable[None],
]
OrderPreSubmissionCallback = Callable[
    [OrderExecutionPlan, datetime],
    Awaitable[None],
]
ExchangeCallResult = TypeVar("ExchangeCallResult")


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
        event_repository: OrderEventRepository,
        live_submit_enabled: bool,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_event: OrderEventCallback | None = None,
        on_before_submit: OrderPreSubmissionCallback | None = None,
        on_exchange_request: ExchangeBoundaryCallback | None = None,
        on_exchange_response: ExchangeBoundaryCallback | None = None,
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
        self._event_repository = event_repository
        self._live_submit_enabled = live_submit_enabled
        self._clock = clock
        self._on_event = on_event
        self._on_before_submit = on_before_submit
        self._on_exchange_request = on_exchange_request
        self._on_exchange_response = on_exchange_response
        self._reconciliation_retry_delays = tuple(reconciliation_retry_delays)
        self._sleep = sleep
        self._observation_lock = asyncio.Lock()
        self._exchange.set_exchange_boundary_callbacks(
            on_request=on_exchange_request,
            on_response=on_exchange_response,
        )

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: _PreparedOrderSubmission,
    ) -> OrderExecutionResult:
        try:
            if not plan.quantized:
                raise ValueError("order plan must be quantized before execution")
            if not self._live_submit_enabled:
                raise LiveSubmissionDisabledError(
                    "live submission requires explicit live_submit_enabled"
                )
            if prepared_submission.plan != plan:
                raise ValueError("prepared submission does not match order plan")
            await self._notify_event(
                prepared_submission.plan,
                prepared_submission.submitting_event,
            )
        except Exception as exc:
            if isinstance(
                exc, (ValueError, LiveSubmissionDisabledError, _OrderPreSubmissionError)
            ):
                raise
            raise _OrderPreSubmissionError(f"pre-submission failed: {exc}") from exc
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
                plan=plan,
            )
        except _OrderPreSubmissionError as exc:
            await self._append_event(
                plan,
                ExchangeOrderState.REJECTED,
                details={
                    "reason": str(exc),
                    "phase": "before_exchange_submit",
                },
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.REJECTED,
                None,
                plan=plan,
            )
        except ExchangeSubmissionTimeoutError as exc:
            query_result = await self._query_order_with_retry(
                plan,
                not_found_reason=str(exc) or "submit_timeout_order_not_found",
            )
            if query_result.snapshot is None:
                await self._append_event(
                    plan,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    details={
                        "reason": query_result.reason
                        or str(exc)
                        or "submit_timeout_order_not_found",
                        "reconciliation_attempts": query_result.attempts,
                    },
                )
                return OrderExecutionResult(
                    plan.client_order_id,
                    ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                    None,
                    plan=plan,
                )
            snapshot = query_result.snapshot
        return await self._apply_snapshot(plan, snapshot)

    async def reconcile_order(
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
                    "reason": query_result.reason or "reconciliation_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                },
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
                plan=plan,
            )
        return await self._apply_snapshot(plan, query_result.snapshot)

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        """Persist an exchange fact without waiting for command network I/O."""
        return await self._apply_snapshot(plan, snapshot)

    async def mark_reconciliation_pending(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        """Persist uncertainty without querying the exchange."""
        async with self._observation_lock:
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={"reason": "incomplete_ws_order_update"},
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
                plan=plan,
            )

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult:
        """Close an unknown order after an independent absence proof."""
        if not plan.quantized:
            raise ValueError("order plan must be quantized before absence resolution")
        async with self._observation_lock:
            await self._append_event(
                plan,
                ExchangeOrderState.ABSENT_RECONCILED,
                details=details,
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.ABSENT_RECONCILED,
                None,
                plan=plan,
            )

    async def cancel_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        """Cancel a known resting order and persist the result.

        Cancellation is a normal part of the B1 grace-timeout flow, so it is
        intentionally separate from the operator-authorized emergency cancel
        control exposed by the Binance client.
        """
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
                plan=plan,
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
                    plan=plan,
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
                plan=plan,
            )
        except ExchangeOrderRejectedError as exc:
            # A cancel rejection only tells us that this request failed. It
            # does not prove that the resting exchange order disappeared, so
            # keep it in the reconciliation queue instead of treating the
            # rejection as a terminal order outcome.
            query_result = await self._query_order_with_retry(
                plan,
                not_found_reason="cancel_rejection_order_not_found",
            )
            if query_result.snapshot is not None:
                return await self._apply_snapshot(plan, query_result.snapshot)
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                details={
                    "reason": str(exc) or "cancel_rejected",
                    "reconciliation_reason": query_result.reason
                    or "cancel_rejection_order_not_found",
                    "reconciliation_attempts": query_result.attempts,
                },
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                None,
                plan=plan,
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
        # All command responses and independently observed facts use this
        # short commit path. Network requests never hold the observation lock.
        async with self._observation_lock:
            return await self._persist_snapshot(plan, snapshot)

    async def _persist_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        if snapshot.client_order_id != plan.client_order_id:
            raise ValueError("exchange response client order id mismatch")
        if snapshot.executed_quantity > 0 and snapshot.average_price <= 0:
            # A reported fill quantity without its quote is incomplete evidence,
            # not a settled terminal receipt. Keep the identity and quantity for
            # recovery; never invent a price from the limit or market quote.
            await self._append_event(
                plan,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                exchange_order_id=snapshot.exchange_order_id,
                details={
                    "reason": "cumulative_fill_price_pending",
                    "reported_state": snapshot.state.value,
                    "executed_quantity": str(snapshot.executed_quantity),
                    "average_price": str(snapshot.average_price),
                },
                occurred_at=snapshot.observed_at,
                fills=snapshot.fills,
            )
            return OrderExecutionResult(
                plan.client_order_id,
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                snapshot.exchange_order_id,
                executed_quantity=snapshot.executed_quantity,
                average_price=snapshot.average_price,
                plan=plan,
            )
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
            fills=snapshot.fills,
        )
        return OrderExecutionResult(
            plan.client_order_id,
            snapshot.state,
            snapshot.exchange_order_id,
            executed_quantity=snapshot.executed_quantity,
            average_price=snapshot.average_price,
            plan=plan,
        )

    async def _append_event(
        self,
        plan: OrderExecutionPlan,
        state: ExchangeOrderState,
        *,
        exchange_order_id: str | None = None,
        details: dict[str, JsonValue] | None = None,
        occurred_at: datetime | None = None,
        fills: tuple[ExchangeOrderFill, ...] = (),
    ) -> None:
        event_at = occurred_at or self._now()
        # Multiple partial fills can share the exchange millisecond. Their
        # cumulative quantities distinguish facts; exact replays retain one ID.
        cumulative_key = (
            f":{details['executed_quantity']}:{details.get('average_price')}"
            if details is not None and "executed_quantity" in details
            else ""
        )
        event_id = str(
            uuid5(
                NAMESPACE_URL,
                f"order-event:{plan.client_order_id}:{state.value}:"
                f"{event_at.isoformat()}{cumulative_key}",
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
        inserted = await self._event_repository.record_order_observation(event, fills)
        if inserted:
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
        try:
            if (
                operation == "submit"
                and not plan.reduce_only
                and self._on_before_submit is not None
            ):
                await self._on_before_submit(plan, self._now())
        except Exception as guard_exc:
            if isinstance(guard_exc, _OrderPreSubmissionError):
                raise
            raise _OrderPreSubmissionError(
                f"pre-submission guard failed: {guard_exc}"
            ) from guard_exc

        exchange_handles_boundary = operation == "submit"
        if not exchange_handles_boundary:
            await self._notify_exchange_boundary(
                plan,
                f"{operation}_request_started",
            )
        try:
            return await call()
        finally:
            if not exchange_handles_boundary:
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
        except Exception as exc:
            # Telemetry is deliberately not allowed to block an exchange
            # command or change its failure semantics.
            log.warning(
                "exchange_boundary_telemetry_failed",
                client_order_id=plan.client_order_id,
                phase=phase,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return now
