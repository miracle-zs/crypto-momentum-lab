"""Map execution-book admission results to submission errors."""

from __future__ import annotations

from crypto_momentum_lab.domain.execution.execution_action_models import (
    Blocked,
    CommandConflict,
    ExecutionActResult,
    ExecutionRecoveryPending,
    PositionNotReady,
    StaleView,
)
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
    OrderProjectionConflictError,
    OrderRecoveryPendingError,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ExecutionReadinessError,
)


def require_accepted_execution_result(
    plan: OrderExecutionPlan, result: ExecutionActResult
) -> None:
    """Raise the precise submission error for a non-accepted Book result."""
    if isinstance(result, Blocked):
        cause = (
            ExecutionReadinessError(result.reason)
            if isinstance(result, (PositionNotReady, ExecutionRecoveryPending))
            else None
        )
        error_type = (
            OrderRecoveryPendingError
            if isinstance(result, ExecutionRecoveryPending) and not plan.reduce_only
            else (
                OrderProjectionConflictError
                if "ReservationConflictError" in result.diagnostics
                else OrderPreSubmissionError
            )
        )
        raise error_type(
            f"Failed to create position reservation for "
            f"{plan.client_order_id}: {result.reason}"
        ) from cause
    if isinstance(result, StaleView):
        error_type = (
            OrderProjectionConflictError
            if plan.reduce_only
            else OrderPreSubmissionError
        )
        raise error_type(
            f"Failed to create position reservation (stale view) for "
            f"{plan.client_order_id}: {result.reason}"
        )
    if isinstance(result, CommandConflict):
        raise OrderPreSubmissionError(
            f"Failed to create position reservation (command conflict) for "
            f"{plan.client_order_id}: {result.reason}"
        )
