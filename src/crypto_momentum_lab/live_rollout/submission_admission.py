"""Final live admission rule evaluated by the execution coordinator after dequeue."""

from collections.abc import Callable

from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate


class LiveSubmissionAdmission:
    def __init__(
        self,
        gate: LiveEntryControlGate,
        context_is_current: Callable[[LiveDaemonRuntimeContext], bool],
    ) -> None:
        self._gate = gate
        self._context_is_current = context_is_current

    def rejection_reason(
        self, plan: OrderExecutionPlan, preparation: OrderSubmissionPreparation
    ) -> str | None:
        if not plan.reduce_only and not self._gate.entry_enabled:
            return self._gate.entry_enabled_reason
        context = preparation.context_token
        if context is not None:
            if not isinstance(context, LiveDaemonRuntimeContext):
                raise TypeError("invalid live submission context token")
            if not self._context_is_current(context):
                return "submission_context_invalidated"
        return None
