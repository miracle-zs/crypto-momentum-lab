"""Domain contracts for pre-exchange submission and durable preparation."""

from dataclasses import dataclass

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)


class OrderPreSubmissionError(RuntimeError):
    """A local precondition failed before an exchange write was attempted."""


@dataclass(frozen=True, slots=True)
class PreparedOrderSubmission:
    """Durable write-ahead journal returned by an atomic order preparation."""

    plan: OrderExecutionPlan
    submitting_event: ExchangeOrderEvent

    def __post_init__(self) -> None:
        if self.submitting_event.state is not ExchangeOrderState.SUBMITTING:
            raise ValueError("prepared submission must contain a SUBMITTING event")
        if self.submitting_event.client_order_id != self.plan.client_order_id:
            raise ValueError("prepared submission event must reference the order plan")
