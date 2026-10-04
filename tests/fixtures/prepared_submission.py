"""Explicit committed submission inputs for exchange and scheduler tests."""

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    PreparedOrderSubmission,
)


def prepared_submission(plan):
    return PreparedOrderSubmission(
        plan=plan,
        submitting_event=ExchangeOrderEvent(
            event_id=f"prepared-{plan.client_order_id}",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=plan.created_at,
            exchange_order_id=None,
            details={},
        ),
    )


async def submit_prepared(executor, plan):
    return await executor.submit(plan, prepared_submission=prepared_submission(plan))
