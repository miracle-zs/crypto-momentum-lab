"""Pre-submission registration of live entry position expectations."""

from datetime import datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
)

log = structlog.get_logger()


class PositionExpectationPublisher(Protocol):
    async def register(self, expectation: AccountPositionExpectation) -> None: ...


class LiveEntryExpectationRegistrar:
    """Publish a strategy-owned entry hint before exchange submission."""

    def __init__(
        self,
        *,
        account_label: str,
        publisher: PositionExpectationPublisher,
    ) -> None:
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        self._account_label = account_label
        self._publisher = publisher

    async def __call__(
        self,
        plan: OrderExecutionPlan,
        registered_at: datetime,
    ) -> None:
        try:
            await self._publisher.register(
                AccountPositionExpectation.from_plan(
                    plan,
                    environment="live",
                    account_label=self._account_label,
                    registered_at=registered_at,
                )
            )
        except Exception as error:
            log.error(
                "live_account_position_expectation_registration_failed",
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                error_type=type(error).__name__,
            )
            raise OrderPreSubmissionError(
                "account position expectation registration failed: "
                f"{type(error).__name__}"
            ) from error


__all__ = ["LiveEntryExpectationRegistrar", "PositionExpectationPublisher"]
