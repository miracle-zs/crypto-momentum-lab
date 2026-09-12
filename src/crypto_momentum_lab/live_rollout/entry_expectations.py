"""Pre-submission registration of live entry position expectations."""

from datetime import datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
)
from crypto_momentum_lab.execution_account.hub import (
    WebSocketAccountPositionExpectationPublisher,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)

log = structlog.get_logger()


class PositionExpectationPublisher(Protocol):
    async def register(self, expectation: AccountPositionExpectation) -> None: ...


class LiveEntryExpectationRegistrar:
    """Publish a strategy-owned entry hint before exchange submission."""

    def __init__(
        self,
        *,
        account_event_hub_url: str,
        account_label: str,
        publisher: PositionExpectationPublisher | None = None,
    ) -> None:
        if not account_event_hub_url.strip():
            raise ValueError("account_event_hub_url must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        self._account_label = account_label
        self._publisher = publisher or WebSocketAccountPositionExpectationPublisher(
            url=account_event_hub_url,
            environment="live",
            account_label=account_label,
        )

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
