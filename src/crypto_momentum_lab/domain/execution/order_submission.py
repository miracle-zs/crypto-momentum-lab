"""Domain contracts for pre-exchange submission and durable preparation."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.risk import RiskEvaluation
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate


class OrderPreSubmissionError(RuntimeError):
    """A local precondition failed before an exchange write was attempted."""


class OrderAlreadyPreparedError(OrderPreSubmissionError):
    """The existing client order must be reconciled instead of submitted again."""


class OrderRecoveryPendingError(OrderPreSubmissionError):
    """Known recovery admission rejection, with no exchange write attempted."""


class OrderProjectionConflictError(OrderPreSubmissionError):
    """The unaccepted plan must be rebuilt from current position facts."""


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


class OrderSubmissionRepository(Protocol):
    async def prepare_submission_in_session(
        self,
        session: object,
        *,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
        plan: OrderExecutionPlan,
        prepared_at: datetime,
        environment: str | None = None,
        account_label: str | None = None,
        strategy_name: str | None = None,
        max_open_positions: int | None = None,
        max_daily_loss: Decimal | None = None,
        max_gross_exposure: Decimal | None = None,
        current_daily_pnl: Decimal | None = None,
        current_gross_exposure: Decimal | None = None,
        open_position_symbols: frozenset[str] | None = None,
        exposure_notional: Decimal | None = None,
        baseline_observed_at: datetime | None = None,
    ) -> PreparedOrderSubmission | None: ...


@dataclass(frozen=True, slots=True)
class OrderSubmissionPreparation:
    intent: OrderIntentCandidate
    evaluation: RiskEvaluation
    environment: str | None = None
    account_label: str | None = None
    strategy_name: str | None = None
    max_open_positions: int | None = None
    max_daily_loss: Decimal | None = None
    max_gross_exposure: Decimal | None = None
    current_daily_pnl: Decimal | None = None
    current_gross_exposure: Decimal | None = None
    open_position_symbols: frozenset[str] | None = None
    exposure_notional: Decimal | None = None
    baseline_observed_at: datetime | None = None
