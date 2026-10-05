"""Public request and result values for execution command acceptance."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_models import (
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.trading import TradeSide as StrategySide


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    request_id: str
    scope: ExecutionScope
    strategy_name: str
    run_id: str
    decision_ref: str
    expected_view_token: str
    action: TradeCommandType
    requested_quantity: Decimal
    side: StrategySide = field(kw_only=True)
    order_type: str = "MARKET"
    limit_price: Decimal | None = None
    reduce_only: bool = False
    target_batch_ids: tuple[str, ...] = ()
    batch_quantities: Mapping[str, Decimal] | None = None
    exit_policy_mode: ExitPolicyMode = ExitPolicyMode.TARGET_BATCHES_ONLY
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id must not be empty")
        if self.requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if not self.expected_view_token.strip():
            raise ValueError("expected_view_token must not be empty")


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    reservations: tuple[PositionReservation, ...]
    committed_at: datetime
    view_token: str
    outbox_entry: OutboxEntry | None = None


@dataclass(frozen=True, slots=True)
class Accepted:
    receipt: ExecutionReceipt
    prepared_submission: PreparedOrderSubmission | None = None


@dataclass(frozen=True, slots=True)
class AlreadyAccepted:
    receipt: ExecutionReceipt


@dataclass(frozen=True, slots=True)
class StaleView:
    expected_token: str
    current_token: str
    reason: str


@dataclass(frozen=True, slots=True)
class Blocked:
    reason: str
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PositionNotReady(Blocked):
    """A typed position readiness guard refused a command before submission."""


@dataclass(frozen=True, slots=True)
class ExecutionRecoveryPending(Blocked):
    """Account command evidence is still reconciling; keep the consumer alive."""


@dataclass(frozen=True, slots=True)
class CommandConflict:
    request_id: str
    reason: str


ExecutionActResult = Accepted | AlreadyAccepted | StaleView | Blocked | CommandConflict
