"""Immutable command dispatch values independent of ExecutionBook."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand


class DispatchState(StrEnum):
    """Local dispatch/settlement projection; exchange order rows own wire state."""

    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


class CommandExecutionState(StrEnum):
    """Canonical 5-state execution dimension according to REFACTOR_SPEC §3."""

    PREPARED = "prepared"
    SENDING = "sending"
    WORKING = "working"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


def to_command_execution_state(
    state: DispatchState | CommandExecutionState | str,
) -> CommandExecutionState:
    """Map operational dispatch or wire state to canonical CommandExecutionState."""
    if isinstance(state, CommandExecutionState):
        return state
    raw = state.value if hasattr(state, "value") else str(state)
    raw_lower = raw.lower()
    if raw_lower in ("prepared", "intent_approved", "planned", "claimed"):
        return CommandExecutionState.PREPARED
    if raw_lower in ("dispatching", "sending", "submitting"):
        return CommandExecutionState.SENDING
    if raw_lower in (
        "acknowledged",
        "working",
        "submitted",
        "partially_filled",
        "canceling",
    ):
        return CommandExecutionState.WORKING
    if raw_lower in ("unknown", "unknown_pending_reconciliation"):
        return CommandExecutionState.UNKNOWN
    if raw_lower in (
        "rejected",
        "terminal",
        "filled",
        "canceled",
        "expired",
        "absent_reconciled",
        "suppressed",
    ):
        return CommandExecutionState.TERMINAL
    raise ValueError(f"Unknown state for CommandExecutionState mapping: {state}")


@dataclass(frozen=True, slots=True)
class ExecutionScope:
    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def to_position_key(self) -> PositionKey:
        return PositionKey(
            environment=self.environment,
            account_label=self.account_label,
            symbol=self.symbol,
            position_side=self.position_side,
        )


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """Immutable dispatch record tracking external order submission attempt."""

    command_id: str
    request_id: str
    scope: ExecutionScope
    command: TradeCommand
    state: DispatchState = DispatchState.PREPARED
    attempt_count: int = 0
    external_order_id: str | None = None
    last_error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def execution_state(self) -> CommandExecutionState:
        return to_command_execution_state(self.state)
