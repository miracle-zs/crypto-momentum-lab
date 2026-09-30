"""Immutable command dispatch values independent of ExecutionBook."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand


class DispatchState(StrEnum):
    """Authoritative lifecycle states for outbound trade commands."""

    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


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
