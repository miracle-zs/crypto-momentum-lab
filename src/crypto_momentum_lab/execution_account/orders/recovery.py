from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderSnapshot,
    OrderExecutionPlan,
)


class ExitRecoveryInspectionUnknownError(RuntimeError):
    """The exchange could not provide a safe recovery observation."""


@dataclass(frozen=True, slots=True)
class ExitRecoveryObservation:
    """A point-in-time exchange view used before retrying an exit."""

    order: ExchangeOrderSnapshot | None
    position_quantity: Decimal
    active_exit_order_client_ids: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.position_quantity < 0:
            raise ValueError("position_quantity must be non-negative")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if any(
            not client_order_id.strip()
            for client_order_id in self.active_exit_order_client_ids
        ):
            raise ValueError(
                "active_exit_order_client_ids must not contain blank values"
            )
        if len(set(self.active_exit_order_client_ids)) != len(
            self.active_exit_order_client_ids
        ):
            raise ValueError("active_exit_order_client_ids must be unique")

    @property
    def has_active_order(self) -> bool:
        return bool(self.active_exit_order_client_ids) or (
            self.order is not None and not self.order.state.terminal
        )


class ExitRecoveryClient(Protocol):
    async def inspect_exit_order(
        self,
        plan: OrderExecutionPlan,
    ) -> ExitRecoveryObservation: ...


__all__ = [
    "ExitRecoveryClient",
    "ExitRecoveryInspectionUnknownError",
    "ExitRecoveryObservation",
]
