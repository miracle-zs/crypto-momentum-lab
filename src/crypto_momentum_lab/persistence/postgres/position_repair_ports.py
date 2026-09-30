"""Execution transaction capability needed by the SQL position repair adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.execution.ports import ExecutionTransactionPort

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class PositionRepairExecutionTransaction(ExecutionTransactionPort, Protocol):
    @property
    def session(self) -> AsyncSession: ...
