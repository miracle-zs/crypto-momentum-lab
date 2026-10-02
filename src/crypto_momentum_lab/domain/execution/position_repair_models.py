"""Position repair values and transaction contracts, independent of computation."""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    JournalFactDelta,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut


class PositionRepairBlocked(RuntimeError):
    """Facts do not prove a safe repair; leave the position unmanaged."""


@dataclass(frozen=True, slots=True)
class PositionRepairRequest:
    key: PositionKey
    run_id: str
    scope: AccountFactStreamScope
    expected_quantity: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.scope.matches(self.key) or not self.run_id.strip():
            raise ValueError("exact position, run and stream identities are required")
        if not self.expected_quantity.is_finite() or self.expected_quantity <= 0:
            raise ValueError("repair requires positive actual account exposure")
        if self.observed_at.tzinfo is None:
            raise ValueError("repair observation must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PositionRepairFacts:
    cut: DurableJournalCut
    head: ExecutionHeadSnapshot | None
    account_fills: tuple[AccountFillEvent, ...]
    owned_order_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class PositionRepair:
    request: PositionRepairRequest
    facts: AccountFacts
    delta: JournalFactDelta
    revision: int
    expected_head_revision: int
    head_payload: dict[str, object]
    projection_version: str
    new_facts: int
    needs_write: bool


@dataclass(frozen=True, slots=True)
class PositionRepairReceipt:
    scope: AccountFactStreamScope
    head_revision: int
    projection_version: str
    changed: bool


@dataclass(frozen=True, slots=True)
class PublishedPositionRepair:
    receipt: PositionRepairReceipt
    view: PositionView
    new_facts: int


class PositionRepairTransaction(Protocol):
    async def load_repair_facts(
        self, request: PositionRepairRequest
    ) -> PositionRepairFacts: ...
    async def persist_repair(self, repair: PositionRepair) -> PositionRepairReceipt: ...


class PositionRepairUnitOfWork(Protocol):
    # Lock is the SAME execution_position lock as normal execution writes.
    # All loads/writes share one transaction; success commits, exceptions roll back.
    def transaction(
        self, key: PositionKey
    ) -> AbstractAsyncContextManager[PositionRepairTransaction]: ...
