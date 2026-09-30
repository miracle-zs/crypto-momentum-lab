"""Durable execution contracts implemented by storage adapters."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import AsyncContextManager, Protocol

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    JournalFactDelta,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    JournalPersistResult,
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


class DecisionCommitConflict(RuntimeError):
    """The durable execution or policy head conflicts with the candidate."""


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceIdentity:
    evidence_id: str
    payload_digest: str
    accepted_at: datetime
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class ExecutionTradeIdentity:
    trade_id: str
    order_id: str
    quantity: Decimal
    price: Decimal
    side: str
    payload_digest: str
    first_seen_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionWatermark:
    order_id: str
    cumulative_quantity: Decimal
    cumulative_quote: Decimal
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionHeadSnapshot:
    revision: int
    stream_id: str
    stream_epoch: str
    projection_version: str
    state_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DurableExecutionPositionState:
    scope: AccountFactStreamScope
    cut: DurableJournalCut
    head: ExecutionHeadSnapshot | None
    trade_ids: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    watermarks: tuple[ExecutionWatermark, ...]


class ExecutionTransactionPort(Protocol):
    """Operations that must commit atomically for one position mutation."""

    async def persist_facts(
        self,
        *,
        scope: AccountFactStreamScope,
        facts: AccountFacts,
        revision: int,
        checkpoint: PositionRecoveryCheckpoint | None = None,
        delta: JournalFactDelta | None = None,
    ) -> JournalPersistResult: ...

    async def load_recovery(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut: ...

    async def load_checkpoint_by_id(
        self,
        *,
        scope: AccountFactStreamScope,
        checkpoint_id: str,
    ) -> PositionRecoveryCheckpoint | None: ...

    async def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        *,
        expected_projection_version: str | None = None,
        batch_quantities: Mapping[str, Decimal] | None = None,
        proven_position_quantity: Decimal | None = None,
    ) -> None: ...

    async def update_reservation(
        self,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None: ...

    async def upsert_outbox(self, **values: object) -> None: ...

    async def record_evidence(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        evidence: ExecutionEvidenceIdentity,
    ) -> bool: ...

    async def record_trade(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        trade: ExecutionTradeIdentity,
    ) -> bool: ...

    async def persist_watermark(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        watermark: ExecutionWatermark,
    ) -> None: ...

    async def persist_head(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        expected_revision: int,
        projection_version: str,
        state_payload: dict[str, object],
        updated_at: datetime,
        stream_adoption_checkpoint_id: str | None = None,
        is_flat_adoption: bool = False,
    ) -> int: ...

    async def load_head(self, key: PositionKey) -> ExecutionHeadSnapshot | None: ...


class ExecutionUnitOfWorkPort(Protocol):
    """Durable reads and atomic transaction seam used by ExecutionBook."""

    async def load_journal_cut(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut: ...

    async def load_positions(
        self,
        *,
        environment: str,
        account_label: str,
        as_of: datetime,
    ) -> tuple[DurableExecutionPositionState, ...]: ...

    def transaction(
        self,
        key: PositionKey,
    ) -> AsyncContextManager[ExecutionTransactionPort]: ...
