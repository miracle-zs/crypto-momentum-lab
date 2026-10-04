"""Durable execution contracts implemented by storage adapters."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import AsyncContextManager, Protocol

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    JournalFactDelta,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
    JournalPersistResult,
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.domain.market.models import JsonValue


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

    @property
    def session(self) -> object | None: ...

    async def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        *,
        batch_quantities: Mapping[str, Decimal],
        proven_position_quantity: Decimal,
    ) -> None: ...

    async def update_reservation(
        self,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None: ...

    async def upsert_outbox(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None: ...

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

    async def load_position(
        self,
        key: PositionKey,
        *,
        as_of: datetime,
    ) -> DurableExecutionPositionState | None: ...

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
        *,
        account_scope: str | None = None,
    ) -> AsyncContextManager[ExecutionTransactionPort]: ...


class CommandRepository(Protocol):
    async def upsert_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None: ...
    async def load_active_execution_commands(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]: ...
    async def load_seen_event_ids(self) -> Sequence[str]: ...
    async def load_seen_fill_trade_ids(self) -> Sequence[str]: ...
    async def load_execution_order_watermarks(
        self,
        *,
        account_label: str | None,
    ) -> Sequence[Mapping[str, object]]: ...


class ReservationRepository(Protocol):
    async def load_reservation(
        self, reservation_id: str
    ) -> PositionReservation | None: ...
    async def load_active_reservations(self) -> tuple[PositionReservation, ...]: ...
    async def save_reservations(
        self,
        reservations: tuple[PositionReservation, ...],
        *,
        batch_quantities: dict[str, Decimal],
    ) -> None: ...
    async def update_reservation(
        self,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None: ...


class OrderReadRepository(Protocol):
    async def load_unresolved_orders(
        self,
        run_id: str | None = None,
    ) -> tuple[PersistedExchangeOrder, ...]: ...

    async def load_order(
        self,
        client_order_id: str,
    ) -> PersistedExchangeOrder | None: ...
