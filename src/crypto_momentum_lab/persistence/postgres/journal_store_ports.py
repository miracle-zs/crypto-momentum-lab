"""Journal operations performed within an execution-owned SQL session."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

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

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ExecutionJournalStore(Protocol):
    async def persist_facts_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        facts: AccountFacts,
        revision: int,
        delta: JournalFactDelta | None = None,
    ) -> JournalPersistResult: ...

    async def save_checkpoint_in_session(
        self,
        session: AsyncSession,
        checkpoint: PositionRecoveryCheckpoint,
    ) -> None: ...

    async def load_checkpoint_by_id_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        checkpoint_id: str,
    ) -> PositionRecoveryCheckpoint | None: ...

    async def load_recovery_in_session(
        self,
        session: AsyncSession,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut: ...

    async def list_scopes_in_session(
        self,
        session: AsyncSession,
        *,
        environment: str,
        account_label: str,
        key: PositionKey | None = None,
    ) -> tuple[AccountFactStreamScope, ...]: ...
