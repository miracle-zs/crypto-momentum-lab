"""Journal operations performed within an execution-owned SQL session."""

from datetime import datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    JournalFactDelta,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    JournalPersistResult,
    PositionRecoveryCheckpoint,
)


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
    ) -> tuple[AccountFactStreamScope, ...]: ...
