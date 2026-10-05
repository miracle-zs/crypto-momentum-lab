"""Atomic PostgreSQL commit boundaries for execution and live decisions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.evidence_digest import trade_payload_digest
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict as _DecisionCommitConflict,
)
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState as _DurableExecutionPositionState,
)
from crypto_momentum_lab.domain.execution.ports import (
    ExecutionEvidenceIdentity as _ExecutionEvidenceIdentity,
)
from crypto_momentum_lab.domain.execution.ports import (
    ExecutionHeadSnapshot as _ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.ports import (
    ExecutionTradeIdentity as _ExecutionTradeIdentity,
)
from crypto_momentum_lab.domain.execution.ports import (
    ExecutionWatermark as _ExecutionWatermark,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    JournalFactDelta,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
    JournalPersistResult,
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.command_store_ports import (
    ExecutionCommandStore,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionOrderWatermarkRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.journal_store_ports import (
    ExecutionJournalStore,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.reservation_store_ports import (
    ExecutionReservationStore,
)


class ExecutionTransaction:
    """Session-bound writes used by one staged ExecutionBook operation."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        journal_store: ExecutionJournalStore,
        command_repository: ExecutionCommandStore,
        reservation_repository: ExecutionReservationStore,
    ) -> None:
        self.session = session
        self._journal_store = journal_store
        self._command_repository = command_repository
        self._reservation_repository = reservation_repository

    async def persist_facts(
        self,
        *,
        scope: AccountFactStreamScope,
        facts: AccountFacts,
        revision: int,
        checkpoint: PositionRecoveryCheckpoint | None = None,
        delta: JournalFactDelta | None = None,
    ) -> JournalPersistResult:
        result: JournalPersistResult = (
            await self._journal_store.persist_facts_in_session(
                self.session,
                scope=scope,
                facts=facts,
                revision=revision,
                delta=delta,
            )
        )
        fact_checkpoint = facts.recovery_checkpoint
        if (
            checkpoint is not None
            and fact_checkpoint is not None
            and (checkpoint != fact_checkpoint)
        ):
            raise _DecisionCommitConflict(
                "checkpoint argument differs from checkpoint embedded in facts"
            )
        if checkpoint is not None and fact_checkpoint is None:
            await self._journal_store.save_checkpoint_in_session(
                self.session, checkpoint
            )
        return result

    async def load_recovery(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut:
        """Load a target stream inside the current mutation transaction."""
        return await self._journal_store.load_recovery_in_session(
            self.session,
            scope=scope,
            as_of=as_of,
        )

    async def load_checkpoint_by_id(
        self,
        *,
        scope: AccountFactStreamScope,
        checkpoint_id: str,
    ) -> PositionRecoveryCheckpoint | None:
        """Verify an immutable parent checkpoint in the active transaction."""
        return await self._journal_store.load_checkpoint_by_id_in_session(
            self.session,
            scope=scope,
            checkpoint_id=checkpoint_id,
        )

    async def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        *,
        batch_quantities: Mapping[str, Decimal],
        proven_position_quantity: Decimal,
    ) -> None:
        await self._reservation_repository.save_reservations_in_session(
            self.session,
            reservations,
            batch_quantities=batch_quantities,
            proven_position_quantity=proven_position_quantity,
        )

    async def update_reservation(
        self,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None:
        await self._reservation_repository.update_reservation_in_session(
            self.session,
            reservation,
            release_reason=release_reason,
        )

    async def upsert_outbox(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._command_repository.upsert_execution_command_in_session(
            self.session,
            command_id=command_id,
            client_order_id=client_order_id,
            command=command,
            status=status,
            requested_at=requested_at,
            details=details,
        )

    async def record_evidence(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        evidence: _ExecutionEvidenceIdentity,
    ) -> bool:
        identity = _execution_identity_values(key, stream_id, stream_epoch)
        primary_key = (*identity, evidence.evidence_id)
        existing = await self.session.get(
            ExecutionEvidenceReceiptRow, primary_key, with_for_update=True
        )
        if existing is not None:
            if (
                existing.payload_digest != evidence.payload_digest
                or existing.sequence != evidence.sequence
            ):
                raise _DecisionCommitConflict(
                    f"evidence {evidence.evidence_id} was reused with different data "
                    f"(existing_payload_digest={existing.payload_digest}, "
                    f"attempted_payload_digest={evidence.payload_digest}, "
                    f"existing_sequence={existing.sequence}, "
                    f"attempted_sequence={evidence.sequence})"
                )
            return False
        self.session.add(
            ExecutionEvidenceReceiptRow(
                **dict(zip(_EXECUTION_SCOPE_FIELDS, identity, strict=True)),
                evidence_id=evidence.evidence_id,
                sequence=evidence.sequence,
                payload_digest=evidence.payload_digest,
                accepted_at=evidence.accepted_at,
            )
        )
        return True

    async def record_trade(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        trade: _ExecutionTradeIdentity,
    ) -> bool:
        if (
            not trade.quantity.is_finite()
            or not trade.price.is_finite()
            or trade.quantity <= 0
            or trade.price <= 0
            or not trade.trade_id.strip()
            or not trade.order_id.strip()
        ):
            raise ValueError("durable execution trade identity is invalid")
        _execution_identity_values(key, stream_id, stream_epoch)
        position_identity = _execution_position_values(key)
        primary_key = (*position_identity, trade.trade_id)
        existing = await self.session.get(
            ExecutionTradeIdentityRow, primary_key, with_for_update=True
        )
        if existing is not None:
            if (
                existing.order_id != trade.order_id
                or existing.quantity != trade.quantity
                or existing.price != trade.price
                or existing.side != trade.side
                or existing.payload_digest != trade.payload_digest
            ):
                raise _DecisionCommitConflict(
                    f"trade {trade.trade_id} conflicts with its durable identity"
                )
            if not await self._stored_trade_fact_is_valid(key, existing, trade):
                raise _DecisionCommitConflict(
                    f"trade {trade.trade_id} has no intact durable source fact"
                )
            return False
        self.session.add(
            ExecutionTradeIdentityRow(
                **dict(zip(_EXECUTION_POSITION_FIELDS, position_identity, strict=True)),
                trade_id=trade.trade_id,
                order_id=trade.order_id,
                quantity=trade.quantity,
                price=trade.price,
                side=trade.side,
                payload_digest=trade.payload_digest,
                first_seen_at=trade.first_seen_at,
            )
        )
        return True

    async def _stored_trade_fact_is_valid(
        self,
        key: PositionKey,
        existing: ExecutionTradeIdentityRow,
        trade: _ExecutionTradeIdentity,
    ) -> bool:
        rows = (
            await self.session.scalars(
                select(PositionFactJournalEventRow).where(
                    PositionFactJournalEventRow.environment == key.environment,
                    PositionFactJournalEventRow.account_label == key.account_label,
                    PositionFactJournalEventRow.symbol == key.symbol,
                    PositionFactJournalEventRow.position_side
                    == key.position_side.value,
                    PositionFactJournalEventRow.event_kind == "fill",
                    PositionFactJournalEventRow.payload["trade_id"].astext
                    == trade.trade_id,
                    PositionFactJournalEventRow.occurred_at == existing.first_seen_at,
                )
            )
        ).all()
        if not rows:
            return False
        for row in rows:
            raw = json.dumps(
                row.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
            if hashlib.sha256(raw).hexdigest() != row.payload_hash:
                return False
            fill = PositionRecoveryCodec.decode_fill(row.payload)
            if (
                fill.environment != key.environment
                or fill.account_label != key.account_label
                or fill.symbol != key.symbol
                or fill.trade_id != trade.trade_id
                or fill.trade_at != row.occurred_at
                or trade_payload_digest(fill) != trade.payload_digest
            ):
                return False
        return True

    async def persist_watermark(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        watermark: _ExecutionWatermark,
    ) -> None:
        if (
            not watermark.cumulative_quantity.is_finite()
            or not watermark.cumulative_quote.is_finite()
            or watermark.cumulative_quantity < 0
            or watermark.cumulative_quote < 0
            or (watermark.cumulative_quantity == 0 and watermark.cumulative_quote != 0)
            or (watermark.cumulative_quantity > 0 and watermark.cumulative_quote <= 0)
        ):
            raise ValueError("execution cumulative watermark is invalid")
        _execution_identity_values(key, stream_id, stream_epoch)
        position_identity = _execution_position_values(key)
        primary_key = (*position_identity, watermark.order_id)
        row = await self.session.get(
            ExecutionOrderWatermarkRow, primary_key, with_for_update=True
        )
        if row is None:
            self.session.add(
                ExecutionOrderWatermarkRow(
                    **dict(
                        zip(
                            _EXECUTION_POSITION_FIELDS,
                            position_identity,
                            strict=True,
                        )
                    ),
                    order_id=watermark.order_id,
                    cumulative_quantity=watermark.cumulative_quantity,
                    cumulative_quote=watermark.cumulative_quote,
                    updated_at=watermark.updated_at,
                )
            )
            return
        if watermark.cumulative_quantity < row.cumulative_quantity:
            raise _DecisionCommitConflict("execution quantity watermark regressed")
        if watermark.cumulative_quote < row.cumulative_quote:
            raise _DecisionCommitConflict("execution quote watermark regressed")
        if (
            watermark.cumulative_quantity == row.cumulative_quantity
            and watermark.cumulative_quote != row.cumulative_quote
        ):
            raise _DecisionCommitConflict(
                "execution quote changed without a quantity increase"
            )
        row.cumulative_quantity = watermark.cumulative_quantity
        row.cumulative_quote = watermark.cumulative_quote
        row.updated_at = watermark.updated_at

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
    ) -> int:
        _execution_identity_values(key, stream_id, stream_epoch)
        identity = _execution_position_values(key)
        row = await self.session.get(
            ExecutionBookHeadRow, identity, with_for_update=True
        )
        if stream_adoption_checkpoint_id is not None:
            checkpoint_scope = AccountFactStreamScope.for_position_key(
                key, stream_id=stream_id, stream_epoch=stream_epoch
            )
            checkpoint_row = await self.session.get(
                PositionRecoveryCheckpointRow,
                (
                    *_execution_position_values(key),
                    stream_id,
                    stream_epoch,
                    stream_adoption_checkpoint_id,
                ),
            )
            if (
                checkpoint_row is None
                or checkpoint_row.payload.get("checkpoint_id")
                != stream_adoption_checkpoint_id
                or checkpoint_row.payload.get("stream_scope")
                != {
                    "environment": checkpoint_scope.environment,
                    "account_label": checkpoint_scope.account_label,
                    "symbol": checkpoint_scope.symbol,
                    "position_side": checkpoint_scope.position_side.value,
                    "stream_id": checkpoint_scope.stream_id,
                    "stream_epoch": checkpoint_scope.stream_epoch,
                }
            ):
                raise _DecisionCommitConflict(
                    "execution stream adoption checkpoint is not in this transaction"
                )
        if row is not None and (
            row.stream_id != stream_id or row.stream_epoch != stream_epoch
        ):
            if not stream_adoption_checkpoint_id and not is_flat_adoption:
                raise _DecisionCommitConflict(
                    "execution stream changed without a validated recovery checkpoint"
                )
            if not is_flat_adoption and stream_adoption_checkpoint_id is None:
                raise _DecisionCommitConflict(
                    "execution stream adoption checkpoint could not be verified"
                )
        current_revision = row.revision if row is not None else 0
        if current_revision != expected_revision:
            raise _DecisionCommitConflict(
                f"execution scope revision changed: expected {expected_revision}, "
                f"current {current_revision}"
            )
        next_revision = current_revision + 1
        if row is None:
            self.session.add(
                ExecutionBookHeadRow(
                    **dict(zip(_EXECUTION_POSITION_FIELDS, identity, strict=True)),
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                    revision=next_revision,
                    projection_version=projection_version,
                    state_payload=state_payload,
                    updated_at=updated_at,
                )
            )
        else:
            row.stream_id = stream_id
            row.stream_epoch = stream_epoch
            row.revision = next_revision
            row.projection_version = projection_version
            row.state_payload = state_payload
            row.updated_at = updated_at
        return next_revision

    async def load_head(self, key: PositionKey) -> _ExecutionHeadSnapshot | None:
        row = await self.session.get(
            ExecutionBookHeadRow,
            _execution_position_values(key),
            with_for_update=True,
        )
        if row is None:
            return None
        return _ExecutionHeadSnapshot(
            revision=row.revision,
            stream_id=row.stream_id,
            stream_epoch=row.stream_epoch,
            projection_version=row.projection_version,
            state_payload=dict(row.state_payload),
        )


_EXECUTION_SCOPE_FIELDS = (
    "environment",
    "account_label",
    "symbol",
    "position_side",
    "stream_id",
    "stream_epoch",
)

_EXECUTION_POSITION_FIELDS = (
    "environment",
    "account_label",
    "symbol",
    "position_side",
)


def _execution_position_values(key: PositionKey) -> tuple[str, str, str, str]:
    return (
        key.environment,
        key.account_label,
        key.symbol,
        key.position_side.value,
    )


def _execution_scope_conditions(
    key: PositionKey,
    scope: AccountFactStreamScope,
) -> tuple[Any, ...]:
    return (
        ExecutionEvidenceReceiptRow.environment == key.environment,
        ExecutionEvidenceReceiptRow.account_label == key.account_label,
        ExecutionEvidenceReceiptRow.symbol == key.symbol,
        ExecutionEvidenceReceiptRow.position_side == key.position_side.value,
        ExecutionEvidenceReceiptRow.stream_id == scope.stream_id,
        ExecutionEvidenceReceiptRow.stream_epoch == scope.stream_epoch,
    )


def _execution_identity_values(
    key: PositionKey, stream_id: str, stream_epoch: str
) -> tuple[str, str, str, str, str, str]:
    if not stream_id.strip() or not stream_epoch.strip():
        raise ValueError("execution stream id and epoch are required for durability")
    return (
        key.environment,
        key.account_label,
        key.symbol,
        key.position_side.value,
        stream_id,
        stream_epoch,
    )


class AsyncPostgresExecutionUnitOfWork:
    """Serialize one account-position mutation into a PostgreSQL transaction."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        journal_store: ExecutionJournalStore,
        command_repository: ExecutionCommandStore,
        reservation_repository: ExecutionReservationStore,
    ) -> None:
        self._session_factory = session_factory
        self._journal_store = journal_store
        self._command_repository = command_repository
        self._reservation_repository = reservation_repository

    async def load_journal_cut(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut:
        """Load one exact historical stream cut without consulting the head.

        Historical reads must not borrow the current head's projection token or
        another stream epoch's checkpoint. The journal store owns the cut
        reconstruction and checkpoint validation.
        """
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        async with self._session_factory() as session:
            bind = session.get_bind()
            if bind is None or bind.dialect.name != "postgresql":
                raise RuntimeError("durable position reads require PostgreSQL")
            return await self._journal_store.load_recovery_in_session(
                session,
                scope=scope,
                as_of=as_of,
            )

    async def load_position(
        self,
        key: PositionKey,
        *,
        as_of: datetime,
    ) -> _DurableExecutionPositionState | None:
        states = await self._load_position_states(
            environment=key.environment,
            account_label=key.account_label,
            as_of=as_of,
            key=key,
        )
        return states[0] if states else None

    async def load_positions(
        self,
        *,
        environment: str,
        account_label: str,
        as_of: datetime,
    ) -> tuple[_DurableExecutionPositionState, ...]:
        return await self._load_position_states(
            environment=environment,
            account_label=account_label,
            as_of=as_of,
        )

    async def _load_position_states(
        self,
        *,
        environment: str,
        account_label: str,
        as_of: datetime,
        key: PositionKey | None = None,
    ) -> tuple[_DurableExecutionPositionState, ...]:
        """Load the exact adopted account stream and all cross-epoch identities."""
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        async with self._session_factory() as session:
            bind = session.get_bind()
            if bind is None or bind.dialect.name != "postgresql":
                raise RuntimeError("durable position restoration requires PostgreSQL")
            scopes = await self._journal_store.list_scopes_in_session(
                session,
                environment=environment,
                account_label=account_label,
                **({"key": key} if key is not None else {}),
            )
            by_position: dict[str, list[AccountFactStreamScope]] = {}
            for scope in scopes:
                key = PositionKey(
                    environment=scope.environment,
                    account_label=scope.account_label,
                    symbol=scope.symbol,
                    position_side=scope.position_side,
                )
                by_position.setdefault(key.canonical_id, []).append(scope)

            recovered: list[_DurableExecutionPositionState] = []
            for scoped_rows in by_position.values():
                first_scope = scoped_rows[0]
                key = PositionKey(
                    environment=first_scope.environment,
                    account_label=first_scope.account_label,
                    symbol=first_scope.symbol,
                    position_side=first_scope.position_side,
                )
                head_row = await session.get(
                    ExecutionBookHeadRow,
                    _execution_position_values(key),
                )
                head = (
                    _ExecutionHeadSnapshot(
                        revision=head_row.revision,
                        stream_id=head_row.stream_id,
                        stream_epoch=head_row.stream_epoch,
                        projection_version=head_row.projection_version,
                        state_payload=dict(head_row.state_payload),
                    )
                    if head_row is not None
                    else None
                )
                if head is not None:
                    matched_scope = next(
                        (
                            candidate
                            for candidate in scoped_rows
                            if candidate.stream_id == head.stream_id
                            and candidate.stream_epoch == head.stream_epoch
                        ),
                        None,
                    )
                    if matched_scope is None:
                        scope = AccountFactStreamScope.for_position_key(
                            key,
                            stream_id=head.stream_id,
                            stream_epoch=head.stream_epoch,
                        )
                    else:
                        scope = matched_scope
                elif len(scoped_rows) == 1:
                    scope = scoped_rows[0]
                else:
                    raise _DecisionCommitConflict(
                        f"position {key.canonical_id} has multiple streams "
                        "but no durable head"
                    )

                cut = await self._journal_store.load_recovery_in_session(
                    session,
                    scope=scope,
                    as_of=as_of,
                )
                position_conditions = (
                    ExecutionTradeIdentityRow.environment == key.environment,
                    ExecutionTradeIdentityRow.account_label == key.account_label,
                    ExecutionTradeIdentityRow.symbol == key.symbol,
                    ExecutionTradeIdentityRow.position_side == key.position_side.value,
                )
                trade_rows = (
                    await session.scalars(
                        select(ExecutionTradeIdentityRow).where(*position_conditions)
                    )
                ).all()
                evidence_rows = (
                    await session.scalars(
                        select(ExecutionEvidenceReceiptRow).where(
                            *_execution_scope_conditions(key, scope)
                        )
                    )
                ).all()
                watermark_rows = (
                    await session.scalars(
                        select(ExecutionOrderWatermarkRow).where(
                            ExecutionOrderWatermarkRow.environment == key.environment,
                            ExecutionOrderWatermarkRow.account_label
                            == key.account_label,
                            ExecutionOrderWatermarkRow.symbol == key.symbol,
                            ExecutionOrderWatermarkRow.position_side
                            == key.position_side.value,
                        )
                    )
                ).all()
                recovered.append(
                    _DurableExecutionPositionState(
                        scope=scope,
                        cut=cut,
                        head=head,
                        trade_ids=tuple(row.trade_id for row in trade_rows),
                        evidence_ids=tuple(row.evidence_id for row in evidence_rows),
                        watermarks=tuple(
                            _ExecutionWatermark(
                                order_id=row.order_id,
                                cumulative_quantity=row.cumulative_quantity,
                                cumulative_quote=row.cumulative_quote,
                                updated_at=row.updated_at,
                            )
                            for row in watermark_rows
                        ),
                    )
                )
        return tuple(recovered)

    @asynccontextmanager
    async def transaction(
        self,
        key: PositionKey,
        *,
        account_scope: str | None = None,
    ) -> AsyncIterator[ExecutionTransaction]:
        async with self._session_factory() as session:
            async with session.begin():
                bind = session.get_bind()
                if bind is None or bind.dialect.name != "postgresql":
                    raise RuntimeError(
                        "durable execution commits require PostgreSQL transactions"
                    )
                await session.execute(text("SET LOCAL synchronous_commit = ON"))
                if account_scope:
                    await session.execute(
                        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                        {"lock_key": f"live-exposure:{account_scope}"},
                    )
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                    {"lock_key": f"execution_position:{key.canonical_id}"},
                )
                yield ExecutionTransaction(
                    session,
                    journal_store=self._journal_store,
                    command_repository=self._command_repository,
                    reservation_repository=self._reservation_repository,
                )


from crypto_momentum_lab.persistence.postgres.decision_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
)

__all__ = ["AsyncPostgresDecisionUnitOfWork"]
