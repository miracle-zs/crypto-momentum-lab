"""Atomic PostgreSQL commit boundaries for execution and live decisions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.evidence_digest import trade_payload_digest
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderRejectionOutcome,
)
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
    ExitOrderSubmissionFact,
    JournalFactDelta,
    PositionKey,
    PositionLedgerProjection,
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
from crypto_momentum_lab.persistence.postgres.account_fact_rows import (
    account_fill_from_row,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    JournalFactConflict,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    active_execution_command_condition,
)
from crypto_momentum_lab.persistence.postgres.command_store_ports import (
    ExecutionCommandStore,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionOrderWatermarkRow,
    ExecutionRetiredStreamRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.journal_store_ports import (
    ExecutionJournalStore,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    AccountFillEventRow,
    ExecutionCommandRow,
    PositionReservationRow,
)
from crypto_momentum_lab.persistence.postgres.order_rejection import (
    execute_order_rejection_cascade,
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

    async def filter_known_facts(
        self,
        *,
        scope: AccountFactStreamScope,
        delta: JournalFactDelta,
    ) -> JournalFactDelta:
        try:
            return await self._journal_store.filter_known_facts_in_session(
                self.session,
                scope=scope,
                delta=delta,
            )
        except JournalFactConflict as err:
            raise _DecisionCommitConflict(str(err)) from err

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

    async def mark_order_rejected(
        self,
        client_order_id: str,
        *,
        reason: str,
        occurred_at: datetime | None = None,
        intent_id: str | None = None,
    ) -> OrderRejectionOutcome:
        return await execute_order_rejection_cascade(
            self.session,
            client_order_id,
            reason=reason,
            occurred_at=occurred_at,
            intent_id=intent_id,
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
        retired = await self.session.get(ExecutionRetiredStreamRow, identity)
        if retired is not None:
            raise _DecisionCommitConflict("execution stream is permanently retired")
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
        target_identity = _execution_identity_values(key, stream_id, stream_epoch)
        if row is None or (row.stream_id, row.stream_epoch) != (
            stream_id,
            stream_epoch,
        ):
            if await self.session.get(ExecutionRetiredStreamRow, target_identity):
                raise _DecisionCommitConflict("execution stream is permanently retired")
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
        if row is not None and (row.stream_id, row.stream_epoch) != (
            stream_id,
            stream_epoch,
        ):
            previous_identity = _execution_identity_values(
                key, row.stream_id, row.stream_epoch
            )
            if await self.session.get(ExecutionRetiredStreamRow, previous_identity):
                raise _DecisionCommitConflict("current execution stream was retired")
            self.session.add(
                ExecutionRetiredStreamRow(
                    **dict(
                        zip(_EXECUTION_SCOPE_FIELDS, previous_identity, strict=True)
                    ),
                    retired_at=updated_at,
                )
            )
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


def _max_projection_timestamp(
    projection: PositionLedgerProjection | None,
) -> datetime | None:
    if projection is None:
        return None
    cuts: list[datetime] = []
    if projection.event_cut is not None:
        cuts.append(projection.event_cut)
    if projection.high_watermark_trade_at is not None:
        cuts.append(projection.high_watermark_trade_at)
    episodes = list(projection.archived_episodes)
    if projection.active_episode is not None:
        episodes.append(projection.active_episode)
    for ep in episodes:
        if ep.opened_at is not None:
            cuts.append(ep.opened_at)
        if ep.closed_at is not None:
            cuts.append(ep.closed_at)
        for b in ep.batches:
            if b.opened_at is not None:
                cuts.append(b.opened_at)
            if b.exit_order_submitted_at is not None:
                cuts.append(b.exit_order_submitted_at)
        for r in ep.reductions:
            if r.reduced_at is not None:
                cuts.append(r.reduced_at)
    discrepancy = projection.discrepancy
    if discrepancy is not None:
        for occurred_at in (
            discrepancy.first_seen_at,
            discrepancy.last_seen_at,
            discrepancy.event_cut,
            discrepancy.snapshot_at,
        ):
            if occurred_at is not None:
                cuts.append(occurred_at)
    return max(cuts) if cuts else None


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
            heads_by_position: dict[tuple[str, str], ExecutionBookHeadRow] = {}
            if key is None:
                head_rows = (
                    await session.scalars(
                        select(ExecutionBookHeadRow).where(
                            ExecutionBookHeadRow.environment == environment,
                            ExecutionBookHeadRow.account_label == account_label,
                        )
                    )
                ).all()
                heads_by_position = {
                    (head.symbol, head.position_side): head for head in head_rows
                }

                if heads_by_position:
                    await self._require_complete_head_coverage(
                        session,
                        environment=environment,
                        account_label=account_label,
                        heads_by_position=heads_by_position,
                    )
                    active_res_rows = (
                        await session.scalars(
                            select(PositionReservationRow).where(
                                PositionReservationRow.environment == environment,
                                PositionReservationRow.account_label == account_label,
                                PositionReservationRow.reserved_quantity
                                > (
                                    PositionReservationRow.consumed_quantity
                                    + PositionReservationRow.released_quantity
                                ),
                            )
                        )
                    ).all()
                    for res in active_res_rows:
                        if (res.symbol, res.position_side) not in heads_by_position:
                            raise _DecisionCommitConflict(
                                f"position {res.symbol}:{res.position_side} has active reservation {res.reservation_id} "
                                "but no durable head; recovery blocked"
                            )
                    active_cmd_rows = (
                        await session.scalars(
                            select(ExecutionCommandRow)
                            .outerjoin(
                                ExchangeOrderRow,
                                ExecutionCommandRow.client_order_id
                                == ExchangeOrderRow.client_order_id,
                            )
                            .where(
                                ExecutionCommandRow.details["scope"][
                                    "environment"
                                ].astext
                                == environment,
                                ExecutionCommandRow.details["scope"][
                                    "account_label"
                                ].astext
                                == account_label,
                                active_execution_command_condition(),
                            )
                        )
                    ).all()
                    for cmd in active_cmd_rows:
                        cmd_sym = cmd.details.get("scope", {}).get("symbol")
                        cmd_ps = cmd.details.get("scope", {}).get("position_side")
                        if (
                            cmd_sym
                            and cmd_ps
                            and (cmd_sym, cmd_ps) not in heads_by_position
                        ):
                            raise _DecisionCommitConflict(
                                f"position {cmd_sym}:{cmd_ps} has active command {cmd.command_id} "
                                "but no durable head; recovery blocked"
                            )
                    scopes = tuple(
                        AccountFactStreamScope.for_position_key(
                            PositionKey(
                                environment=environment,
                                account_label=account_label,
                                symbol=head.symbol,
                                position_side=FuturesPositionSide(head.position_side),
                            ),
                            stream_id=head.stream_id,
                            stream_epoch=head.stream_epoch,
                        )
                        for head in heads_by_position.values()
                    )
                else:
                    scopes = await self._journal_store.list_scopes_in_session(
                        session,
                        environment=environment,
                        account_label=account_label,
                    )
                    if scopes:
                        raise _DecisionCommitConflict(
                            f"account {account_label} has existing journal streams "
                            "but no durable head; recovery blocked"
                        )
                    active_res_rows = (
                        await session.scalars(
                            select(PositionReservationRow).where(
                                PositionReservationRow.environment == environment,
                                PositionReservationRow.account_label == account_label,
                                PositionReservationRow.reserved_quantity
                                > (
                                    PositionReservationRow.consumed_quantity
                                    + PositionReservationRow.released_quantity
                                ),
                            )
                        )
                    ).all()
                    if active_res_rows:
                        active_res = active_res_rows[0]
                        raise _DecisionCommitConflict(
                            f"account {account_label} has active reservation {active_res.reservation_id} "
                            "but no durable head; recovery blocked"
                        )
                    active_cmd_rows = (
                        await session.scalars(
                            select(ExecutionCommandRow)
                            .outerjoin(
                                ExchangeOrderRow,
                                ExecutionCommandRow.client_order_id
                                == ExchangeOrderRow.client_order_id,
                            )
                            .where(
                                ExecutionCommandRow.details["scope"][
                                    "environment"
                                ].astext
                                == environment,
                                ExecutionCommandRow.details["scope"][
                                    "account_label"
                                ].astext
                                == account_label,
                                active_execution_command_condition(),
                            )
                        )
                    ).all()
                    if active_cmd_rows:
                        active_cmd = active_cmd_rows[0]
                        raise _DecisionCommitConflict(
                            f"account {account_label} has active command {active_cmd.command_id} "
                            "but no durable head; recovery blocked"
                        )
                    return ()
            else:
                head_row = await session.get(
                    ExecutionBookHeadRow,
                    _execution_position_values(key),
                )
                if head_row is None:
                    scopes = await self._journal_store.list_scopes_in_session(
                        session,
                        environment=environment,
                        account_label=account_label,
                        key=key,
                    )
                    if scopes:
                        raise _DecisionCommitConflict(
                            f"position {key.canonical_id} has existing journal facts "
                            "but no durable head; recovery blocked"
                        )
                    active_res = (
                        await session.scalars(
                            select(PositionReservationRow).where(
                                PositionReservationRow.environment == environment,
                                PositionReservationRow.account_label == account_label,
                                PositionReservationRow.symbol == key.symbol,
                                PositionReservationRow.position_side
                                == key.position_side.value,
                                PositionReservationRow.reserved_quantity
                                > (
                                    PositionReservationRow.consumed_quantity
                                    + PositionReservationRow.released_quantity
                                ),
                            )
                        )
                    ).first()
                    if active_res is not None:
                        raise _DecisionCommitConflict(
                            f"position {key.canonical_id} has active reservation {active_res.reservation_id} "
                            "but no durable head; recovery blocked"
                        )
                    active_cmd = (
                        await session.scalars(
                            select(ExecutionCommandRow)
                            .outerjoin(
                                ExchangeOrderRow,
                                ExecutionCommandRow.client_order_id
                                == ExchangeOrderRow.client_order_id,
                            )
                            .where(
                                ExecutionCommandRow.details["scope"][
                                    "environment"
                                ].astext
                                == environment,
                                ExecutionCommandRow.details["scope"][
                                    "account_label"
                                ].astext
                                == account_label,
                                ExecutionCommandRow.details["scope"]["symbol"].astext
                                == key.symbol,
                                ExecutionCommandRow.details["scope"][
                                    "position_side"
                                ].astext
                                == key.position_side.value,
                                active_execution_command_condition(),
                            )
                        )
                    ).first()
                    if active_cmd is not None:
                        raise _DecisionCommitConflict(
                            f"position {key.canonical_id} has active command {active_cmd.command_id} "
                            "but no durable head; recovery blocked"
                        )
                    return ()
                heads_by_position[(head_row.symbol, head_row.position_side)] = head_row
                scopes = (
                    AccountFactStreamScope.for_position_key(
                        key,
                        stream_id=head_row.stream_id,
                        stream_epoch=head_row.stream_epoch,
                    ),
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
                head_row = heads_by_position.get((key.symbol, key.position_side.value))
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

                if head is None or "projection" not in head.state_payload:
                    raise _DecisionCommitConflict(
                        f"position {key.canonical_id} requires a valid durable head "
                        "with projection for recovery"
                    )

                try:
                    payload = head.state_payload
                    projection = PositionRecoveryCodec.decode_projection(
                        payload["projection"]
                    )
                    if payload.get("projection_digest"):
                        computed_proj_digest = (
                            PositionRecoveryCodec.compute_projection_digest(projection)
                        )
                        if payload["projection_digest"] != computed_proj_digest:
                            raise _DecisionCommitConflict(
                                f"execution head projection digest mismatch for {key.canonical_id}: "
                                f"stored={payload['projection_digest']} computed={computed_proj_digest}"
                            )
                    coverage = (
                        PositionRecoveryCodec.decode_coverage(payload["coverage"])
                        if payload.get("coverage")
                        else None
                    )
                    facts_hash = str(payload.get("facts_hash", ""))
                    journal_revision = int(payload.get("journal_revision", 0))
                    cp_binding = payload.get("recovery_checkpoint")
                    cp_id = (
                        cp_binding.get("checkpoint_id")
                        if isinstance(cp_binding, dict)
                        else None
                    )
                    trade_ids = tuple(payload.get("active_trade_ids") or ())
                    fills: tuple[Any, ...] = ()
                    if "active_fills" in payload:
                        fills = tuple(
                            PositionRecoveryCodec.decode_fill(item)
                            for item in payload["active_fills"]
                        )
                    elif trade_ids:
                        fill_stmt = select(AccountFillEventRow).where(
                            AccountFillEventRow.environment == key.environment,
                            AccountFillEventRow.account_label == key.account_label,
                            AccountFillEventRow.symbol == key.symbol,
                            AccountFillEventRow.trade_id.in_(trade_ids),
                        )
                        fill_rows = (await session.scalars(fill_stmt)).all()
                        fill_map = {
                            row.trade_id: account_fill_from_row(row)
                            for row in fill_rows
                        }
                        fills = tuple(
                            fill_map[tid] for tid in trade_ids if tid in fill_map
                        )
                    if "active_fills" in payload:
                        await self._require_active_fills_consistency(
                            key=key,
                            trade_ids=trade_ids,
                            fills=fills,
                            active_fills_present=True,
                        )
                        await self._require_active_fills_identity(
                            session,
                            key=key,
                            fills=fills,
                        )
                    elif trade_ids:
                        # The head names its active trade identities without
                        # inlining the fills: every named identity must resolve
                        # to a durable fill, otherwise the current position is
                        # incomplete and the range must block.
                        await self._require_active_fills_consistency(
                            key=key,
                            trade_ids=trade_ids,
                            fills=fills,
                            active_fills_present=False,
                        )

                    boundaries: list[ExitOrderSubmissionFact] = []
                    if "active_exit_boundaries" in payload:
                        boundaries.extend(
                            PositionRecoveryCodec.decode_boundary(item)
                            for item in payload["active_exit_boundaries"]
                        )
                    elif "exit_boundaries" in payload:
                        boundaries.extend(
                            PositionRecoveryCodec.decode_boundary(item)
                            for item in payload["exit_boundaries"]
                        )
                    if projection.active_episode is not None:
                        for batch in projection.active_episode.batches:
                            if batch.exit_order_submitted_at is not None:
                                if not any(
                                    b.target_batch_id == batch.batch_id
                                    for b in boundaries
                                ):
                                    boundaries.append(
                                        ExitOrderSubmissionFact(
                                            order_id=f"boundary:{batch.batch_id}",
                                            submitted_at=batch.exit_order_submitted_at,
                                            symbol=key.symbol,
                                            position_side=key.position_side,
                                            client_order_id=batch.client_order_id,
                                            target_batch_id=batch.batch_id,
                                        )
                                    )
                    snapshots: list[Any] = []
                    if "active_snapshots" in payload:
                        snapshots.extend(
                            PositionRecoveryCodec.decode_snapshot(item)
                            for item in payload["active_snapshots"]
                        )
                    else:
                        snap_stmt = select(PositionFactJournalEventRow).where(
                            PositionFactJournalEventRow.environment
                            == scope.environment,
                            PositionFactJournalEventRow.account_label
                            == scope.account_label,
                            PositionFactJournalEventRow.symbol == scope.symbol,
                            PositionFactJournalEventRow.position_side
                            == scope.position_side.value,
                            PositionFactJournalEventRow.stream_id == scope.stream_id,
                            PositionFactJournalEventRow.stream_epoch
                            == scope.stream_epoch,
                            PositionFactJournalEventRow.event_kind == "snapshot",
                        )
                        snap_rows = (await session.scalars(snap_stmt)).all()
                        snapshots.extend(
                            PositionRecoveryCodec.decode_snapshot(r.payload)
                            for r in snap_rows
                        )
                except _DecisionCommitConflict:
                    raise
                except Exception as err:
                    raise _DecisionCommitConflict(
                        f"corrupted execution head payload for {key.canonical_id}: {err}"
                    ) from err

                checkpoint: PositionRecoveryCheckpoint | None = None
                if cp_id:
                    checkpoint = (
                        await self._journal_store.load_checkpoint_by_id_in_session(
                            session,
                            scope=scope,
                            checkpoint_id=cp_id,
                        )
                    )
                if checkpoint is not None:
                    eff_as_of = max(
                        [as_of, checkpoint.event_cut] + [f.trade_at for f in fills]
                    )
                    prefix_complete = False
                elif cp_id:
                    valid_cuts = [
                        c
                        for c in [
                            projection.event_cut,
                            projection.high_watermark_trade_at,
                            coverage.end_at if coverage else None,
                            _max_projection_timestamp(projection),
                        ]
                        if c is not None
                    ]
                    eff_cut = max(valid_cuts) if valid_cuts else as_of
                    eff_as_of = max([as_of, eff_cut] + [f.trade_at for f in fills])
                    cp_facts_hash = (
                        str(cp_binding.get("facts_hash"))
                        if isinstance(cp_binding, dict) and cp_binding.get("facts_hash")
                        else facts_hash
                    )
                    try:
                        checkpoint = PositionRecoveryCheckpoint(
                            key=key,
                            stream_scope=scope,
                            event_cut=eff_cut,
                            projection=projection,
                            coverage=coverage,
                            facts_hash=cp_facts_hash,
                            source_revision=journal_revision,
                            checkpoint_id=cp_id,
                        )
                    except Exception as err:
                        # A malformed binding must be an explicit recovery
                        # blocker, not a raw schema error that surfaces as an
                        # unexpected crash in the runtime.
                        raise _DecisionCommitConflict(
                            f"corrupted recovery checkpoint binding for "
                            f"{key.canonical_id}: {err}"
                        ) from err
                    prefix_complete = False
                else:
                    eff_as_of = max([as_of] + [f.trade_at for f in fills])
                    prefix_complete = True

                if checkpoint is not None:
                    suffix_fills = tuple(
                        f for f in fills if f.trade_at > checkpoint.event_cut
                    )
                    suffix_boundaries = tuple(
                        b for b in boundaries if b.submitted_at > checkpoint.event_cut
                    )
                    suffix_snapshots = tuple(
                        s for s in snapshots if s.observed_at > checkpoint.event_cut
                    )
                else:
                    suffix_fills = fills
                    suffix_boundaries = tuple(boundaries)
                    suffix_snapshots = tuple(snapshots)

                facts = AccountFacts(
                    position_key=key,
                    stream_scope=scope,
                    coverage=coverage,
                    recovery_checkpoint=checkpoint,
                    fills=suffix_fills,
                    snapshots=suffix_snapshots,
                    exit_boundaries=suffix_boundaries,
                    prefix_facts_complete=prefix_complete,
                )
                cut = DurableJournalCut(
                    scope=scope,
                    facts=facts,
                    revision=journal_revision,
                    as_of=eff_as_of,
                    checkpoint=checkpoint,
                )
                latest_evidence_seq = await session.scalar(
                    select(func.max(ExecutionEvidenceReceiptRow.sequence)).where(
                        *_execution_scope_conditions(key, scope)
                    )
                )
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
                        trade_ids=trade_ids,
                        evidence_ids=(),
                        watermarks=tuple(
                            _ExecutionWatermark(
                                order_id=row.order_id,
                                cumulative_quantity=row.cumulative_quantity,
                                cumulative_quote=row.cumulative_quote,
                                updated_at=row.updated_at,
                            )
                            for row in watermark_rows
                        ),
                        latest_evidence_sequence=latest_evidence_seq,
                    )
                )
        return tuple(recovered)

    async def _require_complete_head_coverage(
        self,
        session: AsyncSession,
        *,
        environment: str,
        account_label: str,
        heads_by_position: Mapping[tuple[str, str], ExecutionBookHeadRow],
    ) -> None:
        """Block recovery unless every durable responsibility has a current head.

        A head is the only record of an active position.  Enumerating only the
        heads that happen to exist cannot detect a deleted or never-written head
        whose active entry fill, journal fact, checkpoint, order watermark or
        evidence receipt is still present.  This audit enumerates the full
        durable responsibility set for the account and requires a head for each
        position identity, so a partial corruption is an explicit blocker rather
        than a silently dropped position.
        """

        head_keys = set(heads_by_position)
        journal_scopes = await self._journal_store.list_scopes_in_session(
            session,
            environment=environment,
            account_label=account_label,
        )
        for scope in journal_scopes:
            key = (scope.symbol, scope.position_side.value)
            if key not in head_keys:
                raise _DecisionCommitConflict(
                    f"position {scope.symbol}:{key[1]} has durable journal facts or "
                    "checkpoints but no durable head; recovery blocked"
                )
        for model, label in (
            (ExecutionOrderWatermarkRow, "order watermark"),
            (ExecutionEvidenceReceiptRow, "evidence receipt"),
            (ExecutionTradeIdentityRow, "trade identity"),
        ):
            rows = (
                await session.execute(
                    select(model.symbol, model.position_side)
                    .where(
                        model.environment == environment,
                        model.account_label == account_label,
                    )
                    .distinct()
                )
            ).all()
            for symbol, position_side in rows:
                if (symbol, position_side) not in head_keys:
                    raise _DecisionCommitConflict(
                        f"position {symbol}:{position_side} has durable {label} "
                        "but no durable head; recovery blocked"
                    )
        # `account_fill_events` is keyed by (symbol, trade_id) and carries no
        # position side.  A symbol with any head is covered; a symbol with none
        # is an active entry fill whose position responsibility lost its head.
        fill_symbols = (
            await session.scalars(
                select(AccountFillEventRow.symbol)
                .where(
                    AccountFillEventRow.environment == environment,
                    AccountFillEventRow.account_label == account_label,
                )
                .distinct()
            )
        ).all()
        for symbol in fill_symbols:
            if not any(head_symbol == symbol for head_symbol, _side in head_keys):
                raise _DecisionCommitConflict(
                    f"symbol {symbol} has durable active entry fills but no durable "
                    "head for any position side; recovery blocked"
                )

    async def _require_active_fills_consistency(
        self,
        *,
        key: PositionKey,
        trade_ids: tuple[str, ...],
        fills: tuple[Any, ...],
        active_fills_present: bool,
    ) -> None:
        """Require the head's named trade identities and its fills to agree.

        A checkpoint folds the *prefix*, not the identity of the active fills, so
        the same completeness contract applies on both branches: the set of
        trade identities the head names must equal the set of fills it carries,
        with no duplicates and no missing member.  Validating only the identity
        of the fills that happen to remain would silently accept a dropped fill.
        """

        fill_ids = [fill.trade_id for fill in fills]
        if len(set(fill_ids)) != len(fill_ids):
            raise _DecisionCommitConflict(
                f"execution head for {key.canonical_id} carries duplicate fill "
                "identities; recovery blocked"
            )
        if len(set(trade_ids)) != len(trade_ids):
            raise _DecisionCommitConflict(
                f"execution head for {key.canonical_id} repeats active trade "
                "identities; recovery blocked"
            )
        if not active_fills_present:
            missing = [
                trade_id for trade_id in trade_ids if trade_id not in set(fill_ids)
            ]
            if missing:
                raise _DecisionCommitConflict(
                    f"execution head for {key.canonical_id} names active trade "
                    f"identities without durable fills ({', '.join(missing)}); "
                    "recovery blocked"
                )
            return
        named = set(trade_ids)
        carried = set(fill_ids)
        if named != carried:
            missing = sorted(named - carried)
            extra = sorted(carried - named)
            detail = []
            if missing:
                detail.append(f"missing {', '.join(missing)}")
            if extra:
                detail.append(f"unnamed {', '.join(extra)}")
            raise _DecisionCommitConflict(
                f"execution head for {key.canonical_id} active fills do not match "
                f"its active trade identities ({'; '.join(detail)}); recovery blocked"
            )

    async def _require_active_fills_identity(
        self,
        session: AsyncSession,
        *,
        key: PositionKey,
        fills: tuple[Any, ...],
    ) -> None:
        """Cross-check head active fills against durable trade identities.

        A checkpoint makes the head's ``facts_hash`` a suffix-only digest, so it
        cannot be recomputed from the payload alone.  The durable trade identity
        table is independent of the payload and records the real quantity of
        every accepted fill, so comparing it against the head's active fills
        detects tampering, missing and duplicated identities even when a
        recovery checkpoint is present.
        """

        seen: dict[str, Decimal] = {}
        for fill in fills:
            trade_id = fill.trade_id
            if trade_id in seen:
                raise _DecisionCommitConflict(
                    f"execution head for {key.canonical_id} repeats active fill "
                    f"identity {trade_id}; recovery blocked"
                )
            seen[trade_id] = fill.quantity
        if not seen:
            return
        identity_rows = (
            await session.scalars(
                select(ExecutionTradeIdentityRow).where(
                    ExecutionTradeIdentityRow.environment == key.environment,
                    ExecutionTradeIdentityRow.account_label == key.account_label,
                    ExecutionTradeIdentityRow.symbol == key.symbol,
                    ExecutionTradeIdentityRow.position_side == key.position_side.value,
                    ExecutionTradeIdentityRow.trade_id.in_(tuple(seen)),
                )
            )
        ).all()
        identities = {row.trade_id: row for row in identity_rows}
        for trade_id, quantity in seen.items():
            row = identities.get(trade_id)
            if row is None:
                raise _DecisionCommitConflict(
                    f"execution head for {key.canonical_id} carries active fill "
                    f"{trade_id} with no durable trade identity; recovery blocked"
                )
            if row.quantity != quantity:
                raise _DecisionCommitConflict(
                    f"execution head for {key.canonical_id} active fill {trade_id} "
                    f"quantity {quantity} conflicts with durable identity "
                    f"{row.quantity}; recovery blocked"
                )

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


from .decision_unit_of_work import (  # noqa: E402
    AsyncPostgresDecisionUnitOfWork,
)

__all__ = ["AsyncPostgresDecisionUnitOfWork"]
