"""Atomic PostgreSQL commit boundaries for execution and live decisions."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
from crypto_momentum_lab.domain.execution.execution_head import (
    EXECUTION_HEAD_SCHEMA_VERSION,
    decode_current_facts,
    decode_execution_head_state,
    is_supported_execution_head_schema,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    PositionRecoveryCheckpoint,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.domain.market.models import JsonValue
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
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    ExecutionCommandRow,
    PositionReservationRow,
)
from crypto_momentum_lab.persistence.postgres.order_rejection import (
    execute_order_rejection_cascade,
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
        command_repository: ExecutionCommandStore,
        reservation_repository: ExecutionReservationStore,
    ) -> None:
        self.session = session
        self._command_repository = command_repository
        self._reservation_repository = reservation_repository

    async def load_current_cut(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> DurableJournalCut:
        """Read the current facts directly from their one durable head."""
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("current execution read as_of must be timezone-aware")
        key = PositionKey(
            scope.environment, scope.account_label, scope.symbol, scope.position_side
        )
        head = await self.session.get(
            ExecutionBookHeadRow, _execution_position_values(key)
        )
        if head is None or (head.stream_id, head.stream_epoch) != (
            scope.stream_id,
            scope.stream_epoch,
        ):
            raise _DecisionCommitConflict(
                f"position {key.canonical_id} has no matching current head"
            )
        payload = head.state_payload
        if not is_supported_execution_head_schema(payload.get("schema_version")):
            raise _DecisionCommitConflict(
                "execution head requires offline format migration"
            )
        try:
            facts = decode_current_facts(payload, key=key, scope=scope)
        except Exception as err:
            raise _DecisionCommitConflict(
                "current execution head facts are invalid"
            ) from err
        if facts.position_key != key or facts.stream_scope != scope:
            raise _DecisionCommitConflict(
                "current execution head facts identity mismatch"
            )
        as_of = max(as_of, _latest_fact_time(facts, default=as_of))
        return DurableJournalCut(
            scope=scope,
            facts=facts,
            revision=head.revision,
            as_of=as_of,
            checkpoint=facts.recovery_checkpoint,
        )

    async def load_checkpoint_from_head(
        self,
        *,
        scope: AccountFactStreamScope,
        checkpoint_id: str,
    ) -> PositionRecoveryCheckpoint | None:
        """Read a checkpoint embedded in the current position head."""
        key = PositionKey(
            scope.environment, scope.account_label, scope.symbol, scope.position_side
        )
        head = await self.session.get(
            ExecutionBookHeadRow, _execution_position_values(key)
        )
        if head is None or (head.stream_id, head.stream_epoch) != (
            scope.stream_id,
            scope.stream_epoch,
        ):
            return None
        payload = head.state_payload
        if not is_supported_execution_head_schema(payload.get("schema_version")):
            raise _DecisionCommitConflict(
                "execution head requires offline format migration"
            )
        try:
            facts = decode_current_facts(payload, key=key, scope=scope)
        except Exception as err:
            raise _DecisionCommitConflict(
                "current execution head facts are invalid"
            ) from err
        checkpoint = facts.recovery_checkpoint
        if checkpoint is None or checkpoint.checkpoint_id != checkpoint_id:
            return None
        return checkpoint

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
        if state_payload.get("schema_version") != EXECUTION_HEAD_SCHEMA_VERSION:
            raise _DecisionCommitConflict(
                f"new execution writes require head schema {EXECUTION_HEAD_SCHEMA_VERSION}"
            )
        try:
            state_book = decode_execution_head_state(
                state_payload,
                key=key,
                scope=AccountFactStreamScope.for_position_key(
                    key, stream_id=stream_id, stream_epoch=stream_epoch
                ),
            )
        except Exception as err:
            raise _DecisionCommitConflict(
                "new execution head position state is invalid"
            ) from err
        expected_scope = state_book.stream_scope
        if expected_scope != AccountFactStreamScope.for_position_key(
            key, stream_id=stream_id, stream_epoch=stream_epoch
        ):
            raise _DecisionCommitConflict("new execution head stream identity mismatch")
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
            checkpoint = state_book.current_facts.recovery_checkpoint
            if (
                checkpoint is None
                or checkpoint.checkpoint_id != stream_adoption_checkpoint_id
                or checkpoint.stream_scope != expected_scope
            ):
                raise _DecisionCommitConflict(
                    "execution stream adoption checkpoint is missing from the new head"
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


def _latest_fact_time(facts: object, *, default: datetime) -> datetime:
    times = [
        *(fill.trade_at for fill in facts.fills),
        *(fill.trade_at for fill in facts.late_fills),
        *(snapshot.observed_at for snapshot in facts.snapshots),
        *(boundary.submitted_at for boundary in facts.exit_boundaries),
    ]
    if facts.coverage is not None:
        times.append(facts.coverage.end_at)
        for value in (
            facts.coverage.evidence_observed_at,
            facts.coverage.checkpoint_event_cut,
        ):
            if value is not None:
                times.append(value)
    if facts.fill_load_provenance is not None:
        times.append(facts.fill_load_provenance.observed_at)
    if facts.recovery_checkpoint is not None:
        times.append(facts.recovery_checkpoint.event_cut)
    return max((default, *times))


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
        command_repository: ExecutionCommandStore,
        reservation_repository: ExecutionReservationStore,
    ) -> None:
        self._session_factory = session_factory
        self._command_repository = command_repository
        self._reservation_repository = reservation_repository

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
        """Restore current positions from their heads and separate obligations."""
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        async with self._session_factory() as session:
            bind = session.get_bind()
            if bind is None or bind.dialect.name != "postgresql":
                raise RuntimeError("durable position restoration requires PostgreSQL")

            if key is None:
                rows = (
                    await session.scalars(
                        select(ExecutionBookHeadRow).where(
                            ExecutionBookHeadRow.environment == environment,
                            ExecutionBookHeadRow.account_label == account_label,
                        )
                    )
                ).all()
            else:
                row = await session.get(
                    ExecutionBookHeadRow, _execution_position_values(key)
                )
                rows = () if row is None else (row,)
            heads = {(row.symbol, row.position_side): row for row in rows}
            await self._require_complete_head_coverage(
                session,
                environment=environment,
                account_label=account_label,
                heads_by_position=heads,
                key=key,
            )

            states: list[_DurableExecutionPositionState] = []
            for row in rows:
                position_key = PositionKey(
                    environment=row.environment,
                    account_label=row.account_label,
                    symbol=row.symbol,
                    position_side=FuturesPositionSide(row.position_side),
                )
                scope = AccountFactStreamScope.for_position_key(
                    position_key,
                    stream_id=row.stream_id,
                    stream_epoch=row.stream_epoch,
                )
                head = _ExecutionHeadSnapshot(
                    revision=row.revision,
                    stream_id=row.stream_id,
                    stream_epoch=row.stream_epoch,
                    projection_version=row.projection_version,
                    state_payload=dict(row.state_payload),
                )
                try:
                    payload = head.state_payload
                    state_book = decode_execution_head_state(
                        payload, key=position_key, scope=scope
                    )
                    facts = state_book.current_facts
                    sequence = payload.get("last_sequence")
                    if sequence is not None and (
                        type(sequence) is not int or sequence < 0
                    ):
                        raise ValueError("head sequence is invalid")
                    for field in (
                        "active_reservation_ids",
                        "external_recovery_ids",
                        "recovery_command_ids",
                    ):
                        values = payload.get(field)
                        if not isinstance(values, list) or any(
                            not isinstance(value, str) or not value for value in values
                        ):
                            raise ValueError(f"head {field} is invalid")
                except Exception as err:
                    raise _DecisionCommitConflict(
                        f"invalid execution head for {position_key.canonical_id}: {err}"
                    ) from err

                event_times = [
                    *(
                        item
                        for item in (state_book.latest_trade_at, state_book.projection.event_cut)
                        if item is not None
                    ),
                    *(snapshot.observed_at for snapshot in facts.snapshots),
                    *(boundary.submitted_at for boundary in facts.exit_boundaries),
                ]
                if facts.coverage is not None:
                    event_times.append(facts.coverage.end_at)
                    if facts.coverage.evidence_observed_at is not None:
                        event_times.append(facts.coverage.evidence_observed_at)
                    if facts.coverage.checkpoint_event_cut is not None:
                        event_times.append(facts.coverage.checkpoint_event_cut)
                if facts.fill_load_provenance is not None:
                    event_times.append(facts.fill_load_provenance.observed_at)
                if facts.recovery_checkpoint is not None:
                    event_times.append(facts.recovery_checkpoint.event_cut)
                cut = DurableJournalCut(
                    scope=scope,
                    facts=facts,
                    revision=row.revision,
                    as_of=max((as_of, *event_times)),
                    checkpoint=facts.recovery_checkpoint,
                )
                trade_rows = (
                    await session.scalars(
                        select(ExecutionTradeIdentityRow.trade_id).where(
                            ExecutionTradeIdentityRow.environment
                            == position_key.environment,
                            ExecutionTradeIdentityRow.account_label
                            == position_key.account_label,
                            ExecutionTradeIdentityRow.symbol == position_key.symbol,
                            ExecutionTradeIdentityRow.position_side
                            == position_key.position_side.value,
                        )
                    )
                ).all()
                latest_evidence_sequence = await session.scalar(
                    select(func.max(ExecutionEvidenceReceiptRow.sequence)).where(
                        *_execution_scope_conditions(position_key, scope)
                    )
                )
                watermark_rows = (
                    await session.scalars(
                        select(ExecutionOrderWatermarkRow).where(
                            ExecutionOrderWatermarkRow.environment == environment,
                            ExecutionOrderWatermarkRow.account_label == account_label,
                            ExecutionOrderWatermarkRow.symbol == row.symbol,
                            ExecutionOrderWatermarkRow.position_side
                            == row.position_side,
                        )
                    )
                ).all()
                states.append(
                    _DurableExecutionPositionState(
                        scope=scope,
                        cut=cut,
                        head=head,
                        trade_ids=tuple(trade_rows),
                        evidence_ids=(),
                        watermarks=tuple(
                            _ExecutionWatermark(
                                order_id=watermark.order_id,
                                cumulative_quantity=watermark.cumulative_quantity,
                                cumulative_quote=watermark.cumulative_quote,
                                updated_at=watermark.updated_at,
                            )
                            for watermark in watermark_rows
                        ),
                        latest_evidence_sequence=latest_evidence_sequence,
                    )
                )
            return tuple(states)

    async def _require_complete_head_coverage(
        self,
        session: AsyncSession,
        *,
        environment: str,
        account_label: str,
        heads_by_position: Mapping[tuple[str, str], ExecutionBookHeadRow],
        key: PositionKey | None = None,
    ) -> None:
        """Load current state from heads and the independent live obligations."""
        head_keys = set(heads_by_position)

        for model, label in (
            (ExecutionOrderWatermarkRow, "order watermark"),
            (ExecutionEvidenceReceiptRow, "evidence receipt"),
            (ExecutionTradeIdentityRow, "trade identity"),
        ):
            conditions = [
                model.environment == environment,
                model.account_label == account_label,
            ]
            if key is not None:
                conditions.extend(
                    (
                        model.symbol == key.symbol,
                        model.position_side == key.position_side.value,
                    )
                )
            rows = (
                await session.execute(
                    select(model.symbol, model.position_side)
                    .where(*conditions)
                    .distinct()
                )
            ).all()
            for symbol, position_side in rows:
                if (symbol, position_side) not in head_keys:
                    raise _DecisionCommitConflict(
                        f"position {symbol}:{position_side} has durable {label} "
                        "but no durable head; recovery blocked"
                    )

        reservation_conditions = [
            PositionReservationRow.environment == environment,
            PositionReservationRow.account_label == account_label,
            PositionReservationRow.reserved_quantity
            > (
                PositionReservationRow.consumed_quantity
                + PositionReservationRow.released_quantity
            ),
        ]
        if key is not None:
            reservation_conditions.extend(
                (
                    PositionReservationRow.symbol == key.symbol,
                    PositionReservationRow.position_side == key.position_side.value,
                )
            )
        reservations = (
            await session.scalars(
                select(PositionReservationRow).where(*reservation_conditions)
            )
        ).all()
        for reservation in reservations:
            if (reservation.symbol, reservation.position_side) not in head_keys:
                raise _DecisionCommitConflict(
                    f"position {reservation.symbol}:{reservation.position_side} "
                    "has active "
                    f"reservation {reservation.reservation_id} but no durable head"
                )

        command_conditions = [
            ExecutionCommandRow.details["scope"]["environment"].astext == environment,
            ExecutionCommandRow.details["scope"]["account_label"].astext
            == account_label,
            active_execution_command_condition(),
        ]
        if key is not None:
            command_conditions.extend(
                (
                    ExecutionCommandRow.details["scope"]["symbol"].astext == key.symbol,
                    ExecutionCommandRow.details["scope"]["position_side"].astext
                    == key.position_side.value,
                )
            )
        commands = (
            await session.scalars(
                select(ExecutionCommandRow)
                .outerjoin(
                    ExchangeOrderRow,
                    ExecutionCommandRow.client_order_id
                    == ExchangeOrderRow.client_order_id,
                )
                .where(*command_conditions)
            )
        ).all()
        for command in commands:
            command_scope = command.details.get("scope", {})
            symbol = command_scope.get("symbol")
            position_side = command_scope.get("position_side")
            if symbol and position_side and (symbol, position_side) not in head_keys:
                raise _DecisionCommitConflict(
                    f"position {symbol}:{position_side} has active command "
                    f"{command.command_id} but no durable head"
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
                    command_repository=self._command_repository,
                    reservation_repository=self._reservation_repository,
                )


from .decision_unit_of_work import (  # noqa: E402
    AsyncPostgresDecisionUnitOfWork,
)

__all__ = ["AsyncPostgresDecisionUnitOfWork"]
