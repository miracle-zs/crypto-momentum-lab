"""Atomic PostgreSQL commit boundaries for execution and live decisions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.decision.decision_engine import PolicyState
from crypto_momentum_lab.domain.decision.policy_transition import (
    canonicalize_policy_value,
    compute_policy_state_digest,
    serialize_policy_state,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExitAllocation,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    DurableExecutionPositionState,
    ExecutionEvidenceIdentity,
    ExecutionHeadSnapshot,
    ExecutionTradeIdentity,
    ExecutionWatermark,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.operational.retention_models import ConsumerDependency
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    DurableDecisionExitRow,
    DurablePolicyCommitRow,
    DurablePolicyStateRow,
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionOrderWatermarkRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionTraceRow,
    MarketRevisionRefRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.retention_repository import (
    AsyncPostgresRetentionRepository,
)


@dataclass(frozen=True, slots=True)
class DecisionCommit:
    """Complete, typed input for an atomic live decision commit."""

    trace: DecisionTrace
    policy_key: str
    expected_policy_revision: int
    expected_prior_digest: str
    prior_policy_state: PolicyState
    next_policy_state: PolicyState
    dependencies: tuple[ConsumerDependency, ...] = ()
    accepted_exit: TradeCommand | None = None

    def __post_init__(self) -> None:
        if not self.policy_key.strip():
            raise ValueError("policy_key must not be empty")
        if self.expected_policy_revision < 0:
            raise ValueError("expected_policy_revision must be non-negative")
        if not self.expected_prior_digest.strip():
            raise ValueError("expected_prior_digest must not be empty")
        if not self.trace.decision_id.strip():
            raise ValueError("decision trace id must not be empty")


@dataclass(frozen=True, slots=True)
class DecisionCommitReceipt:
    decision_id: str
    policy_key: str
    prior_state_digest: str
    next_state_digest: str
    policy_revision: int
    durable_at: datetime
    pending_exit_id: str | None = None
    is_replay: bool = False


@dataclass(frozen=True, slots=True)
class DurablePolicySnapshot:
    state: PolicyState
    state_digest: str
    revision: int
    last_decision_id: str


class ExecutionTransaction:
    """Session-bound writes used by one staged ExecutionBook operation."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        journal_store: Any,
        order_repository: PostgresOrderRepository,
        reservation_repository: AsyncPostgresPositionReservationRepository,
    ) -> None:
        self.session = session
        self._journal_store = journal_store
        self._order_repository = order_repository
        self._reservation_repository = reservation_repository

    async def persist_facts(
        self,
        *,
        scope: Any,
        facts: Any,
        revision: int,
        checkpoint: Any | None = None,
        delta: Any | None = None,
    ) -> Any:
        result = await self._journal_store.persist_facts_in_session(
            self.session,
            scope=scope,
            facts=facts,
            revision=revision,
            delta=delta,
        )
        fact_checkpoint = getattr(facts, "recovery_checkpoint", None)
        if checkpoint is not None and fact_checkpoint is not None and (
            checkpoint != fact_checkpoint
        ):
            raise DecisionCommitConflict(
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
    ) -> Any:
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
    ) -> Any | None:
        """Verify an immutable parent checkpoint in the active transaction."""
        return await self._journal_store.load_checkpoint_by_id_in_session(
            self.session,
            scope=scope,
            checkpoint_id=checkpoint_id,
        )

    async def save_reservations(
        self,
        reservations: Sequence[Any],
        *,
        expected_projection_version: str | None = None,
        batch_quantities: Mapping[str, Decimal] | None = None,
        proven_position_quantity: Decimal | None = None,
    ) -> None:
        await self._reservation_repository.save_reservations_in_session(
            self.session,
            reservations,
            expected_projection_version=expected_projection_version,
            batch_quantities=batch_quantities,
            proven_position_quantity=proven_position_quantity,
        )

    async def update_reservation(
        self,
        reservation: Any,
        *,
        release_reason: str | None = None,
    ) -> None:
        await self._reservation_repository.update_reservation_in_session(
            self.session,
            reservation,
            release_reason=release_reason,
        )

    async def upsert_outbox(self, **values: Any) -> None:
        await self._order_repository.upsert_execution_command_in_session(
            self.session, **values
        )

    async def record_evidence(
        self,
        *,
        key: PositionKey,
        stream_id: str,
        stream_epoch: str,
        evidence: ExecutionEvidenceIdentity,
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
                raise DecisionCommitConflict(
                    f"evidence {evidence.evidence_id} was reused with different data"
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
        trade: ExecutionTradeIdentity,
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
                raise DecisionCommitConflict(
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
        watermark: ExecutionWatermark,
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
                    **dict(zip(_EXECUTION_POSITION_FIELDS, position_identity, strict=True)),
                    order_id=watermark.order_id,
                    cumulative_quantity=watermark.cumulative_quantity,
                    cumulative_quote=watermark.cumulative_quote,
                    updated_at=watermark.updated_at,
                )
            )
            return
        if watermark.cumulative_quantity < row.cumulative_quantity:
            raise DecisionCommitConflict("execution quantity watermark regressed")
        if watermark.cumulative_quote < row.cumulative_quote:
            raise DecisionCommitConflict("execution quote watermark regressed")
        if (
            watermark.cumulative_quantity == row.cumulative_quantity
            and watermark.cumulative_quote != row.cumulative_quote
        ):
            raise DecisionCommitConflict(
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
                raise DecisionCommitConflict(
                    "execution stream adoption checkpoint is not in this transaction"
                )
        if row is not None and (
            row.stream_id != stream_id or row.stream_epoch != stream_epoch
        ):
            if not stream_adoption_checkpoint_id and not is_flat_adoption:
                raise DecisionCommitConflict(
                    "execution stream changed without a validated recovery checkpoint"
                )
            if not is_flat_adoption and stream_adoption_checkpoint_id is None:
                raise DecisionCommitConflict(
                    "execution stream adoption checkpoint could not be verified"
                )
        current_revision = row.revision if row is not None else 0
        if current_revision != expected_revision:
            raise DecisionCommitConflict(
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

    async def load_head(self, key: PositionKey) -> ExecutionHeadSnapshot | None:
        row = await self.session.get(
            ExecutionBookHeadRow,
            _execution_position_values(key),
            with_for_update=True,
        )
        if row is None:
            return None
        return ExecutionHeadSnapshot(
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
        journal_store: Any,
        order_repository: PostgresOrderRepository,
        reservation_repository: AsyncPostgresPositionReservationRepository,
    ) -> None:
        self._session_factory = session_factory
        self._journal_store = journal_store
        self._order_repository = order_repository
        self._reservation_repository = reservation_repository

    async def load_journal_cut(
        self,
        *,
        scope: AccountFactStreamScope,
        as_of: datetime,
    ) -> Any:
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

    async def load_positions(
        self,
        *,
        environment: str,
        account_label: str,
        as_of: datetime,
    ) -> tuple[DurableExecutionPositionState, ...]:
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

            recovered: list[DurableExecutionPositionState] = []
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
                    ExecutionHeadSnapshot(
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
                    scope = next(
                        (
                            candidate
                            for candidate in scoped_rows
                            if candidate.stream_id == head.stream_id
                            and candidate.stream_epoch == head.stream_epoch
                        ),
                        None,
                    )
                    if scope is None:
                        scope = AccountFactStreamScope.for_position_key(
                            key,
                            stream_id=head.stream_id,
                            stream_epoch=head.stream_epoch,
                        )
                elif len(scoped_rows) == 1:
                    scope = scoped_rows[0]
                else:
                    raise DecisionCommitConflict(
                        f"position {key.canonical_id} has multiple streams but no durable head"
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
                    DurableExecutionPositionState(
                        scope=scope,
                        cut=cut,
                        head=head,
                        trade_ids=tuple(row.trade_id for row in trade_rows),
                        evidence_ids=tuple(row.evidence_id for row in evidence_rows),
                        watermarks=tuple(
                            ExecutionWatermark(
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
    ) -> AsyncIterator[ExecutionTransaction]:
        async with self._session_factory() as session:
            async with session.begin():
                bind = session.get_bind()
                if bind is None or bind.dialect.name != "postgresql":
                    raise RuntimeError(
                        "durable execution commits require PostgreSQL transactions"
                    )
                await session.execute(text("SET LOCAL synchronous_commit = ON"))
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                    {"lock_key": f"execution_position:{key.canonical_id}"},
                )
                yield ExecutionTransaction(
                    session,
                    journal_store=self._journal_store,
                    order_repository=self._order_repository,
                    reservation_repository=self._reservation_repository,
                )


def _dependency_payload(dependency: ConsumerDependency) -> dict[str, Any]:
    spec = dependency.recovery_spec
    return {
        "consumer_id": dependency.consumer_id,
        "dataset_name": dependency.dataset_name,
        "generation": dependency.generation,
        "dependency_version": dependency.dependency_version,
        "updated_at": dependency.updated_at.astimezone(UTC).isoformat(),
        "recovery_spec": {
            "source_dataset": spec.source_dataset,
            "earliest_needed_watermark": spec.earliest_needed_watermark.astimezone(
                UTC
            ).isoformat(),
            "earliest_checkpoint_id": spec.earliest_checkpoint_id,
            "recovery_deadline": (
                spec.recovery_deadline.astimezone(UTC).isoformat()
                if spec.recovery_deadline is not None
                else None
            ),
            "cold_recovery_supported": spec.cold_recovery_supported,
            "reason": spec.reason,
        },
    }


def _trade_command_payload(command: TradeCommand) -> dict[str, Any]:
    plan = command.allocation_plan
    return {
        "command_id": command.command_id,
        "position_key": {
            "environment": command.position_key.environment,
            "account_label": command.position_key.account_label,
            "symbol": command.position_key.symbol,
            "position_side": command.position_key.position_side.value,
        },
        "command_type": command.command_type.value,
        "side": command.side.value,
        "order_type": command.order_type.value,
        "requested_quantity": str(command.requested_quantity),
        "limit_price": str(command.limit_price) if command.limit_price is not None else None,
        "reduce_only": command.reduce_only,
        "reason": command.reason,
        "created_at": command.created_at.astimezone(UTC).isoformat(),
        "fencing_token": command.fencing_token,
        "idempotency_key": command.idempotency_key,
        "expected_projection_version": command.expected_projection_version,
        "reservation_id": command.reservation_id,
        "allocation_plan": (
            {
                "allocations": [
                    {
                        "batch_id": allocation.batch_id,
                        "allocated_quantity": str(allocation.allocated_quantity),
                        "entry_price": str(allocation.entry_price),
                    }
                    for allocation in plan.allocations
                ],
                "total_allocated_quantity": str(plan.total_allocated_quantity),
                "policy": plan.policy.value,
                "absorbed_dust": str(plan.absorbed_dust),
                "unallocated_remainder": str(plan.unallocated_remainder),
                "reason": plan.reason,
                "projection_version": plan.projection_version,
                "reservation_id": plan.reservation_id,
                "batch_quantities": (
                    {key: str(value) for key, value in plan.batch_quantities.items()}
                    if plan.batch_quantities is not None
                    else None
                ),
            }
            if plan is not None
            else None
        ),
    }


def _trade_command_from_payload(payload: dict[str, Any]) -> TradeCommand:
    key_data = payload["position_key"]
    key = PositionKey(
        environment=str(key_data["environment"]),
        account_label=str(key_data["account_label"]),
        symbol=str(key_data["symbol"]),
        position_side=FuturesPositionSide(str(key_data["position_side"])),
    )
    plan_data = payload.get("allocation_plan")
    plan = None
    if isinstance(plan_data, dict):
        batch_quantities = plan_data.get("batch_quantities")
        plan = ExitAllocationPlan(
            position_key=key,
            allocations=tuple(
                ExitAllocation(
                    batch_id=str(row["batch_id"]),
                    allocated_quantity=Decimal(str(row["allocated_quantity"])),
                    entry_price=Decimal(str(row.get("entry_price", "0"))),
                )
                for row in plan_data["allocations"]
            ),
            total_allocated_quantity=Decimal(
                str(plan_data["total_allocated_quantity"])
            ),
            policy=ExitPolicyMode(str(plan_data["policy"])),
            absorbed_dust=Decimal(str(plan_data.get("absorbed_dust", "0"))),
            unallocated_remainder=Decimal(
                str(plan_data.get("unallocated_remainder", "0"))
            ),
            reason=str(plan_data.get("reason", "")),
            projection_version=plan_data.get("projection_version"),
            reservation_id=plan_data.get("reservation_id"),
            batch_quantities=(
                {key: Decimal(str(value)) for key, value in batch_quantities.items()}
                if isinstance(batch_quantities, dict)
                else None
            ),
        )
    return TradeCommand(
        command_id=str(payload["command_id"]),
        position_key=key,
        command_type=TradeCommandType(str(payload["command_type"])),
        side=StrategySide(str(payload["side"])),
        order_type=EntryType(str(payload["order_type"])),
        requested_quantity=Decimal(str(payload["requested_quantity"])),
        limit_price=(
            Decimal(str(payload["limit_price"]))
            if payload.get("limit_price") is not None
            else None
        ),
        reduce_only=bool(payload["reduce_only"]),
        allocation_plan=plan,
        reason=str(payload.get("reason", "")),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        fencing_token=payload.get("fencing_token"),
        idempotency_key=payload.get("idempotency_key"),
        expected_projection_version=payload.get("expected_projection_version"),
        reservation_id=payload.get("reservation_id"),
    )


class AsyncPostgresDecisionUnitOfWork:
    """Commit policy state, complete trace, retention and accepted exits at once."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        trace_repository: PostgresDecisionTraceRepository | None = None,
        retention_repository: AsyncPostgresRetentionRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._trace_repository = trace_repository or PostgresDecisionTraceRepository(
            session_factory
        )
        self._retention_repository = (
            retention_repository or AsyncPostgresRetentionRepository(session_factory)
        )

    async def commit_decision(self, commit: DecisionCommit) -> DecisionCommitReceipt:
        trace = commit.trace
        prior_digest = compute_policy_state_digest(commit.prior_policy_state)
        next_digest = compute_policy_state_digest(commit.next_policy_state)
        if prior_digest != commit.expected_prior_digest:
            raise DecisionCommitConflict("prior policy state digest does not match")
        payload = trace.trace_payload
        if (
            not trace.input_hash
            or not trace.frame_digest
            or payload.get("prior_policy_state")
            != serialize_policy_state(commit.prior_policy_state)
            or payload.get("next_policy_state")
            != serialize_policy_state(commit.next_policy_state)
        ):
            raise DecisionCommitConflict(
                "decision trace is incomplete or does not bind prior/next policy state"
            )
        context = payload.get("decision_context")
        position_view = (
            context.get("position_view") if isinstance(context, dict) else None
        )
        position_data = (
            position_view.get("position_key")
            if isinstance(position_view, dict)
            else None
        )
        if not isinstance(position_data, dict):
            raise DecisionCommitConflict(
                "decision trace is missing its complete position identity"
            )
        environment = position_data.get("environment")
        if (
            position_data.get("account_label") != trace.account_label
            or position_view.get("symbol") != position_data.get("symbol")
            or not isinstance(environment, str)
            or commit.policy_key
            != f"{environment}/{trace.account_label}/{trace.strategy_name}"
        ):
            raise DecisionCommitConflict(
                "policy key and decision trace account/symbol identity disagree"
            )
        if commit.accepted_exit is not None:
            if commit.accepted_exit.command_type != TradeCommandType.EXIT:
                raise DecisionCommitConflict("accepted decision command must be an exit")
            expected_exit = payload.get("output_exit_command")
            actual_exit = canonicalize_policy_value(commit.accepted_exit)
            if expected_exit != actual_exit:
                raise DecisionCommitConflict(
                    "accepted exit does not match the immutable trace output"
                )
            command_key = commit.accepted_exit.position_key
            if (
                command_key.environment != environment
                or command_key.account_label != trace.account_label
                or command_key.symbol != position_data.get("symbol")
                or command_key.position_side.value
                != position_data.get("position_side")
            ):
                raise DecisionCommitConflict(
                    "accepted exit scope does not match the decision input"
                )
        elif payload.get("output_exit_command") is not None:
            raise DecisionCommitConflict(
                "decision trace has an exit output but the commit omitted it"
            )

        commit_content = {
            "policy_key": commit.policy_key,
            "prior_digest": prior_digest,
            "next_digest": next_digest,
            "trace": {
                "decision_id": trace.decision_id,
                "strategy_name": trace.strategy_name,
                "account_label": trace.account_label,
                "decision_time": trace.decision_time.astimezone(UTC).isoformat(),
                "intent_produced": trace.intent_produced,
                "intent_id": trace.intent_id,
                "rejection_reason": trace.rejection_reason,
                "input_hash": trace.input_hash,
                "frame_digest": trace.frame_digest,
                "payload": trace.trace_payload,
                "market_refs": [
                    canonicalize_policy_value(ref)
                    for ref in trace.evaluated_market_refs
                ],
            },
            "dependencies": sorted(
                (_dependency_payload(item) for item in commit.dependencies),
                key=lambda item: (item["consumer_id"], item["dataset_name"]),
            ),
            "accepted_exit": (
                _trade_command_payload(commit.accepted_exit)
                if commit.accepted_exit is not None
                else None
            ),
        }
        commit_digest = hashlib.sha256(
            json.dumps(commit_content, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()

        now = datetime.now(UTC)
        async with self._session_factory() as session:
            async with session.begin():
                await self._require_durable_commit(session)
                await self._lock_policy(session, commit.policy_key)

                prior_commit = await session.get(
                    DurablePolicyCommitRow, trace.decision_id
                )
                if prior_commit is not None:
                    if (
                        prior_commit.policy_key != commit.policy_key
                        or prior_commit.prior_state_digest != prior_digest
                        or prior_commit.next_state_digest != next_digest
                        or prior_commit.commit_digest != commit_digest
                    ):
                        raise DecisionCommitConflict(
                            f"decision {trace.decision_id} was already committed "
                            "with conflicting policy contents"
                        )
                    await self._trace_repository.save_decision_traces_in_session(
                        session, (trace,)
                    )
                    existing_exit = await session.get(
                        DurableDecisionExitRow, trace.decision_id
                    )
                    expected_exit = commit.accepted_exit is not None
                    if expected_exit != (existing_exit is not None) or (
                        expected_exit
                        and existing_exit is not None
                        and (
                            existing_exit.command_id
                            != commit.accepted_exit.command_id
                            or existing_exit.command_payload
                            != _trade_command_payload(commit.accepted_exit)
                        )
                    ):
                        raise DecisionCommitConflict(
                            f"decision {trace.decision_id} exit outbox conflicts"
                        )
                    durable_at = prior_commit.committed_at
                    return DecisionCommitReceipt(
                        decision_id=trace.decision_id,
                        policy_key=commit.policy_key,
                        prior_state_digest=prior_digest,
                        next_state_digest=next_digest,
                        policy_revision=prior_commit.policy_revision,
                        durable_at=durable_at,
                        pending_exit_id=(
                            trace.decision_id if expected_exit else None
                        ),
                        is_replay=True,
                    )

                row = await session.get(
                    DurablePolicyStateRow,
                    commit.policy_key,
                    with_for_update=True,
                )
                current_revision = row.policy_revision if row is not None else 0
                current_digest = (
                    row.state_digest
                    if row is not None
                    else compute_policy_state_digest(PolicyState())
                )
                if current_revision != commit.expected_policy_revision:
                    raise DecisionCommitConflict(
                        f"policy revision changed: expected "
                        f"{commit.expected_policy_revision}, current {current_revision}"
                    )
                if current_digest != prior_digest:
                    raise DecisionCommitConflict(
                        "durable policy state does not match the frozen prior state"
                    )

                await self._trace_repository.save_decision_traces_in_session(
                    session, (trace,)
                )
                for dependency in commit.dependencies:
                    await self._retention_repository.save_dependency_in_session(
                        session, dependency
                    )

                new_revision = current_revision + 1
                state_values = {
                    "policy_key": commit.policy_key,
                    "policy_revision": new_revision,
                    "policy_version": commit.next_policy_state.policy_version,
                    "state_digest": next_digest,
                    "state_payload": serialize_policy_state(
                        commit.next_policy_state
                    ),
                    "last_decision_id": trace.decision_id,
                    "updated_at": now,
                }
                if row is None:
                    session.add(DurablePolicyStateRow(**state_values))
                else:
                    for key, value in state_values.items():
                        setattr(row, key, value)

                session.add(
                    DurablePolicyCommitRow(
                        decision_id=trace.decision_id,
                        policy_key=commit.policy_key,
                        prior_state_digest=prior_digest,
                        next_state_digest=next_digest,
                        commit_digest=commit_digest,
                        policy_revision=new_revision,
                        committed_at=now,
                    )
                )
                if commit.accepted_exit is not None:
                    command = commit.accepted_exit
                    session.add(
                        DurableDecisionExitRow(
                            decision_id=trace.decision_id,
                            policy_key=commit.policy_key,
                            command_id=command.command_id,
                            command_payload=_trade_command_payload(command),
                            status="PENDING",
                            created_at=now,
                            updated_at=now,
                        )
                    )

                return DecisionCommitReceipt(
                    decision_id=trace.decision_id,
                    policy_key=commit.policy_key,
                    prior_state_digest=prior_digest,
                    next_state_digest=next_digest,
                    policy_revision=new_revision,
                    durable_at=now,
                    pending_exit_id=(
                        trace.decision_id if commit.accepted_exit is not None else None
                    ),
                )

    async def load_policy_state(
        self, policy_key: str
    ) -> DurablePolicySnapshot | None:
        async with self._session_factory() as session:
            row = await session.get(DurablePolicyStateRow, policy_key)
        if row is None:
            return None
        state = _policy_state_from_payload(row.state_payload)
        digest = compute_policy_state_digest(state)
        if digest != row.state_digest:
            raise DecisionCommitConflict(
                f"stored policy state {policy_key} failed its digest check"
            )
        return DurablePolicySnapshot(
            state=state,
            state_digest=digest,
            revision=row.policy_revision,
            last_decision_id=row.last_decision_id,
        )

    async def load_or_import_policy_state(
        self,
        policy_key: str,
        strategy_name: str,
        account_label: str,
    ) -> DurablePolicySnapshot | None:
        """Load the durable head or import a fully verifiable legacy trace.

        A missing head is fresh only when no trace exists for this exact policy.
        The legacy trace id becomes the bootstrap provenance in
        ``last_decision_id``; incomplete or inconsistent state is rejected.
        """
        if not policy_key.strip() or not strategy_name.strip() or not account_label.strip():
            raise ValueError("policy, strategy, and account identity are required")
        if not policy_key.endswith(f"/{account_label}/{strategy_name}"):
            raise DecisionCommitConflict(
                "policy key does not match the requested strategy/account identity"
            )
        async with self._session_factory() as session:
            async with session.begin():
                await self._require_durable_commit(session)
                await self._lock_policy(session, policy_key)
                state_row = await session.get(
                    DurablePolicyStateRow,
                    policy_key,
                    with_for_update=True,
                )
                if state_row is not None:
                    state = _policy_state_from_payload(state_row.state_payload)
                    digest = compute_policy_state_digest(state)
                    if digest != state_row.state_digest:
                        raise DecisionCommitConflict(
                            f"stored policy state {policy_key} failed its digest check"
                        )
                    return DurablePolicySnapshot(
                        state=state,
                        state_digest=digest,
                        revision=state_row.policy_revision,
                        last_decision_id=state_row.last_decision_id,
                    )

                trace = await session.scalar(
                    select(DecisionTraceRow)
                    .where(
                        DecisionTraceRow.strategy_name == strategy_name,
                        DecisionTraceRow.account_label == account_label,
                    )
                    .order_by(
                        DecisionTraceRow.decision_time.desc(),
                        DecisionTraceRow.created_at.desc(),
                        DecisionTraceRow.decision_id.desc(),
                    )
                    .limit(1)
                    .with_for_update()
                )
                if trace is None:
                    return None
                payload = trace.trace_payload
                if (
                    not isinstance(payload, dict)
                    or payload.get("trace_schema_version") != 1
                    or not isinstance(payload.get("input_hash"), str)
                    or not payload.get("input_hash")
                    or not isinstance(payload.get("frame_digest"), str)
                    or not payload.get("frame_digest")
                ):
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} is incomplete"
                    )

                expected_state_fields = {
                    "serialization_version",
                    "policy_version",
                    "cooldown_until",
                    "anchor_prices",
                    "active_intent_ids",
                    "custom_state",
                    "signal_memory",
                    "warmup_status",
                    "grace_until",
                    "holding_deadline",
                    "sizing_state",
                }

                def decode_legacy_state(field_name: str) -> PolicyState:
                    serialized = payload.get(field_name)
                    if (
                        not isinstance(serialized, dict)
                        or set(serialized) != expected_state_fields
                        or serialized.get("serialization_version") != 1
                        or type(serialized.get("policy_version")) is not int
                        or serialized.get("policy_version", 0) < 1
                        or any(
                            not isinstance(serialized.get(name), dict)
                            for name in (
                                "cooldown_until",
                                "anchor_prices",
                                "active_intent_ids",
                                "custom_state",
                                "signal_memory",
                                "warmup_status",
                                "grace_until",
                                "holding_deadline",
                                "sizing_state",
                            )
                        )
                    ):
                        raise DecisionCommitConflict(
                            f"legacy decision trace {trace.decision_id} has an "
                            f"incomplete {field_name}"
                        )
                    try:
                        state = _policy_state_from_payload(serialized)
                    except (TypeError, ValueError) as err:
                        raise DecisionCommitConflict(
                            f"legacy decision trace {trace.decision_id} has an "
                            f"invalid {field_name}"
                        ) from err
                    if serialize_policy_state(state) != serialized:
                        raise DecisionCommitConflict(
                            f"legacy decision trace {trace.decision_id} has a "
                            f"non-canonical {field_name}"
                        )
                    return state

                prior_state = decode_legacy_state("prior_policy_state")
                next_state = decode_legacy_state("next_policy_state")
                prior_digest = compute_policy_state_digest(prior_state)
                next_digest = compute_policy_state_digest(next_state)
                frame = payload.get("decision_frame")
                if (
                    not isinstance(frame, dict)
                    or frame.get("policy_state_digest") != prior_digest
                    or payload.get("frame_digest")
                    != trace.trace_payload.get("frame_digest")
                ):
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} does not bind "
                        "its prior policy state"
                    )
                revision_ids = trace.evaluated_revision_ids
                if (
                    not isinstance(revision_ids, list)
                    or not revision_ids
                    or any(not isinstance(value, str) or not value for value in revision_ids)
                    or len(revision_ids) != len(set(revision_ids))
                ):
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} has invalid "
                        "market revision references"
                    )
                refs = (
                    await session.scalars(
                        select(MarketRevisionRefRow).where(
                            MarketRevisionRefRow.revision_id.in_(revision_ids)
                        )
                    )
                ).all()
                if {row.revision_id for row in refs} != set(revision_ids) or any(
                    not isinstance(row.payload, dict)
                    or row.payload.get("reference_only") is True
                for row in refs
                ):
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} has missing or "
                        "incomplete market facts"
                    )

                ref_by_id = {row.revision_id: row for row in refs}
                try:
                    domain_trace = DecisionTrace(
                        decision_id=trace.decision_id,
                        strategy_name=trace.strategy_name,
                        account_label=trace.account_label,
                        decision_time=trace.decision_time,
                        evaluated_market_refs=tuple(
                            MarketRevisionRef(
                                scope=ref_by_id[revision_id].scope,
                                symbol=ref_by_id[revision_id].symbol,
                                interval=ref_by_id[revision_id].interval,
                                bucket_start=ref_by_id[revision_id].bucket_start,
                                bucket_end=ref_by_id[revision_id].bucket_end,
                                revision_id=ref_by_id[revision_id].revision_id,
                                content_hash=ref_by_id[revision_id].content_hash,
                                published_at=ref_by_id[revision_id].published_at,
                                source_epoch=ref_by_id[revision_id].source_epoch,
                                visibility_mode=MarketVisibilityMode(
                                    ref_by_id[revision_id].visibility_mode
                                ),
                            )
                            for revision_id in revision_ids
                        ),
                        intent_produced=trace.intent_produced,
                        intent_id=trace.intent_id,
                        rejection_reason=trace.rejection_reason,
                        input_hash=str(payload["input_hash"]),
                        frame_digest=str(payload["frame_digest"]),
                        trace_payload=dict(payload),
                    )
                    frame_evidence = payload.get("decision_frame")
                    position_context = payload.get("decision_context")
                    position_view = (
                        position_context.get("position_view")
                        if isinstance(position_context, dict)
                        else None
                    )
                    position_key = (
                        position_view.get("position_key")
                        if isinstance(position_view, dict)
                        else None
                    )
                    clock_event = payload.get("clock_event")
                    if (
                        not isinstance(frame_evidence, dict)
                        or frame_evidence.get("market_revision_ids") != revision_ids
                        or not isinstance(position_key, dict)
                        or position_key.get("account_label") != account_label
                        or position_key.get("environment")
                        != policy_key.split("/", maxsplit=1)[0]
                        or not isinstance(clock_event, dict)
                        or clock_event.get("timestamp")
                        != trace.decision_time.astimezone(UTC).isoformat()
                    ):
                        raise DecisionCommitConflict(
                            f"legacy decision trace {trace.decision_id} has an "
                            "inconsistent decision scope or clock"
                        )
                    from crypto_momentum_lab.tools.reproduce_decision import (
                        audit_decision_trace,
                    )

                    audit = await audit_decision_trace(
                        trace.decision_id,
                        trace_override=domain_trace,
                    )
                except DecisionCommitConflict:
                    raise
                except Exception as err:
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} could not be "
                        "fully reconstructed"
                    ) from err
                if (
                    not isinstance(audit, dict)
                    or audit.get("status") != "VERIFIED_REPRODUCIBLE"
                    or audit.get("reproduced") is not True
                    or audit.get("strategy_name") != strategy_name
                    or audit.get("account_label") != account_label
                ):
                    detail = (
                        audit.get("error")
                        if isinstance(audit, dict)
                        else "invalid audit result"
                    )
                    raise DecisionCommitConflict(
                        f"legacy decision trace {trace.decision_id} failed strict "
                        f"replay verification: {detail}"
                    )

                imported_at = datetime.now(UTC)
                state_row = DurablePolicyStateRow(
                    policy_key=policy_key,
                    policy_revision=1,
                    policy_version=next_state.policy_version,
                    state_digest=next_digest,
                    state_payload=serialize_policy_state(next_state),
                    last_decision_id=trace.decision_id,
                    updated_at=imported_at,
                )
                session.add(state_row)
                return DurablePolicySnapshot(
                    state=next_state,
                    state_digest=next_digest,
                    revision=1,
                    last_decision_id=trace.decision_id,
                )

    async def restore_or_import_policy_state(
        self,
        policy_key: str,
        strategy_name: str,
        account_label: str,
    ) -> DurablePolicySnapshot | None:
        """Compatibility alias for startup policy recovery callers."""
        return await self.load_or_import_policy_state(
            policy_key,
            strategy_name,
            account_label,
        )

    async def load_pending_exits(
        self, policy_key: str | None = None
    ) -> tuple[tuple[str, TradeCommand], ...]:
        async with self._session_factory() as session:
            query = select(DurableDecisionExitRow).where(
                DurableDecisionExitRow.status == "PENDING"
            )
            if policy_key is not None:
                query = query.where(DurableDecisionExitRow.policy_key == policy_key)
            rows = (
                await session.scalars(
                    query.order_by(DurableDecisionExitRow.created_at)
                )
            ).all()
        return tuple(
            (row.decision_id, _trade_command_from_payload(row.command_payload))
            for row in rows
        )

    async def mark_exit_dispatched(
        self, decision_id: str, command_id: str
    ) -> bool:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            async with session.begin():
                await self._require_durable_commit(session)
                row = await session.get(
                    DurableDecisionExitRow, decision_id, with_for_update=True
                )
                if row is None:
                    return False
                if row.command_id != command_id:
                    raise DecisionCommitConflict(
                        f"exit command identity conflict for {decision_id}"
                    )
                if row.status == "DISPATCHED":
                    return True
                if row.status != "PENDING":
                    raise DecisionCommitConflict(
                        f"exit {decision_id} has invalid status {row.status}"
                    )
                row.status = "DISPATCHED"
                row.updated_at = now
                row.dispatched_at = now
                row.dispatch_receipt = command_id
                return True

    async def mark_exit_superseded(
        self,
        decision_id: str,
        command_id: str,
        reason: str,
    ) -> bool:
        """Terminally close an exit only while it is still pending.

        The caller must first reconcile the exchange and establish that the
        command was never posted. A dispatched or already superseded command
        cannot be silently rewritten.
        """
        if not reason.strip():
            raise ValueError("supersede reason must not be empty")
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            async with session.begin():
                await self._require_durable_commit(session)
                row = await session.get(
                    DurableDecisionExitRow, decision_id, with_for_update=True
                )
                if row is None:
                    return False
                if row.command_id != command_id:
                    raise DecisionCommitConflict(
                        f"exit command identity conflict for {decision_id}"
                    )
                if row.status == "SUPERSEDED":
                    if row.disposition_reason != reason:
                        raise DecisionCommitConflict(
                            f"exit {decision_id} has a different terminal disposition"
                        )
                    return True
                if row.status != "PENDING":
                    raise DecisionCommitConflict(
                        f"exit {decision_id} cannot be superseded from {row.status}"
                    )
                row.status = "SUPERSEDED"
                row.disposition_reason = reason
                row.updated_at = now
                return True

    @staticmethod
    async def _require_durable_commit(session: AsyncSession) -> None:
        bind = session.get_bind()
        if bind is None or bind.dialect.name != "postgresql":
            raise RuntimeError(
                "durable live decision commits require PostgreSQL synchronous commit"
            )
        await session.execute(text("SET LOCAL synchronous_commit = ON"))

    @staticmethod
    async def _lock_policy(session: AsyncSession, policy_key: str) -> None:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": f"durable_policy:{policy_key}"},
        )


def _policy_state_from_payload(payload: dict[str, Any]) -> PolicyState:
    cooldown = payload.get("cooldown_until_by_symbol") or payload.get(
        "cooldown_until", {}
    )
    anchors = payload.get("anchor_prices_by_symbol") or payload.get(
        "anchor_prices", {}
    )
    intents = payload.get("active_intent_ids_by_symbol") or payload.get(
        "active_intent_ids", {}
    )
    grace = payload.get("grace_until_by_symbol") or payload.get("grace_until", {})
    deadlines = payload.get("holding_deadline_by_symbol") or payload.get(
        "holding_deadline", {}
    )
    sizing = payload.get("sizing_state_by_symbol") or payload.get("sizing_state", {})
    return PolicyState(
        policy_version=int(payload.get("policy_version", 1)),
        cooldown_until_by_symbol={
            key: datetime.fromisoformat(value) if isinstance(value, str) else value
            for key, value in cooldown.items()
        },
        anchor_prices_by_symbol={
            key: Decimal(str(value)) for key, value in anchors.items()
        },
        active_intent_ids_by_symbol=dict(intents),
        custom_state=dict(payload.get("custom_state", {})),
        signal_memory=dict(payload.get("signal_memory", {})),
        warmup_status=dict(payload.get("warmup_status", {})),
        grace_until_by_symbol={
            key: datetime.fromisoformat(value) if isinstance(value, str) else value
            for key, value in grace.items()
        },
        holding_deadline_by_symbol={
            key: datetime.fromisoformat(value) if isinstance(value, str) else value
            for key, value in deadlines.items()
        },
        sizing_state_by_symbol=dict(sizing),
    )


__all__ = [
    "AsyncPostgresDecisionUnitOfWork",
    "DecisionCommit",
    "DecisionCommitConflict",
    "DecisionCommitReceipt",
    "DurablePolicySnapshot",
]
