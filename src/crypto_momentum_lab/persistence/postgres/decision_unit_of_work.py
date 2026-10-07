"""PostgreSQL transaction boundary for durable policy and decision commits."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.decision import commit_models
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
    DecisionCommitConflict as _DecisionCommitConflict,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
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
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionTraceRow,
)
from crypto_momentum_lab.persistence.postgres.retention_repository import (
    AsyncPostgresRetentionRepository,
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
        "limit_price": (
            str(command.limit_price) if command.limit_price is not None else None
        ),
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
    plan_data = payload["allocation_plan"]
    plan = None
    if plan_data is not None:
        batch_quantities = plan_data["batch_quantities"]
        plan = ExitAllocationPlan(
            position_key=key,
            allocations=tuple(
                ExitAllocation(
                    batch_id=str(row["batch_id"]),
                    allocated_quantity=Decimal(str(row["allocated_quantity"])),
                    entry_price=Decimal(str(row["entry_price"])),
                )
                for row in plan_data["allocations"]
            ),
            total_allocated_quantity=Decimal(
                str(plan_data["total_allocated_quantity"])
            ),
            policy=ExitPolicyMode(str(plan_data["policy"])),
            unallocated_remainder=Decimal(str(plan_data["unallocated_remainder"])),
            reason=str(plan_data["reason"]),
            projection_version=plan_data["projection_version"],
            reservation_id=plan_data["reservation_id"],
            batch_quantities=(
                {key: Decimal(str(value)) for key, value in batch_quantities.items()}
                if batch_quantities is not None
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
            if payload["limit_price"] is not None
            else None
        ),
        reduce_only=bool(payload["reduce_only"]),
        allocation_plan=plan,
        reason=str(payload["reason"]),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        fencing_token=payload["fencing_token"],
        idempotency_key=payload["idempotency_key"],
        expected_projection_version=payload["expected_projection_version"],
        reservation_id=payload["reservation_id"],
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

    async def commit_decision(
        self, commit: commit_models.DecisionCommit
    ) -> commit_models.DecisionCommitReceipt:
        trace = commit.trace
        prior_digest = compute_policy_state_digest(commit.prior_policy_state)
        next_digest = compute_policy_state_digest(commit.next_policy_state)
        if prior_digest != commit.expected_prior_digest:
            raise _DecisionCommitConflict("prior policy state digest does not match")
        payload = trace.trace_payload
        if (
            not trace.input_hash
            or not trace.frame_digest
            or payload.get("prior_policy_state")
            != serialize_policy_state(commit.prior_policy_state)
            or payload.get("next_policy_state")
            != serialize_policy_state(commit.next_policy_state)
        ):
            raise _DecisionCommitConflict(
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
        if not isinstance(position_view, dict) or not isinstance(position_data, dict):
            raise _DecisionCommitConflict(
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
            raise _DecisionCommitConflict(
                "policy key and decision trace account/symbol identity disagree"
            )
        if commit.accepted_exit is not None:
            if commit.accepted_exit.command_type != TradeCommandType.EXIT:
                raise _DecisionCommitConflict(
                    "accepted decision command must be an exit"
                )
            expected_exit = payload.get("output_exit_command")
            actual_exit = canonicalize_policy_value(commit.accepted_exit)
            if expected_exit != actual_exit:
                raise _DecisionCommitConflict(
                    "accepted exit does not match the immutable trace output"
                )
            command_key = commit.accepted_exit.position_key
            if (
                command_key.environment != environment
                or command_key.account_label != trace.account_label
                or command_key.symbol != position_data.get("symbol")
                or command_key.position_side.value != position_data.get("position_side")
            ):
                raise _DecisionCommitConflict(
                    "accepted exit scope does not match the decision input"
                )
        elif payload.get("output_exit_command") is not None:
            raise _DecisionCommitConflict(
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
                        or prior_commit.commit_digest != commit_digest
                    ):
                        raise _DecisionCommitConflict(
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
                            existing_exit.command_id != commit.accepted_exit.command_id
                            or existing_exit.command_payload
                            != _trade_command_payload(commit.accepted_exit)
                        )
                    ):
                        raise _DecisionCommitConflict(
                            f"decision {trace.decision_id} exit outbox conflicts"
                        )
                    durable_at = prior_commit.committed_at
                    return commit_models.DecisionCommitReceipt(
                        decision_id=trace.decision_id,
                        policy_key=commit.policy_key,
                        prior_state_digest=prior_digest,
                        next_state_digest=next_digest,
                        policy_revision=prior_commit.policy_revision,
                        durable_at=durable_at,
                        pending_exit_id=(trace.decision_id if expected_exit else None),
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
                    raise _DecisionCommitConflict(
                        f"policy revision changed: expected "
                        f"{commit.expected_policy_revision}, current {current_revision}"
                    )
                if current_digest != prior_digest:
                    raise _DecisionCommitConflict(
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
                    "state_payload": serialize_policy_state(commit.next_policy_state),
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

                return commit_models.DecisionCommitReceipt(
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
    ) -> commit_models.DurablePolicySnapshot | None:
        async with self._session_factory() as session:
            row = await session.get(DurablePolicyStateRow, policy_key)
        if row is None:
            return None
        state = _policy_state_from_payload(row.state_payload)
        digest = compute_policy_state_digest(state)
        if digest != row.state_digest:
            raise _DecisionCommitConflict(
                f"stored policy state {policy_key} failed its digest check"
            )
        return commit_models.DurablePolicySnapshot(
            state=state,
            state_digest=digest,
            revision=row.policy_revision,
            last_decision_id=row.last_decision_id,
        )

    async def load_policy_state_for_startup(
        self,
        policy_key: str,
        strategy_name: str,
        account_label: str,
    ) -> commit_models.DurablePolicySnapshot | None:
        """Load current durable state; refuse trace-only state recovery."""
        if (
            not policy_key.strip()
            or not strategy_name.strip()
            or not account_label.strip()
        ):
            raise ValueError("policy, strategy, and account identity are required")
        if not policy_key.endswith(f"/{account_label}/{strategy_name}"):
            raise _DecisionCommitConflict(
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
                        raise _DecisionCommitConflict(
                            f"stored policy state {policy_key} failed its digest check"
                        )
                    return commit_models.DurablePolicySnapshot(
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
                raise _DecisionCommitConflict(
                    f"decision trace {trace.decision_id} has no durable policy head"
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
                await session.scalars(query.order_by(DurableDecisionExitRow.created_at))
            ).all()
        return tuple(
            (row.decision_id, _trade_command_from_payload(row.command_payload))
            for row in rows
        )

    async def mark_exit_dispatched(self, decision_id: str, command_id: str) -> bool:
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
                    raise _DecisionCommitConflict(
                        f"exit command identity conflict for {decision_id}"
                    )
                if row.status == "DISPATCHED":
                    return True
                if row.status != "PENDING":
                    raise _DecisionCommitConflict(
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
                    raise _DecisionCommitConflict(
                        f"exit command identity conflict for {decision_id}"
                    )
                if row.status == "SUPERSEDED":
                    if row.disposition_reason != reason:
                        raise _DecisionCommitConflict(
                            f"exit {decision_id} has a different terminal disposition"
                        )
                    return True
                if row.status != "PENDING":
                    raise _DecisionCommitConflict(
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
    expected_fields = {
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
    from crypto_momentum_lab.domain.decision.policy_transition import (
        POLICY_STATE_SERIALIZATION_VERSION,
    )

    if (
        set(payload) != expected_fields
        or payload["serialization_version"] != POLICY_STATE_SERIALIZATION_VERSION
    ):
        raise _DecisionCommitConflict("stored policy state uses an unsupported schema")
    cooldown = payload["cooldown_until"]
    anchors = payload["anchor_prices"]
    intents = payload["active_intent_ids"]
    grace = payload["grace_until"]
    deadlines = payload["holding_deadline"]
    sizing = payload["sizing_state"]
    return PolicyState(
        policy_version=int(payload["policy_version"]),
        cooldown_until_by_symbol={
            key: datetime.fromisoformat(value) if isinstance(value, str) else value
            for key, value in cooldown.items()
        },
        anchor_prices_by_symbol={
            key: Decimal(str(value)) for key, value in anchors.items()
        },
        active_intent_ids_by_symbol=dict(intents),
        custom_state=dict(payload["custom_state"]),
        signal_memory=dict(payload["signal_memory"]),
        warmup_status=dict(payload["warmup_status"]),
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


__all__ = ["AsyncPostgresDecisionUnitOfWork"]
