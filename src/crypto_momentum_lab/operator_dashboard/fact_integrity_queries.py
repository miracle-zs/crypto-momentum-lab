"""Read verified ledger coverage, independently of process/reconciliation flags."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Integer, cast, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import AccountReconciliationHeadRow
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionRecoveryCheckpointRow,
)


@dataclass(frozen=True, slots=True)
class FactIntegritySummary:
    gaps: int | None
    observed_at: datetime | None
    verified_position_count: int


async def load_fact_integrity(
    sessions: async_sessionmaker[AsyncSession],
    *,
    accounts: dict[str, AccountReconciliationHeadRow],
    now: datetime,
    max_age_seconds: float = 180.0,
) -> dict[str, FactIntegritySummary]:
    if not accounts:
        return {}
    async with sessions() as session:
        heads = (
            await session.scalars(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label.in_(tuple(accounts)),
                    or_(
                        cast(
                            ExecutionBookHeadRow.state_payload[
                                "seen_trade_count"
                            ].astext,
                            Integer,
                        )
                        > 0,
                        ExecutionBookHeadRow.state_payload["recovery_checkpoint"][
                            "checkpoint_id"
                        ].astext.is_not(None),
                    ),
                )
            )
        ).all()
        bindings = []
        for head in heads:
            binding = head.state_payload.get("recovery_checkpoint")
            if isinstance(binding, dict) and binding.get("checkpoint_id"):
                bindings.append(
                    (
                        head.account_label,
                        head.symbol,
                        head.position_side,
                        head.stream_id,
                        head.stream_epoch,
                        binding["checkpoint_id"],
                    )
                )
        rows = (
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow).where(
                        PositionRecoveryCheckpointRow.environment == "live",
                        tuple_(
                            PositionRecoveryCheckpointRow.account_label,
                            PositionRecoveryCheckpointRow.symbol,
                            PositionRecoveryCheckpointRow.position_side,
                            PositionRecoveryCheckpointRow.stream_id,
                            PositionRecoveryCheckpointRow.stream_epoch,
                            PositionRecoveryCheckpointRow.checkpoint_id,
                        ).in_(bindings),
                    )
                )
            ).all()
            if bindings
            else []
        )
    checkpoints = {
        (
            row.account_label,
            row.symbol,
            row.position_side,
            row.stream_id,
            row.stream_epoch,
            row.checkpoint_id,
        ): row
        for row in rows
    }
    grouped: dict[str, list[ExecutionBookHeadRow]] = {
        account: [] for account in accounts
    }
    for head in heads:
        grouped[head.account_label].append(head)
    result = {}
    for account, account_heads in grouped.items():
        confirmed = []
        invalid = 0
        unknown = not bool(account_heads)
        active_positions = 0
        for head in account_heads:
            binding = head.state_payload.get("recovery_checkpoint")
            if not isinstance(binding, dict):
                unknown = True
                continue
            row = checkpoints.get(
                (
                    account,
                    head.symbol,
                    head.position_side,
                    head.stream_id,
                    head.stream_epoch,
                    str(binding.get("checkpoint_id")),
                )
            )
            if row is None:
                unknown = True
                continue
            checkpoint = PositionRecoveryCodec.decode_checkpoint(row.payload)
            if (
                checkpoint.has_conflicts
                or checkpoint.has_synthetic_fills
                or checkpoint.has_late_events
                or checkpoint.integrity_issues
                or checkpoint.projection.health_status.value != "READY"
                or checkpoint.projection.reconciliation_gap != 0
            ):
                invalid += 1
                continue
            if (
                checkpoint.key.environment != "live"
                or checkpoint.key.account_label != account
                or (checkpoint.key.symbol, checkpoint.key.position_side.value)
                != (head.symbol, head.position_side)
                or recovery_checkpoint_head_binding(checkpoint) != binding
                or checkpoint.coverage is None
                or not checkpoint.coverage.is_authoritative
                or not checkpoint.coverage.covers(checkpoint.event_cut)
                or checkpoint.event_cut < head.updated_at
                or not 0
                <= (now - checkpoint.event_cut).total_seconds()
                <= max_age_seconds
            ):
                unknown = True
                continue
            confirmed.append(checkpoint.event_cut)
            active_positions += checkpoint.projection.total_active_quantity != 0
        if active_positions != accounts[account].position_count:
            unknown = True
        result[account] = FactIntegritySummary(
            invalid if invalid else None if unknown else 0,
            min(confirmed) if confirmed else None,
            len(confirmed),
        )
    return result
