"""Check live position facts from the single durable execution head."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
    is_supported_execution_head_schema,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import AccountReconciliationHeadRow

REASON_ACTIVE_POSITION_COUNT_MISMATCH = "active_position_count_mismatch"
REASON_NO_RELEVANT_HEAD = "no_relevant_head"


@dataclass(frozen=True, slots=True)
class FactIntegritySummary:
    """Account-level readiness derived from current head facts."""

    gaps: int | None
    observed_at: datetime | None
    reasons: tuple[str, ...] = ()
    unknown_scopes: tuple[str, ...] = ()
    historical_reasons: tuple[str, ...] = ()


async def load_fact_integrity(
    sessions: async_sessionmaker[AsyncSession],
    *,
    accounts: dict[str, AccountReconciliationHeadRow],
    now: datetime,
    max_age_seconds: float = 180.0,
) -> dict[str, FactIntegritySummary]:
    """Verify decoded position facts and their current coverage watermark."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("fact integrity time must be timezone-aware")
    if not accounts:
        return {}

    async with sessions() as session:
        heads = (
            await session.scalars(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label.in_(tuple(accounts)),
                )
            )
        ).all()

    grouped: dict[str, list[ExecutionBookHeadRow]] = {
        account: [] for account in accounts
    }
    for head in heads:
        grouped[head.account_label].append(head)

    summaries: dict[str, FactIntegritySummary] = {}
    for account, account_heads in grouped.items():
        current_count = 0
        current_cuts: list[datetime] = []
        proven_cuts: list[datetime] = []
        historical_reasons: list[str] = []
        reasons: list[str] = []
        unknown_scopes: list[str] = []
        invalid = 0
        current_unknown = False

        if not account_heads and accounts[account].position_count:
            reasons.append(REASON_NO_RELEVANT_HEAD)
            unknown_scopes.append("*")
            current_unknown = True

        for head in account_heads:
            scope_label = f"{head.symbol}:{head.position_side}"
            payload = head.state_payload or {}
            is_current = bool(
                payload.get("active_reservation_ids")
                or payload.get("recovery_command_ids")
                or payload.get("external_recovery_ids")
            )
            if not is_supported_execution_head_schema(payload.get("schema_version")):
                # Unknown head formats cannot prove current exposure. Any live
                # exchange position still appears in the account-level count.
                continue
            try:
                key = PositionKey(
                    "live",
                    account,
                    head.symbol,
                    FuturesPositionSide(head.position_side),
                )
                scope = AccountFactStreamScope.for_position_key(
                    key,
                    stream_id=head.stream_id,
                    stream_epoch=head.stream_epoch,
                )
                current_book = decode_execution_head_state(
                    payload, key=key, scope=scope
                )
                facts = current_book.current_facts
                projection = current_book.projection
            except Exception:
                invalid += 1
                reasons.append("head_invalid")
                unknown_scopes.append(scope_label)
                current_unknown = True
                continue

            quantity = projection.total_active_quantity
            is_current = is_current or quantity != 0
            if quantity != 0:
                current_count += 1
            if (
                facts.fact_conflicts
                or facts.integrity_issues
                or facts.has_synthetic_fills
                or facts.has_late_events
            ):
                if is_current:
                    invalid += 1
                    reasons.append("facts_conflict")
                    unknown_scopes.append(scope_label)
                    continue
                historical_reasons.append("facts_conflict")
                continue
            coverage = facts.coverage
            if coverage is None or not coverage.is_authoritative:
                if is_current:
                    current_unknown = True
                    reasons.append("coverage_missing")
                    unknown_scopes.append(scope_label)
                else:
                    historical_reasons.append("coverage_missing")
                continue
            if projection.health_status.value != "READY":
                if is_current:
                    current_unknown = True
                    reasons.append("position_not_ready")
                    unknown_scopes.append(scope_label)
                else:
                    historical_reasons.append("position_not_ready")
                continue
            if projection.reconciliation_gap != Decimal(
                "0"
            ) or projection.unallocated_quantity != Decimal("0"):
                if is_current:
                    invalid += 1
                    reasons.append("position_not_ready")
                    unknown_scopes.append(scope_label)
                else:
                    historical_reasons.append("position_not_ready")
                continue

            age_seconds = (now - coverage.end_at).total_seconds()
            latest_fact_at = max(
                (
                    *(fill.trade_at for fill in facts.fills),
                    *(snapshot.observed_at for snapshot in facts.snapshots),
                    *(boundary.submitted_at for boundary in facts.exit_boundaries),
                    coverage.end_at,
                )
            )
            if (
                age_seconds < 0
                or age_seconds > max_age_seconds
                or coverage.end_at < latest_fact_at
            ):
                if is_current:
                    current_unknown = True
                    reasons.append("coverage_stale")
                    unknown_scopes.append(scope_label)
                else:
                    historical_reasons.append("coverage_stale")
                continue
            proven_cuts.append(coverage.end_at)
            if is_current:
                current_cuts.append(coverage.end_at)

        if current_count != accounts[account].position_count:
            reasons.append(REASON_ACTIVE_POSITION_COUNT_MISMATCH)
            unknown_scopes.append("*")
            current_unknown = True

        if invalid:
            gaps: int | None = invalid
        elif current_unknown:
            gaps = None
        else:
            gaps = 0
        summaries[account] = FactIntegritySummary(
            gaps=gaps,
            observed_at=min(proven_cuts) if proven_cuts else None,
            reasons=tuple(dict.fromkeys(reasons)),
            unknown_scopes=tuple(dict.fromkeys(unknown_scopes)),
            historical_reasons=tuple(dict.fromkeys(historical_reasons)),
        )
    return summaries


__all__ = ["FactIntegritySummary", "load_fact_integrity"]
