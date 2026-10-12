"""Read-only first-divergence report for one live position scope.

This tool answers "which evidence disagrees, and how old is each cut" for a
single account/symbol/side, using only that account's own durable facts:

* the execution head (stream, revision, journal revision, checkpoint binding,
  unresolved recovery identities),
* the bound recovery checkpoint (cut, revision, coverage, conflicts, projected
  active quantity),
* the persisted journal suffix after that cut (completeness by revision),
* that scope's exchange orders (states, cumulative executed quantity),
* that scope's exchange fills (trade ids and quantities),
* the latest account position snapshot and reconciliation head.

It never writes and never repairs: every statement is a SELECT, and the report
names the missing or disagreeing evidence instead of substituting another
account's numbers. Run it against the affected account before changing state.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    AccountReconciliationHeadRow,
    ExchangeFillRow,
    ExchangeOrderRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

log = structlog.get_logger(__name__)


def _identities(value: object) -> tuple[str, ...]:
    """Read a persisted identity list without trusting its JSON shape."""
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item) for item in value)


def _amount(value: Decimal | None) -> str:
    """Render a quantity without trailing scale noise (5.000... -> 5)."""
    return "unknown" if value is None else f"{value.normalize():f}"


@dataclass(slots=True)
class ScopeEvidence:
    """One scope's evidence, each fact tagged with its own cut time."""

    account_label: str
    symbol: str
    position_side: str
    findings: list[str] = field(default_factory=list)
    head_revision: int | None = None
    head_stream: tuple[str | None, str | None] = (None, None)
    head_journal_revision: int | None = None
    head_updated_at: datetime | None = None
    checkpoint_id: str | None = None
    checkpoint_event_cut: datetime | None = None
    checkpoint_source_revision: int | None = None
    checkpoint_coverage_end: datetime | None = None
    checkpoint_active_quantity: Decimal | None = None
    checkpoint_conflicts: bool = False
    suffix_expected: int = 0
    suffix_distinct_revisions: int = 0
    suffix_newest_at: datetime | None = None
    order_executed_total: Decimal = Decimal("0")
    order_count: int = 0
    order_latest_at: datetime | None = None
    fill_quantity_total: Decimal = Decimal("0")
    fill_count: int = 0
    fill_latest_at: datetime | None = None
    snapshot_position_amt: Decimal | None = None
    snapshot_observed_at: datetime | None = None
    reconciliation_status: str | None = None
    reconciliation_mismatches: int | None = None

    def verdicts(self) -> tuple[str, ...]:
        """Report every disagreement visible here, plus what cannot be judged.

        Structural proof problems are reported first; the independent
        order/fill/snapshot comparisons always run so an obvious disagreement
        is never hidden behind a missing checkpoint.
        """
        findings = list(self.findings)
        if self.head_revision is None:
            findings.append("no_execution_head_for_scope")
        elif self.checkpoint_id is None:
            findings.append("head_has_no_bound_checkpoint")
        else:
            if self.checkpoint_conflicts:
                findings.append("checkpoint_records_unresolved_conflicts")
            if self.suffix_distinct_revisions != self.suffix_expected:
                findings.append(
                    "journal_suffix_incomplete"
                    f"(expected={self.suffix_expected}, "
                    f"persisted={self.suffix_distinct_revisions})"
                )
            if (
                self.checkpoint_coverage_end is not None
                and self.order_latest_at is not None
                and self.checkpoint_coverage_end < self.order_latest_at
            ):
                findings.append("order_fact_newer_than_coverage")
        if self.order_executed_total != self.fill_quantity_total:
            findings.append(
                "orders_vs_fills_mismatch"
                f"(orders={_amount(self.order_executed_total)}, "
                f"fills={_amount(self.fill_quantity_total)})"
            )
        if (
            self.snapshot_position_amt is not None
            and self.checkpoint_active_quantity is not None
            and self.snapshot_position_amt != self.checkpoint_active_quantity
        ):
            findings.append(
                "snapshot_vs_projection_mismatch"
                f"(snapshot={_amount(self.snapshot_position_amt)}, "
                f"projection={_amount(self.checkpoint_active_quantity)})"
            )
        if (self.reconciliation_mismatches or 0) > 0:
            findings.append(
                f"reconciliation_mismatches={self.reconciliation_mismatches}"
            )
        return tuple(findings)


async def collect_scope_evidence(
    sessions: async_sessionmaker[AsyncSession],
    *,
    account_label: str,
    symbol: str,
    position_side: str,
) -> ScopeEvidence:
    """Collect every fact the report needs, whether or not proof is complete."""
    evidence = ScopeEvidence(
        account_label=account_label,
        symbol=symbol,
        position_side=position_side,
    )
    async with sessions() as session:
        await _collect_recovery_evidence(
            session,
            evidence,
            account_label=account_label,
            symbol=symbol,
            position_side=position_side,
        )
        await _collect_market_evidence(session, evidence)
    return evidence


async def _collect_recovery_evidence(
    session: AsyncSession,
    evidence: ScopeEvidence,
    *,
    account_label: str,
    symbol: str,
    position_side: str,
) -> None:
    """Head, checkpoint and persisted suffix; missing pieces are findings."""
    head = await session.scalar(
        select(ExecutionBookHeadRow).where(
            ExecutionBookHeadRow.environment == "live",
            ExecutionBookHeadRow.account_label == account_label,
            ExecutionBookHeadRow.symbol == symbol,
            ExecutionBookHeadRow.position_side == position_side,
            ExecutionBookHeadRow.stream_id.is_not(None),
        )
    )
    if head is None:
        return
    evidence.head_revision = head.revision
    evidence.head_stream = (head.stream_id, head.stream_epoch)
    evidence.head_updated_at = head.updated_at
    payload = head.state_payload or {}
    revision = payload.get("journal_revision")
    evidence.head_journal_revision = revision if isinstance(revision, int) else None
    for identity in _identities(payload.get("recovery_command_ids")):
        evidence.findings.append(f"recovery_command_id={identity}")
    for identity in _identities(payload.get("external_recovery_ids")):
        evidence.findings.append(f"external_recovery_id={identity}")

    binding = payload.get("recovery_checkpoint")
    checkpoint_id = (
        str(binding.get("checkpoint_id"))
        if isinstance(binding, dict) and binding.get("checkpoint_id")
        else None
    )
    evidence.checkpoint_id = checkpoint_id
    if checkpoint_id is None:
        return

    row = await session.scalar(
        select(PositionRecoveryCheckpointRow).where(
            PositionRecoveryCheckpointRow.environment == "live",
            PositionRecoveryCheckpointRow.account_label == account_label,
            PositionRecoveryCheckpointRow.symbol == symbol,
            PositionRecoveryCheckpointRow.position_side == position_side,
            PositionRecoveryCheckpointRow.stream_id == head.stream_id,
            PositionRecoveryCheckpointRow.stream_epoch == head.stream_epoch,
            PositionRecoveryCheckpointRow.checkpoint_id == checkpoint_id,
        )
    )
    if row is None:
        evidence.findings.append("bound_checkpoint_row_missing")
        return
    evidence.checkpoint_event_cut = row.event_cut
    evidence.checkpoint_source_revision = row.source_revision
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )

    checkpoint = PositionRecoveryCodec.decode_checkpoint(row.payload)
    coverage = checkpoint.coverage
    evidence.checkpoint_coverage_end = (
        coverage.end_at if coverage is not None else None
    )
    evidence.checkpoint_active_quantity = (
        checkpoint.projection.total_active_quantity
    )
    evidence.checkpoint_conflicts = bool(
        checkpoint.has_conflicts
        or checkpoint.has_synthetic_fills
        or checkpoint.has_late_events
        or checkpoint.integrity_issues
    )
    if evidence.head_journal_revision is not None:
        evidence.suffix_expected = max(
            0, evidence.head_journal_revision - row.source_revision
        )
    suffix = (
        await session.execute(
            select(
                func.count(
                    func.distinct(PositionFactJournalEventRow.source_revision)
                ),
                func.max(PositionFactJournalEventRow.occurred_at),
            ).where(
                PositionFactJournalEventRow.environment == "live",
                PositionFactJournalEventRow.account_label == account_label,
                PositionFactJournalEventRow.symbol == symbol,
                PositionFactJournalEventRow.position_side == position_side,
                PositionFactJournalEventRow.stream_id == head.stream_id,
                PositionFactJournalEventRow.stream_epoch == head.stream_epoch,
                PositionFactJournalEventRow.source_revision > row.source_revision,
            )
        )
    ).one()
    evidence.suffix_distinct_revisions = int(suffix[0] or 0)
    evidence.suffix_newest_at = suffix[1]


async def _collect_market_evidence(
    session: AsyncSession,
    evidence: ScopeEvidence,
) -> None:
    """Orders, fills, latest snapshot and reconciliation head for the scope."""
    orders = (
        await session.scalars(
            select(ExchangeOrderRow).where(
                ExchangeOrderRow.symbol == evidence.symbol,
                ExchangeOrderRow.position_side == evidence.position_side,
            )
        )
    ).all()
    evidence.order_count = len(orders)
    if orders:
        evidence.order_executed_total = sum(
            (Decimal(o.executed_quantity or 0) for o in orders),
            Decimal("0"),
        )
        evidence.order_latest_at = max(o.updated_at for o in orders)
    order_ids = tuple(o.client_order_id for o in orders)
    if order_ids:
        fills = (
            await session.scalars(
                select(ExchangeFillRow).where(
                    ExchangeFillRow.client_order_id.in_(order_ids)
                )
            )
        ).all()
        evidence.fill_count = len(fills)
        if fills:
            evidence.fill_quantity_total = sum(
                (Decimal(f.quantity or 0) for f in fills), Decimal("0")
            )
            evidence.fill_latest_at = max(f.filled_at for f in fills)

    snapshot = await session.scalar(
        select(AccountPositionSnapshotRow)
        .where(
            AccountPositionSnapshotRow.environment == "live",
            AccountPositionSnapshotRow.account_label == evidence.account_label,
            AccountPositionSnapshotRow.symbol == evidence.symbol,
            AccountPositionSnapshotRow.position_side == evidence.position_side,
        )
        .order_by(AccountPositionSnapshotRow.observed_at.desc())
        .limit(1)
    )
    if snapshot is not None:
        evidence.snapshot_position_amt = snapshot.position_amt
        evidence.snapshot_observed_at = snapshot.observed_at

    reconciliation = await session.scalar(
        select(AccountReconciliationHeadRow).where(
            AccountReconciliationHeadRow.environment == "live",
            AccountReconciliationHeadRow.account_label == evidence.account_label,
        )
    )
    if reconciliation is not None:
        evidence.reconciliation_status = reconciliation.status
        evidence.reconciliation_mismatches = reconciliation.mismatch_count


def render(evidence: ScopeEvidence) -> str:
    lines = [
        f"scope {evidence.account_label} {evidence.symbol} "
        f"{evidence.position_side}",
        f"  head revision={evidence.head_revision} "
        f"journal_revision={evidence.head_journal_revision} "
        f"updated_at={evidence.head_updated_at} "
        f"stream={evidence.head_stream[0]}@{evidence.head_stream[1]}",
        f"  checkpoint id={evidence.checkpoint_id} "
        f"cut={evidence.checkpoint_event_cut} "
        f"source_revision={evidence.checkpoint_source_revision} "
        f"coverage_end={evidence.checkpoint_coverage_end} "
        f"projection_active={_amount(evidence.checkpoint_active_quantity)} "
        f"conflicts={evidence.checkpoint_conflicts}",
        f"  suffix expected={evidence.suffix_expected} "
        f"persisted={evidence.suffix_distinct_revisions} "
        f"newest={evidence.suffix_newest_at}",
        f"  orders count={evidence.order_count} "
        f"executed_total={_amount(evidence.order_executed_total)} "
        f"latest_at={evidence.order_latest_at}",
        f"  fills count={evidence.fill_count} "
        f"quantity_total={_amount(evidence.fill_quantity_total)} "
        f"latest_at={evidence.fill_latest_at}",
        f"  snapshot amount={_amount(evidence.snapshot_position_amt)} "
        f"observed_at={evidence.snapshot_observed_at}",
        f"  reconciliation status={evidence.reconciliation_status} "
        f"mismatches={evidence.reconciliation_mismatches}",
    ]
    verdicts = evidence.verdicts()
    if verdicts:
        lines.append("  findings:")
        lines.extend(f"    - {verdict}" for verdict in verdicts)
    else:
        lines.append("  findings: none (evidence agrees and is covered)")
    return "\n".join(lines)


async def _run(args: argparse.Namespace) -> int:
    database_url = resolve_database_url(
        args.database_url,
        "CML_EXECUTION_DATABASE_URL",
        "CML_DATABASE_URL",
    )
    if not database_url:
        raise ValueError("database URL must be provided or configured")
    engine = create_async_database_engine(database_url, pool_size=1)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        evidence = await collect_scope_evidence(
            sessions,
            account_label=args.account_label,
            symbol=args.symbol.upper(),
            position_side=args.position_side.upper(),
        )
    finally:
        await engine.dispose()
    print(render(evidence))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url")
    parser.add_argument("--account-label", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument(
        "--position-side", default="BOTH", choices=("BOTH", "LONG", "SHORT")
    )
    args = parser.parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())


__all__: list[str] = ["ScopeEvidence", "collect_scope_evidence", "render"]
