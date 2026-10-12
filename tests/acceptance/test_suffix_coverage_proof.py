"""SYSTEM.md section 8: old checkpoint + complete suffix + latest legal coverage.

"新覆盖与状态更新而旧checkpoint未变｜当前证明仍能使用最新合法覆盖；缺覆盖仍待
证据，真实冲突仍隔离" and T1 "修正当前 coverage 读取".

This is the end-to-end acceptance for that known gap. A recovery checkpoint is
published at an older cut; the journal suffix after it is complete; and a newer
authoritative coverage scan has already proven a later cut. The account must
read as proven for current risk even though its bound checkpoint never moved.

The checkpoint and head rows are produced by the real authority book against
real PostgreSQL. The suffix conditions the current engine cannot yet produce
(a head revision advanced past its bound checkpoint plus the matching later
coverage scan) are seeded as journal rows through the same codec and table
contract the read model queries. Coverage windows respect the documented rule
that a scan's coverage never claims a cut beyond the checkpoint it was built
from.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import Applied
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    PositionKey,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
    load_fact_integrity,
)
from crypto_momentum_lab.operator_dashboard.fact_proof import (
    REASON_COVERAGE_STALE,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


class _AccountSummary:
    """Minimal account read-model shape consumed by load_fact_integrity."""

    def __init__(self, *, position_count: int) -> None:
        self.position_count = position_count


def _book(factory: async_sessionmaker) -> ExecutionBook:
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="suffix-coverage-test"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    return ExecutionBook(
        command_repository=commands,
        reservation_repository=reservations,
        execution_unit_of_work=uow,
    )


def _snapshot(key: PositionKey, at: datetime, quantity: str) -> AccountPositionSnapshot:
    amount = Decimal(quantity)
    return AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=amount,
        entry_price=Decimal("100"),
        mark_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        notional=abs(amount) * Decimal("100"),
        leverage=2,
        margin_type="cross",
        observed_at=at,
        raw_payload={
            "positionSide": key.position_side.value,
            "include_flat": amount == 0,
        },
    )


def _coverage(
    scope: AccountFactStreamScope,
    *,
    load_id: str,
    scan_origin: datetime,
    anchor_id: str,
    anchor_cut: datetime,
    anchor_kind: str,
    checked_through: datetime,
) -> CoverageEvidence:
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id=load_id,
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(scan_origin.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=checked_through,
        observed_at=checked_through,
        source_anchor_id=anchor_id,
        source_anchor_event_cut=anchor_cut,
        source_anchor_kind=anchor_kind,
    )
    endpoint_checkpoint_id = (
        anchor_id
        if anchor_kind == "zero_snapshot" and checked_through == anchor_cut
        else f"source-cut-{load_id}"
    )
    return CoverageEvidence(
        fill_cursor_id=load_id,
        fill_load_start=provenance.origin_start_at,
        fill_checked_through=checked_through,
        checkpoint_id=endpoint_checkpoint_id,
        checkpoint_event_cut=checked_through,
        stream_scope=scope,
        evidence_observed_at=checked_through,
        page_exhausted=True,
        not_truncated=True,
        load_provenance=provenance,
    )


def _evidence(
    key: PositionKey,
    *,
    evidence_id: str,
    scope: AccountFactStreamScope,
    at: datetime,
    sequence: int,
    snapshot: AccountPositionSnapshot | None = None,
    fill: AccountFillEvent | None = None,
    proof: CoverageEvidence,
) -> ExecutionEvidence:
    return ExecutionEvidence(
        evidence_id=evidence_id,
        scope=ExecutionScope(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side=key.position_side,
        ),
        observed_at=at,
        snapshot=snapshot,
        fill=fill,
        coverage_evidence=proof,
        fill_load_provenance=proof.load_provenance,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        sequence=sequence,
    )


def _canonical_digest(value: object) -> str:
    """The same canonical JSON hashing the journal store applies to payloads."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _journal_row(
    *,
    scope: AccountFactStreamScope,
    kind: str,
    event_id: str,
    revision: int,
    occurred_at: datetime,
    payload: dict[str, object],
) -> PositionFactJournalEventRow:
    payload_hash = _canonical_digest(payload)
    return PositionFactJournalEventRow(
        event_record_id=_canonical_digest(
            [scope.canonical_id, kind, event_id, payload_hash]
        ),
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        event_kind=kind,
        event_id=event_id,
        payload_hash=payload_hash,
        source_revision=revision,
        occurred_at=occurred_at,
        payload=payload,
    )


async def _publish_real_checkpoint(
    factory: async_sessionmaker,
) -> tuple[str, AccountFactStreamScope, PositionKey, datetime, datetime]:
    """Publish a real checkpoint and head through the real authority book."""
    account = f"suffix-coverage-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-hub", stream_epoch="epoch-1"
    )
    opened_at = NOW - timedelta(minutes=20)
    checkpoint_cut = NOW - timedelta(minutes=15)

    book = _book(factory)
    await book.restore(account_label=account)
    anchor_id = stable_snapshot_anchor_id(_snapshot(key, opened_at, "0"))
    flat = await book.observe(
        _evidence(
            key,
            evidence_id="flat-anchor",
            scope=scope,
            at=opened_at,
            sequence=1,
            snapshot=_snapshot(key, opened_at, "0"),
            proof=_coverage(
                scope,
                load_id="flat-scan",
                scan_origin=opened_at,
                anchor_id=anchor_id,
                anchor_cut=opened_at,
                anchor_kind="zero_snapshot",
                checked_through=opened_at,
            ),
        )
    )
    assert isinstance(flat, Applied), flat
    opened = await book.observe(
        _evidence(
            key,
            evidence_id="recovery-cut",
            scope=scope,
            at=checkpoint_cut,
            sequence=2,
            snapshot=_snapshot(key, checkpoint_cut, "2"),
            fill=AccountFillEvent(
                environment=key.environment,
                account_label=key.account_label,
                symbol=key.symbol,
                trade_id="entry-1",
                order_id="order-entry-1",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("2"),
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=checkpoint_cut,
                raw_payload={"positionSide": key.position_side.value},
            ),
            proof=_coverage(
                scope,
                load_id="recovery-scan",
                scan_origin=opened_at,
                anchor_id=anchor_id,
                anchor_cut=opened_at,
                anchor_kind="zero_snapshot",
                checked_through=checkpoint_cut,
            ),
        )
    )
    assert isinstance(opened, Applied), opened
    return account, scope, key, opened_at, checkpoint_cut


@pytest.mark.asyncio
async def test_newer_legal_coverage_proves_an_old_checkpoint_with_complete_suffix(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        (
            account,
            scope,
            _key,
            _opened_at,
            checkpoint_cut,
        ) = await _publish_real_checkpoint(factory)
        head_at = checkpoint_cut + timedelta(minutes=5)

        async with factory() as session:
            head = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account
                )
            )
            assert head is not None
            binding = head.state_payload["recovery_checkpoint"]
            assert isinstance(binding, dict)
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            assert checkpoint_row.event_cut == checkpoint_cut
            base_revision = checkpoint_row.source_revision

            # A complete journal suffix strictly after the checkpoint revision:
            # distinct revisions up to and including the head's own revision.
            suffix_revisions = [base_revision + 1, base_revision + 2]
            for offset, revision in enumerate(suffix_revisions, start=1):
                session.add(
                    _journal_row(
                        scope=scope,
                        kind="late_fill",
                        event_id=f"suffix-{revision}",
                        revision=revision,
                        occurred_at=checkpoint_cut + timedelta(minutes=offset),
                        payload={"trade_id": f"suffix-{revision}"},
                    )
                )
            head_revision = suffix_revisions[-1]

            # The newest authoritative scan reaches the head's own cut.
            newer_coverage = _coverage(
                scope,
                load_id="newer-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="newer-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(
                        compose_fact_coverage(
                            newer_coverage,
                            start=checkpoint_cut - timedelta(minutes=1),
                            end=head_at,
                            expected_scope=scope,
                        )
                    ),
                )
            )
            checkpoint = PositionRecoveryCodec.decode_checkpoint(
                checkpoint_row.payload
            )
            await session.execute(
                update(ExecutionBookHeadRow)
                .where(ExecutionBookHeadRow.account_label == account)
                .values(
                    state_payload={
                        **head.state_payload,
                        "journal_revision": head_revision,
                        "recovery_checkpoint": recovery_checkpoint_head_binding(
                            checkpoint
                        ),
                    }
                )
            )
            await session.commit()

        integrity = await load_fact_integrity(
            factory,
            accounts={account: _AccountSummary(position_count=1)},
            now=NOW,
            max_age_seconds=3600.0,
        )

        summary = integrity[account]
        assert summary.gaps == 0, (
            "an old checkpoint with a complete suffix and the latest legal "
            f"coverage must prove current risk, got {summary.reasons}"
        )
        assert summary.unknown_scopes == ()
        assert summary.reasons == (), (
            "the read model ignored the newer persisted coverage and re-derived "
            "the cut from the bound checkpoint's frozen coverage: "
            f"{summary.reasons} (expected {REASON_COVERAGE_STALE} today)"
        )

        # Negative control: dropping the later coverage scan leaves the same
        # head and suffix unknown, so the proof above came from the new coverage.
        async with factory() as session:
            await session.execute(
                delete(PositionFactJournalEventRow).where(
                    PositionFactJournalEventRow.account_label == account,
                    PositionFactJournalEventRow.event_kind == "coverage",
                    PositionFactJournalEventRow.event_id == "newer-coverage",
                )
            )
            await session.commit()

        without_newer = await load_fact_integrity(
            factory,
            accounts={account: _AccountSummary(position_count=1)},
            now=NOW,
            max_age_seconds=3600.0,
        )
        assert without_newer[account].gaps != 0
    finally:
        await engine.dispose()
