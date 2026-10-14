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
            # start must be >= provenance.source_anchor_event_cut (checkpoint_cut)
            # for proves_complete to succeed and coverage to be CONFIRMED.
            newer_coverage = _coverage(
                scope,
                load_id="newer-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            legal_coverage = compose_fact_coverage(
                newer_coverage,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=scope,
            )
            assert legal_coverage.is_authoritative, (
                "the positive fixture must produce authoritative coverage"
            )
            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="newer-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(
                        legal_coverage
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


@pytest.mark.asyncio
async def test_non_authoritative_suffix_coverage_must_be_unknown(
    async_database_url: str,
) -> None:
    """Coverage with start before anchor cut is PENDING, not authoritative.

    ``compose_fact_coverage`` with start < source_anchor_event_cut fails
    ``proves_complete``, producing PENDING status.  The read model must not
    accept this as proof of current risk.
    """
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # start < source_anchor_event_cut => proves_complete fails =>
            # PENDING, non-authoritative.
            bad_coverage = _coverage(
                scope,
                load_id="bad-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            bad_interval = compose_fact_coverage(
                bad_coverage,
                start=checkpoint_cut - timedelta(minutes=1),
                end=head_at,
                expected_scope=scope,
            )
            assert not bad_interval.is_authoritative, (
                "the negative fixture must produce non-authoritative coverage"
            )

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="bad-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(bad_interval),
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
        assert summary.gaps is None, (
            "non-authoritative suffix coverage must not prove current risk"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_suffix_coverage_with_wrong_stream_scope_must_be_unknown(
    async_database_url: str,
) -> None:
    """Coverage with a mismatched stream_scope cannot prove this head."""
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # Build a coverage for a DIFFERENT stream (wrong_epoch).
            wrong_scope = AccountFactStreamScope.for_position_key(
                _key, stream_id="account-hub", stream_epoch="wrong-epoch"
            )
            wrong_coverage = _coverage(
                wrong_scope,
                load_id="wrong-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            wrong_interval = compose_fact_coverage(
                wrong_coverage,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=wrong_scope,
            )
            assert wrong_interval.is_authoritative

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="wrong-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(wrong_interval),
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
        assert summary.gaps is None, (
            "coverage with wrong stream_epoch must not prove current risk"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sparse_suffix_revisions_with_coverage_prove_current_head(
    async_database_url: str,
) -> None:
    """Revision gaps are valid when the latest transaction reached the head."""
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

            # This transaction can advance multiple fact revisions while the
            # store writes its changed facts at the final revision.
            session.add(
                _journal_row(
                    scope=scope,
                    kind="late_fill",
                    event_id=f"suffix-{base_revision + 1}",
                    revision=base_revision + 1,
                    occurred_at=checkpoint_cut + timedelta(minutes=1),
                    payload={"trade_id": f"suffix-{base_revision + 1}"},
                )
            )
            head_revision = base_revision + 3

            good_coverage = _coverage(
                scope,
                load_id="good-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            good_interval = compose_fact_coverage(
                good_coverage,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=scope,
            )
            assert good_interval.is_authoritative

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="good-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(good_interval),
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
            "a sparse revision suffix ending at the head should prove current risk"
        )
        assert summary.reasons == ()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_coverage_with_old_payload_end_at_is_unknown_even_if_occurred_at_is_new(
    async_database_url: str,
) -> None:
    """occurred_at is NOT a proxy for coverage end_at.

    The coverage event's ``occurred_at`` may be recent, but the decoded
    payload's real ``end_at`` may be older.  The read model must use the
    decoded ``end_at``, not ``occurred_at``, to decide proof.
    """
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # Build a coverage whose real end_at only reaches checkpoint_cut,
            # not head_at.  The occurred_at is set to head_at (recent), but
            # the payload's end_at is checkpoint_cut (old).
            old_coverage = _coverage(
                scope,
                load_id="old-end-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=checkpoint_cut,  # end_at = checkpoint_cut
            )
            old_interval = compose_fact_coverage(
                old_coverage,
                start=checkpoint_cut,
                end=checkpoint_cut,  # end_at = checkpoint_cut, not head_at
                expected_scope=scope,
            )
            assert old_interval.is_authoritative
            assert old_interval.end_at == checkpoint_cut

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="old-end-coverage",
                    revision=head_revision,
                    occurred_at=head_at,  # recent — but irrelevant
                    payload=PositionRecoveryCodec.encode_coverage(old_interval),
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
        assert summary.gaps is None, (
            "coverage with old payload end_at must not prove current risk "
            "even if occurred_at is recent"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_coverage_with_wrong_environment_is_unknown(
    async_database_url: str,
) -> None:
    """Coverage with stream_scope.environment != 'live' cannot prove live risk."""
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # Build a coverage for a different environment (paper, not live).
            from crypto_momentum_lab.domain.execution.order_state import (
                FuturesPositionSide,
            )
            from crypto_momentum_lab.domain.execution.position_ledger_models import (
                AccountFactStreamScope,
                PositionKey,
            )

            paper_key = PositionKey(
                environment="paper",
                account_label=account,
                symbol=_key.symbol,
                position_side=FuturesPositionSide(_key.position_side.value),
            )
            paper_scope = AccountFactStreamScope.for_position_key(
                paper_key, stream_id="account-hub", stream_epoch="epoch-1"
            )
            paper_coverage = _coverage(
                paper_scope,
                load_id="paper-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            paper_interval = compose_fact_coverage(
                paper_coverage,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=paper_scope,
            )
            assert paper_interval.is_authoritative

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="paper-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(paper_interval),
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
        assert summary.gaps is None, (
            "coverage with environment='paper' must not prove live risk"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_coverage_that_does_not_cover_bound_cut_is_unknown(
    async_database_url: str,
) -> None:
    """Coverage with start > bound checkpoint event_cut cannot bridge the gap.

    ``compose_fact_coverage(start=cut+1min, end=cut+5min)`` is authoritative
    (``is_authoritative=True``) but ``covers(cut)`` is False because the
    interval starts after the bound cut.  The read model must reject this as
    UNKNOWN — the coverage does not connect to the bound recovery cut.
    """
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # Coverage starts AFTER the bound cut — authoritative but does
            # not bridge the gap to the bound recovery cut.
            gap_coverage = _coverage(
                scope,
                load_id="gap-scan",
                scan_origin=checkpoint_cut,
                anchor_id=checkpoint_row.checkpoint_id,
                anchor_cut=checkpoint_cut,
                anchor_kind="recovery_checkpoint",
                checked_through=head_at,
            )
            gap_interval = compose_fact_coverage(
                gap_coverage,
                start=checkpoint_cut + timedelta(minutes=1),  # AFTER bound cut
                end=head_at,
                expected_scope=scope,
            )
            assert gap_interval.is_authoritative, (
                "fixture must produce authoritative coverage"
            )
            assert not gap_interval.covers(checkpoint_cut), (
                "fixture must NOT cover the bound cut"
            )

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="gap-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(gap_interval),
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
        assert summary.gaps is None, (
            "coverage that does not cover the bound cut must not prove "
            f"current risk, got gaps={summary.gaps} reasons={summary.reasons}"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_zero_snapshot_from_unknown_source_is_unknown(
    async_database_url: str,
) -> None:
    """A zero_snapshot anchor with mismatched source id/cut cannot prove.

    The bound checkpoint was built from a specific zero_snapshot source.
    A suffix coverage claiming a different zero_snapshot source_anchor_id
    must be rejected.
    """
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # Coverage with a zero_snapshot anchor referencing a DIFFERENT
            # source than the bound checkpoint's original zero_snapshot.
            bad_zero_coverage = _coverage(
                scope,
                load_id="bad-zero-scan",
                scan_origin=checkpoint_cut,
                anchor_id="fake-zero-snapshot-id",  # wrong source
                anchor_cut=checkpoint_cut,
                anchor_kind="zero_snapshot",
                checked_through=head_at,
            )
            bad_interval = compose_fact_coverage(
                bad_zero_coverage,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=scope,
            )
            assert bad_interval.is_authoritative

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="bad-zero-coverage",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(bad_interval),
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
        assert summary.gaps is None, (
            "zero_snapshot from unknown source must not prove current risk"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_zero_snapshot_when_cp_source_is_not_zero_is_unknown(
    async_database_url: str,
) -> None:
    """When the bound checkpoint's coverage anchor is recovery_checkpoint
    (not zero_snapshot), a suffix coverage claiming a zero_snapshot anchor
    cannot be verified and must be UNKNOWN.

    This is the fail-closed case: cp_coverage.load_provenance exists but
    source_anchor_kind != "zero_snapshot".
    """
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
            checkpoint_row = await session.scalar(
                select(PositionRecoveryCheckpointRow).where(
                    PositionRecoveryCheckpointRow.account_label == account,
                    PositionRecoveryCheckpointRow.checkpoint_id
                    == str(binding["checkpoint_id"]),
                )
            )
            assert checkpoint_row is not None
            base_revision = checkpoint_row.source_revision

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

            # The bound checkpoint's original coverage uses a
            # recovery_checkpoint anchor (not zero_snapshot).
            # A suffix coverage claiming a zero_snapshot anchor cannot
            # be verified against this checkpoint.
            bad_zero = _coverage(
                scope,
                load_id="bad-zero-nonzero-cp",
                scan_origin=checkpoint_cut,
                anchor_id="some-zero-id",
                anchor_cut=checkpoint_cut,
                anchor_kind="zero_snapshot",
                checked_through=head_at,
            )
            bad_interval = compose_fact_coverage(
                bad_zero,
                start=checkpoint_cut,
                end=head_at,
                expected_scope=scope,
            )
            assert bad_interval.is_authoritative

            session.add(
                _journal_row(
                    scope=scope,
                    kind="coverage",
                    event_id="bad-zero-nonzero-cp",
                    revision=head_revision,
                    occurred_at=head_at,
                    payload=PositionRecoveryCodec.encode_coverage(bad_interval),
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
        assert summary.gaps is None, (
            "zero_snapshot when cp source is not zero_snapshot must be UNKNOWN"
        )
        assert REASON_COVERAGE_STALE in summary.reasons
    finally:
        await engine.dispose()
