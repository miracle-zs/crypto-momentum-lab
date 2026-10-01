"""Real PostgreSQL recovery across execution stream epochs."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    StreamCheckpointAdoption,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
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
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _book(factory):
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="epoch-adoption-test"
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


def _snapshot(key: PositionKey, at: datetime, quantity: str, entry_price: str):
    amount = Decimal(quantity)
    return AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=amount,
        entry_price=Decimal(entry_price),
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


def _fill(key: PositionKey, trade_id: str, side: str, qty: str, at: datetime):
    return AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id=trade_id,
        order_id=f"order-{trade_id}",
        side=side,
        price=Decimal("100"),
        quantity=Decimal(qty),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=at,
        raw_payload={"positionSide": key.position_side.value},
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
    adoption: StreamCheckpointAdoption | None = None,
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
        stream_checkpoint_adoption=adoption,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        sequence=sequence,
    )


@pytest.mark.asyncio
async def test_nonzero_checkpoint_adoption_survives_restart_and_carries_batches(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"epoch-adoption-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    old_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-hub", stream_epoch="epoch-old"
    )
    new_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-hub", stream_epoch="epoch-new"
    )
    flat_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    parent_at = flat_at + timedelta(minutes=1)
    target_at = flat_at + timedelta(minutes=2)
    flat = _snapshot(key, flat_at, "0", "0")
    parent_snapshot = _snapshot(key, parent_at, "2", "100")
    target_snapshot = _snapshot(key, target_at, "1.5", "100")
    anchor_id = stable_snapshot_anchor_id(flat)

    try:
        book = _book(factory)
        await book.restore(account_label=account)
        initial = _coverage(
            old_scope,
            load_id="old-flat-scan",
            scan_origin=flat_at,
            anchor_id=anchor_id,
            anchor_cut=flat_at,
            anchor_kind="zero_snapshot",
            checked_through=flat_at,
        )
        assert isinstance(
            await book.observe(
                _evidence(
                    key,
                    evidence_id="old-flat",
                    scope=old_scope,
                    at=flat_at,
                    sequence=1,
                    snapshot=flat,
                    proof=initial,
                )
            ),
            Applied,
        )

        old_coverage = _coverage(
            old_scope,
            load_id="old-position-scan",
            scan_origin=flat_at,
            anchor_id=anchor_id,
            anchor_cut=flat_at,
            anchor_kind="zero_snapshot",
            checked_through=parent_at,
        )
        assert isinstance(
            await book.observe(
                _evidence(
                    key,
                    evidence_id="old-open",
                    scope=old_scope,
                    at=parent_at,
                    sequence=2,
                    snapshot=parent_snapshot,
                    fill=_fill(key, "old-open-trade", "BUY", "2", parent_at),
                    proof=old_coverage,
                )
            ),
            Applied,
        )
        original_parent = await book.load_recovery_checkpoint(old_scope)
        assert original_parent is not None
        assert original_parent.projection.total_active_quantity == Decimal("2")

        # Recreate the service before changing streams so the parent must be
        # loaded from PostgreSQL and matched to the durable execution head.
        book = _book(factory)
        await book.restore(account_label=account)
        parent = await book.load_recovery_checkpoint(old_scope)
        assert parent == original_parent

        target_coverage = _coverage(
            new_scope,
            load_id="new-stream-scan",
            scan_origin=flat_at,
            anchor_id=parent.checkpoint_id,
            anchor_cut=parent.event_cut,
            anchor_kind="recovery_checkpoint",
            checked_through=target_at,
        )
        adoption = StreamCheckpointAdoption(
            parent_checkpoint=parent,
            target_scope=new_scope,
            target_event_cut=target_at,
            fill_load_provenance=target_coverage.load_provenance,
        )
        result = await book.observe(
            _evidence(
                key,
                evidence_id="new-reduce",
                scope=new_scope,
                at=target_at,
                sequence=1,
                snapshot=target_snapshot,
                fill=_fill(key, "new-reduce-trade", "SELL", "0.5", target_at),
                proof=target_coverage,
                adoption=adoption,
            )
        )
        assert isinstance(result, Applied), result
        adopted = await book.read(ExecutionScope("live", account, "BTCUSDT"))
        assert adopted.total_quantity == Decimal("1.5")
        # The adopted suffix carries the parent batch identity forward; the SELL
        # observed together with the adoption moves the quantity, not the identity.
        assert len(adopted.batches) == 1
        assert len(parent.projection.active_batches) == 1
        adopted_batch = adopted.batches[0]
        parent_batch = parent.projection.active_batches[0]
        assert adopted_batch.batch_id == parent_batch.batch_id
        assert adopted_batch.episode_id == parent_batch.episode_id
        assert adopted_batch.order_id == parent_batch.order_id
        assert adopted_batch.opened_at == parent_batch.opened_at
        assert adopted_batch.original_quantity == parent_batch.original_quantity
        assert adopted_batch.quantity == Decimal("1.5")

        restarted = _book(factory)
        await restarted.restore(account_label=account)
        restored = await restarted.read(ExecutionScope("live", account, "BTCUSDT"))
        target_checkpoint = await restarted.load_recovery_checkpoint(new_scope)
        assert target_checkpoint is not None
        assert target_checkpoint.parent_stream_scope == old_scope
        assert target_checkpoint.parent_checkpoint_id == parent.checkpoint_id
        assert restored.total_quantity == Decimal("1.5")
        assert restored.batches == adopted.batches
        assert restored.projection_version == adopted.projection_version
        assert target_checkpoint.projection.active_batches == adopted.batches
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_new_epoch_without_checkpoint_adoption_fails_closed(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"epoch-adoption-block-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    old_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-hub", stream_epoch="epoch-old"
    )
    new_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-hub", stream_epoch="epoch-new"
    )
    anchor_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=3)
    snapshot = _snapshot(key, anchor_at, "0", "0")
    anchor_id = stable_snapshot_anchor_id(snapshot)

    try:
        book = _book(factory)
        await book.restore(account_label=account)
        proof = _coverage(
            old_scope,
            load_id="block-old-scan",
            scan_origin=anchor_at,
            anchor_id=anchor_id,
            anchor_cut=anchor_at,
            anchor_kind="zero_snapshot",
            checked_through=anchor_at,
        )
        assert isinstance(
            await book.observe(
                _evidence(
                    key,
                    evidence_id="block-old",
                    scope=old_scope,
                    at=anchor_at,
                    sequence=1,
                    snapshot=snapshot,
                    proof=proof,
                )
            ),
            Applied,
        )
        old_view = await book.read(ExecutionScope("live", account, "BTCUSDT"))
        rejected_proof = _coverage(
            new_scope,
            load_id="block-new-scan",
            scan_origin=anchor_at,
            anchor_id="untrusted-old-checkpoint",
            anchor_cut=anchor_at,
            anchor_kind="recovery_checkpoint",
            checked_through=anchor_at + timedelta(minutes=1),
        )
        result = await book.observe(
            _evidence(
                key,
                evidence_id="block-new",
                scope=new_scope,
                at=anchor_at + timedelta(minutes=1),
                sequence=1,
                snapshot=_snapshot(key, anchor_at + timedelta(minutes=1), "0", "0"),
                proof=rejected_proof,
            )
        )
        assert not isinstance(result, Applied)
        current_view = await book.read(ExecutionScope("live", account, "BTCUSDT"))
        assert current_view.projection_version == old_view.projection_version
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [True, False])
@pytest.mark.parametrize("empty_suffix", [True, False])
async def test_runtime_full_zero_anchored_scan_repairs_stale_position_atomically(
    async_database_url,
    complete,
    empty_suffix,
):
    from crypto_momentum_lab.domain.account.models import (
        AccountFillLoadScan,
        AccountFillPageScan,
    )
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"business-{uuid4().hex[:12]}"
    key = PositionKey("live", account, "BTCUSDT", "LONG")
    scope = ExecutionScope("live", account, "BTCUSDT", key.position_side)
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    entry = _fill(key, "entry", "BUY", "2", start + timedelta(minutes=1))
    exit_fill = _fill(key, "exit", "SELL", "2", start + timedelta(minutes=2))
    old_snapshot = _snapshot(key, start + timedelta(minutes=1), "2", "100")
    from dataclasses import replace

    baseline = replace(
        _snapshot(
            key, start + timedelta(minutes=2) if empty_suffix else start, "0", "0"
        ),
        raw_payload={"symbol": "BTCUSDT", "positionAmt": "0.000", "updateTime": 0},
    )
    target = _snapshot(key, start + timedelta(minutes=3), "0", "0")
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        result = await book.observe(
            ExecutionEvidence(
                "old-open",
                scope,
                old_snapshot.observed_at,
                fill=entry,
                snapshot=old_snapshot,
                stream_id="hub",
                stream_epoch="old",
                sequence=1,
            )
        )
        assert isinstance(result, Applied)
        assert (await book.read(scope)).total_quantity == 2
        # Legacy head diagnostic counts are global, not evidence for this key.
        from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
            ExecutionBookHeadRow,
        )

        async with factory() as session, session.begin():
            session.add(
                ExecutionBookHeadRow(
                    environment="live",
                    account_label=account,
                    symbol="ETHUSDT",
                    position_side="LONG",
                    stream_id="hub",
                    stream_epoch="old",
                    revision=1,
                    projection_version="pv_initial",
                    state_payload={"seen_trade_count": 999},
                    updated_at=start,
                )
            )
        from crypto_momentum_lab.execution_account.fill_scan_plan import plan_fill_scan
        from crypto_momentum_lab.persistence.postgres.fill_recovery_sources import (
            load_fill_recovery_sources,
        )
        from crypto_momentum_lab.persistence.postgres.models import (
            AccountPositionSnapshotRow,
        )

        async with factory() as session, session.begin():
            session.add(
                AccountPositionSnapshotRow(
                    snapshot_id=uuid4(),
                    environment="live",
                    account_label=account,
                    symbol="BTCUSDT",
                    position_side="LONG",
                    position_amt=Decimal("0"),
                    entry_price=Decimal("0"),
                    mark_price=baseline.mark_price,
                    unrealized_pnl=Decimal("0"),
                    notional=Decimal("0"),
                    leverage=2,
                    margin_type="cross",
                    observed_at=baseline.observed_at,
                    raw_payload=baseline.raw_payload,
                )
            )
        initial_sources = await load_fill_recovery_sources(
            factory, environment="live", account_label=account
        )
        assert ("ETHUSDT", "LONG") not in initial_sources
        planned = plan_fill_scan(target, initial_sources[("BTCUSDT", "LONG")])
        assert planned is not None
        scan = AccountFillLoadScan(
            "live",
            account,
            "BTCUSDT",
            "LONG",
            AccountFillPageScan(
                "BTCUSDT",
                "repair-scan",
                planned.start_time_ms,
                None,
                1,
                complete,
                not complete,
                target.observed_at,
            ),
            target.observed_at,
            planned.source_anchor_id,
            planned.source_anchor_event_cut,
            planned.source_anchor_kind,
            planned.source_stream_id,
            planned.source_stream_epoch,
            source_anchor_snapshot=planned.source_anchor_snapshot,
        )
        from crypto_momentum_lab.execution_account.hub import (
            AccountEvent,
            decode_account_event,
            encode_account_event,
        )

        transported = decode_account_event(
            encode_account_event(
                AccountEvent(
                    "live",
                    account,
                    "snapshot",
                    "proof",
                    target.observed_at,
                    target.observed_at,
                    fill_load_scans=(scan,),
                ),
                sequence=1,
            ),
            expected_environment="live",
            expected_account_label=account,
        )
        scan = transported.fill_load_scans[0]
        coordinator = OrderExecutionCoordinator(
            backend=object(),
            environment="live",
            account_label=account,
            execution_book=book,
        )
        await coordinator.observe_account_snapshot(
            target,
            fills=() if empty_suffix else (entry, exit_fill),
            fill_load_scans=(scan,),
            stream_id="hub",
            stream_epoch="new",
            sequence=1,
        )
        if not complete:
            preserved = await book.read(scope, stream_id="hub", stream_epoch="old")
            assert preserved.total_quantity == 2
            assert (
                await book.load_recovery_checkpoint(
                    AccountFactStreamScope.for_position_key(
                        key, stream_id="hub", stream_epoch="new"
                    )
                )
                is None
            )
            return
        view = await book.read(scope, stream_id="hub", stream_epoch="new")
        assert view.total_quantity == 0
        checkpoint = await book.load_recovery_checkpoint(
            AccountFactStreamScope.for_position_key(
                key, stream_id="hub", stream_epoch="new"
            )
        )
        assert checkpoint is not None and checkpoint.coverage.is_authoritative
        from types import SimpleNamespace

        from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
            load_fact_integrity,
        )
        from crypto_momentum_lab.persistence.postgres.fill_recovery_sources import (
            load_fill_recovery_sources,
        )

        sources = await load_fill_recovery_sources(
            factory, environment="live", account_label=account
        )
        assert sources[("BTCUSDT", "LONG")].checkpoint_id == checkpoint.checkpoint_id
        assert sources[("BTCUSDT", "LONG")].stream_epoch == "new"
        integrity = await load_fact_integrity(
            factory,
            accounts={account: SimpleNamespace(position_count=0)},
            now=target.observed_at,
        )
        assert integrity[account].gaps == 0
        assert integrity[account].observed_at == target.observed_at
        stale = await load_fact_integrity(
            factory,
            accounts={account: SimpleNamespace(position_count=0)},
            now=target.observed_at + timedelta(minutes=4),
        )
        assert stale[account].gaps is None
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        recovered = await restarted.read(scope, stream_id="hub", stream_epoch="new")
        assert recovered.total_quantity == 0
        assert recovered.projection_version == view.projection_version
        # The next poll must load the newly adopted checkpoint and extend it,
        # rather than attempting the original zero anchor again.
        later = _snapshot(key, target.observed_at + timedelta(seconds=30), "0", "0")
        continuation = AccountFillLoadScan(
            "live",
            account,
            "BTCUSDT",
            "LONG",
            AccountFillPageScan(
                "BTCUSDT",
                "continuation",
                int(checkpoint.event_cut.timestamp() * 1000),
                None,
                1,
                True,
                False,
                later.observed_at,
            ),
            later.observed_at,
            checkpoint.checkpoint_id,
            checkpoint.event_cut,
            "recovery_checkpoint",
            "hub",
            "new",
        )
        await coordinator.observe_account_snapshot(
            later,
            fill_load_scans=(continuation,),
            stream_id="hub",
            stream_epoch="new",
            sequence=2,
        )
        extended = await book.load_recovery_checkpoint(
            AccountFactStreamScope.for_position_key(
                key, stream_id="hub", stream_epoch="new"
            )
        )
        assert extended.event_cut == later.observed_at
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [True, False])
@pytest.mark.parametrize("anchor_integrity", ["valid", "bad_hash", "wrong_scope"])
async def test_journal_only_flat_anchor_recovers_live_nonzero_position(
    async_database_url, complete, anchor_integrity
):
    from crypto_momentum_lab.domain.account.models import (
        AccountFillLoadScan,
        AccountFillPageScan,
    )
    from crypto_momentum_lab.execution_account.fill_scan_plan import plan_fill_scan
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.persistence.postgres.fill_recovery_sources import (
        load_fill_recovery_sources,
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"journal-anchor-{uuid4().hex[:12]}"
    key = PositionKey("live", account, "XVSUSDT", "LONG")
    scope = ExecutionScope("live", account, "XVSUSDT", key.position_side)
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    baseline = _snapshot(key, start, "0", "0")
    entry = _fill(key, "real-entry", "BUY", "26.6", start + timedelta(minutes=1))
    target = _snapshot(key, start + timedelta(minutes=3), "26.6", "100")
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        for sequence, snapshot, fill in [
            (1, baseline, None),
            (2, _snapshot(key, entry.trade_at, "26.6", "100"), entry),
        ]:
            assert isinstance(
                await book.observe(
                    ExecutionEvidence(
                        f"old-{sequence}",
                        scope,
                        snapshot.observed_at,
                        snapshot=snapshot,
                        fill=fill,
                        stream_id="hub",
                        stream_epoch="old",
                        sequence=sequence,
                    )
                ),
                Applied,
            )
        # Earlier runtime versions persisted explicit flat observations to the
        # immutable journal even when account snapshot retention dropped them.
        from crypto_momentum_lab.domain.execution.account_journal import AccountJournal

        old_scope = AccountFactStreamScope.for_position_key(
            key, stream_id="hub", stream_epoch="old"
        )
        history = AccountJournal(key, stream_scope=old_scope)
        history.record_snapshot(baseline)
        async with factory() as session, session.begin():
            await PostgresAccountJournalStore().persist_facts_in_session(
                session,
                scope=old_scope,
                facts=history.read_cut(),
                revision=1,
            )
        if anchor_integrity != "valid":
            import hashlib
            import json

            from sqlalchemy import select

            from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
                PositionFactJournalEventRow,
            )

            async with factory() as session, session.begin():
                row = await session.scalar(
                    select(PositionFactJournalEventRow).where(
                        PositionFactJournalEventRow.account_label == account,
                        PositionFactJournalEventRow.event_kind == "snapshot",
                        PositionFactJournalEventRow.occurred_at == baseline.observed_at,
                    )
                )
                if anchor_integrity == "bad_hash":
                    row.payload_hash = "0" * 64
                else:
                    row.payload = {**row.payload, "account_label": "other-account"}
                    row.payload_hash = hashlib.sha256(
                        json.dumps(
                            row.payload,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ).encode()
                    ).hexdigest()
        sources = await load_fill_recovery_sources(
            factory, environment="live", account_label=account
        )
        anchor = sources[("XVSUSDT", "LONG")]
        if anchor_integrity != "valid":
            assert anchor is None
            return
        assert anchor is not None, (
            "real flat journal observation must survive sparse snapshot storage"
        )
        assert anchor.zero_snapshot == baseline
        planned = plan_fill_scan(target, anchor)
        assert planned is not None
        scan = AccountFillLoadScan(
            "live",
            account,
            "XVSUSDT",
            "LONG",
            AccountFillPageScan(
                "XVSUSDT",
                "journal-repair",
                planned.start_time_ms,
                None,
                1,
                complete,
                not complete,
                target.observed_at,
            ),
            target.observed_at,
            planned.source_anchor_id,
            planned.source_anchor_event_cut,
            planned.source_anchor_kind,
            planned.source_stream_id,
            planned.source_stream_epoch,
            source_anchor_snapshot=planned.source_anchor_snapshot,
        )
        from crypto_momentum_lab.execution_account.hub import (
            AccountEvent,
            AccountEventHub,
            decode_account_event,
        )

        hub = AccountEventHub()
        hub.publish(
            AccountEvent(
                "live",
                account,
                "ORDER_TRADE_UPDATE",
                "already-seen",
                entry.trade_at,
                entry.trade_at,
                fills=(entry,),
            )
        )
        hub.publish(
            AccountEvent(
                "live",
                account,
                "ACCOUNT_SNAPSHOT",
                "complete-proof",
                target.observed_at,
                target.observed_at,
                fills=(entry,),
                fill_load_scans=(scan,),
            )
        )
        transported = decode_account_event(
            hub._latest_messages[("live", account)],
            expected_environment="live",
            expected_account_label=account,
        )
        assert transported.fills == (entry,)
        coordinator = OrderExecutionCoordinator(
            backend=object(),
            environment="live",
            account_label=account,
            execution_book=book,
        )
        await coordinator.observe_account_snapshot(
            target,
            fills=transported.fills,
            fill_load_scans=transported.fill_load_scans,
            stream_id="hub",
            stream_epoch="new",
            sequence=1,
        )
        checkpoint = await book.load_recovery_checkpoint(
            AccountFactStreamScope.for_position_key(
                key, stream_id="hub", stream_epoch="new"
            )
        )
        if not complete:
            assert checkpoint is None
            return
        assert checkpoint is not None and checkpoint.coverage.is_authoritative
        assert checkpoint.projection.total_active_quantity == Decimal("26.6")
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert (
            await restarted.read(scope, stream_id="hub", stream_epoch="new")
        ).total_quantity == Decimal("26.6")
    finally:
        await engine.dispose()
