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
from crypto_momentum_lab.domain.execution.execution_book import (
    Applied,
    ExecutionBook,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    StreamCheckpointAdoption,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _book(factory):
    orders = PostgresOrderRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="epoch-adoption-test"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        journal_store=PostgresAccountJournalStore(),
        order_repository=orders,
        reservation_repository=reservations,
    )
    return ExecutionBook(
        command_repository=orders,
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
    anchor_id = PositionRecoveryCodec.stable_snapshot_anchor_id(flat)

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
    anchor_id = PositionRecoveryCodec.stable_snapshot_anchor_id(snapshot)

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
                snapshot=_snapshot(
                    key, anchor_at + timedelta(minutes=1), "0", "0"
                ),
                proof=rejected_proof,
            )
        )
        assert not isinstance(result, Applied)
        current_view = await book.read(ExecutionScope("live", account, "BTCUSDT"))
        assert current_view.projection_version == old_view.projection_version
    finally:
        await engine.dispose()
