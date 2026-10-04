"""Durable fill-scan continuity tests against PostgreSQL."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    JournalFactConflict,
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _provenance(
    scope: AccountFactStreamScope,
    *,
    load_id: str,
    anchor_cut: datetime,
    observed_at: datetime,
    request_from_id: int | None,
    next_from_id: int | None,
    exhausted: bool,
) -> AccountFillLoadProvenance:
    return AccountFillLoadProvenance(
        stream_scope=scope,
        load_id=load_id,
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(anchor_cut.timestamp() * 1000),
        request_from_id=request_from_id,
        next_from_id=next_from_id,
        page_count=1,
        page_exhausted=exhausted,
        truncated=not exhausted,
        checked_through=observed_at,
        observed_at=observed_at,
        source_anchor_id="anchor-checkpoint",
        source_anchor_event_cut=anchor_cut,
        source_anchor_kind="recovery_checkpoint",
    )


@pytest.mark.asyncio
async def test_store_restores_resumed_fill_scan_provenance(async_database_url: str):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    key = PositionKey(
        "live", f"provenance-{uuid4().hex}", "BTCUSDT", FuturesPositionSide.BOTH
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trades", stream_epoch=uuid4().hex
    )
    anchor_cut = datetime.now(UTC) - timedelta(minutes=2)
    partial_at = anchor_cut + timedelta(seconds=1)
    complete_at = anchor_cut + timedelta(seconds=2)
    first = _provenance(
        scope,
        load_id="bootstrap-load",
        anchor_cut=anchor_cut,
        observed_at=partial_at,
        request_from_id=None,
        next_from_id=100,
        exhausted=False,
    )
    resumed = _provenance(
        scope,
        load_id="bootstrap-load",
        anchor_cut=anchor_cut,
        observed_at=complete_at,
        request_from_id=100,
        next_from_id=None,
        exhausted=True,
    )
    try:
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session,
                scope=scope,
                facts=AccountFacts(
                    position_key=key,
                    stream_scope=scope,
                    fill_load_provenance=first,
                ),
                revision=1,
            )
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session,
                scope=scope,
                facts=AccountFacts(
                    position_key=key,
                    stream_scope=scope,
                    fill_load_provenance=resumed,
                ),
                revision=2,
            )
        async with factory() as session:
            cut = await store.load_recovery_in_session(
                session,
                scope=scope,
                # Source observation time precedes durable recording time.
                as_of=complete_at + timedelta(minutes=10),
            )

        assert cut.facts.fill_load_provenance == resumed
        assert cut.revision == 2
        encoded = PositionRecoveryCodec.encode_facts(cut.facts)
        assert PositionRecoveryCodec.decode_facts(encoded) == cut.facts
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_store_rejects_discontinuous_resumed_fill_scan(async_database_url: str):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    key = PositionKey(
        "live", f"provenance-{uuid4().hex}", "BTCUSDT", FuturesPositionSide.BOTH
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trades", stream_epoch=uuid4().hex
    )
    anchor_cut = datetime.now(UTC) - timedelta(minutes=2)
    first = _provenance(
        scope,
        load_id="bootstrap-load",
        anchor_cut=anchor_cut,
        observed_at=anchor_cut + timedelta(seconds=1),
        request_from_id=None,
        next_from_id=100,
        exhausted=False,
    )
    skipped = _provenance(
        scope,
        load_id="bootstrap-load",
        anchor_cut=anchor_cut,
        observed_at=anchor_cut + timedelta(seconds=2),
        request_from_id=101,
        next_from_id=None,
        exhausted=True,
    )
    try:
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session,
                scope=scope,
                facts=AccountFacts(
                    position_key=key,
                    stream_scope=scope,
                    fill_load_provenance=first,
                ),
                revision=1,
            )
        with pytest.raises(JournalFactConflict, match="cursor is discontinuous"):
            async with factory() as session, session.begin():
                await store.persist_facts_in_session(
                    session,
                    scope=scope,
                    facts=AccountFacts(
                        position_key=key,
                        stream_scope=scope,
                        fill_load_provenance=skipped,
                    ),
                    revision=2,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_store_loads_checkpoint_by_exact_old_stream_identity(
    async_database_url: str,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    key = PositionKey(
        "live", f"checkpoint-{uuid4().hex}", "BTCUSDT", FuturesPositionSide.BOTH
    )
    old_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trades", stream_epoch="old-epoch"
    )
    new_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trades", stream_epoch="new-epoch"
    )
    event_at = datetime.now(UTC) - timedelta(minutes=1)
    fill = AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id="opening-trade",
        order_id="opening-order",
        side="BUY",
        quantity=Decimal("2"),
        price=Decimal("10"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=event_at,
        raw_payload={"positionSide": "BOTH"},
    )
    checkpoint = PositionLedger(key).create_recovery_checkpoint(
        AccountFacts(position_key=key, stream_scope=old_scope, fills=(fill,)),
        source_revision=1,
        event_cut=event_at,
    )
    try:
        async with factory() as session, session.begin():
            await store.save_checkpoint_in_session(session, checkpoint)
        async with factory() as session:
            loaded = await store.load_checkpoint_by_id_in_session(
                session,
                scope=old_scope,
                checkpoint_id=checkpoint.checkpoint_id,
            )
            under_new_epoch = await store.load_checkpoint_by_id_in_session(
                session,
                scope=new_scope,
                checkpoint_id=checkpoint.checkpoint_id,
            )

        assert loaded == checkpoint
        assert under_new_epoch is None
    finally:
        await engine.dispose()
