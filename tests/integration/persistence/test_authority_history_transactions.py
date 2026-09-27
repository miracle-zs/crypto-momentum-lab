"""Independent real-database acceptance of nonzero fact recovery."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _facts():
    key = PositionKey(
        environment="live",
        account_label=f"test-history-{uuid4().hex}",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trade-source", stream_epoch="one"
    )
    start = datetime.now(UTC) - timedelta(minutes=1)

    def fill(identity, side, quantity, at):
        return AccountFillEvent(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            trade_id=identity,
            order_id=identity,
            side=side,
            price=Decimal("100"),
            quantity=Decimal(quantity),
            realized_pnl=Decimal(0),
            fee=Decimal(0),
            fee_asset="USDT",
            trade_at=at,
            raw_payload={"positionSide": "BOTH", "is_system": True},
        )

    prefix = AccountFacts(
        position_key=key, stream_scope=scope, fills=(fill("open", "BUY", "10", start),)
    )
    suffix = fill("reduce", "SELL", "3", start + timedelta(seconds=1))
    return scope, prefix, suffix


@pytest.mark.asyncio
async def test_store_restores_nonzero_checkpoint_and_suffix(async_database_url: str):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    scope, prefix, reducing = _facts()
    ledger = PositionLedger(prefix.position_key)
    checkpoint = ledger.create_recovery_checkpoint(prefix, source_revision=1)
    try:
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session, scope=scope, facts=prefix, revision=1
            )
            await store.save_checkpoint_in_session(session, checkpoint)
        suffix = AccountFacts(
            position_key=prefix.position_key,
            stream_scope=scope,
            fills=(reducing,),
            recovery_checkpoint=checkpoint,
            prefix_facts_complete=False,
        )
        async with factory() as session, session.begin():
            await store.persist_facts_in_session(
                session, scope=scope, facts=suffix, revision=2
            )
        async with factory() as session:
            cut = await store.load_recovery_in_session(
                session, scope=scope, as_of=datetime.now(UTC)
            )
        assert cut.checkpoint is not None
        assert cut.revision == 2
        projection = ledger.project(cut.facts)
        full = ledger.project(
            AccountFacts(
                position_key=prefix.position_key,
                stream_scope=scope,
                fills=(*prefix.fills, reducing),
            )
        )
        assert projection.active_episode == full.active_episode
        assert projection.total_active_quantity == Decimal("7")
        # Roll forward from the persisted seed rather than reloading its prefix.
        next_checkpoint = ledger.create_recovery_checkpoint(
            cut.facts, source_revision=cut.revision, event_cut=reducing.trade_at
        )
        async with factory() as session, session.begin():
            await store.save_checkpoint_in_session(session, next_checkpoint)
        async with factory() as session:
            second_cut = await store.load_recovery_in_session(
                session, scope=scope, as_of=datetime.now(UTC)
            )
        assert second_cut.checkpoint.checkpoint_id == next_checkpoint.checkpoint_id
        assert ledger.project(second_cut.facts).active_episode == full.active_episode
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_store_never_commits_callers_failed_transaction(async_database_url: str):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    scope, facts, _ = _facts()
    try:
        with pytest.raises(RuntimeError, match="injected"):
            async with factory() as session, session.begin():
                await store.persist_facts_in_session(
                    session, scope=scope, facts=facts, revision=1
                )
                raise RuntimeError("injected after journal write")
        async with factory() as session:
            row = await session.scalar(
                select(PositionFactJournalEventRow).where(
                    PositionFactJournalEventRow.account_label == scope.account_label,
                )
            )
        assert row is None
    finally:
        await engine.dispose()
