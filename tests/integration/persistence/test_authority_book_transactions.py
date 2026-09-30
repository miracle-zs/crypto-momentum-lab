"""Cross-module acceptance of staged Book facts and PostgreSQL rollback."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    Applied,
    Duplicate,
    ExecutionBook,
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


def _book(factory, uow_type=AsyncPostgresExecutionUnitOfWork):
    orders = PostgresOrderRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="authority-test"
    )
    uow = uow_type(
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


def _evidence(account, identity, quantity="1"):
    now = datetime.now(UTC)
    scope = ExecutionScope(environment="live", account_label=account, symbol="BTCUSDT")
    fill = AccountFillEvent(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        trade_id=identity,
        order_id=identity,
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal(quantity),
        realized_pnl=Decimal(0),
        fee=Decimal(0),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"positionSide": "BOTH", "is_system": True},
    )
    return ExecutionEvidence(
        evidence_id=identity,
        scope=scope,
        observed_at=now,
        fill=fill,
        stream_id="trades",
        stream_epoch="one",
        sequence=int(identity),
    )


@pytest.mark.asyncio
async def test_book_nonzero_restore_preserves_token_and_evidence_identity(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-book-{uuid4().hex}"
    evidence = _evidence(account, "1")
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        assert isinstance(await book.observe(evidence), Applied)
        historical_cut = datetime.now(UTC)
        assert isinstance(await book.observe(_evidence(account, "2")), Applied)
        original = await book.read(evidence.scope)
        restored = _book(factory)
        await restored.restore(account_label=account)
        view = await restored.read(evidence.scope)
        assert view.total_quantity == Decimal("2")
        assert view.projection_version == original.projection_version
        historical = await restored.read(evidence.scope, event_cut=historical_cut)
        assert historical.total_quantity == Decimal("1")
        assert historical.projection_version != view.projection_version
        assert isinstance(await restored.observe(evidence), Duplicate)
        assert (await restored.read(evidence.scope)).total_quantity == Decimal("2")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_book_sql_failure_never_publishes_candidate(async_database_url):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-book-fail-{uuid4().hex}"

    class FailAfterWrites(AsyncPostgresExecutionUnitOfWork):
        @asynccontextmanager
        async def transaction(self, key):
            async with super().transaction(key) as transaction:
                yield transaction
                raise RuntimeError("injected before PostgreSQL commit")

    try:
        first = _evidence(account, "1")
        book = _book(factory)
        await book.restore(account_label=account)
        assert isinstance(await book.observe(first), Applied)
        failed_book = _book(factory, FailAfterWrites)
        await failed_book.restore(account_label=account)
        published = failed_book._books[first.scope.to_position_key().canonical_id]
        original = published.get_view()
        with pytest.raises(RuntimeError, match="injected"):
            await failed_book.observe(_evidence(account, "2"))
        assert published.get_view().total_quantity == original.total_quantity
        restored = _book(factory)
        await restored.restore(account_label=account)
        assert (
            await restored.read(first.scope)
        ).projection_version == original.projection_version
        assert (await restored.read(first.scope)).total_quantity == Decimal("1")
    finally:
        await engine.dispose()
