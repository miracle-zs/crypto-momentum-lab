"""Position repair adapter reusing the normal execution transaction and CAS."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.evidence_codec import _trade_payload_digest
from crypto_momentum_lab.domain.execution.ports import ExecutionTradeIdentity
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepair,
    PositionRepairFacts,
    PositionRepairReceipt,
    PositionRepairRequest,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
    _fill_from_row,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderRow,
)
from crypto_momentum_lab.persistence.postgres.position_repair_ports import (
    PositionRepairExecutionTransaction,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)


class PostgresPositionRepairTransaction:
    def __init__(self, transaction: PositionRepairExecutionTransaction) -> None:
        self._transaction = transaction

    async def load_repair_facts(
        self, request: PositionRepairRequest
    ) -> PositionRepairFacts:
        tx = self._transaction
        session = tx.session
        key = request.key
        # Normal execution's advisory lock is already held before these reads.
        head = await tx.load_head(key)
        owned_order_ids = frozenset(
            (
                await session.scalars(
                    select(ExchangeOrderRow.exchange_order_id).where(
                        ExchangeOrderRow.run_id == request.run_id,
                        ExchangeOrderRow.symbol == key.symbol,
                        ExchangeOrderRow.position_side == key.position_side.value,
                        ExchangeOrderRow.reduce_only.is_(False),
                        ExchangeOrderRow.exchange_order_id.is_not(None),
                    )
                )
            ).all()
        )
        rows = (
            await session.scalars(
                select(AccountFillEventRow)
                .where(
                    AccountFillEventRow.environment == key.environment,
                    AccountFillEventRow.account_label == key.account_label,
                    AccountFillEventRow.symbol == key.symbol,
                )
                .order_by(AccountFillEventRow.trade_at.asc())
            )
        ).all()
        fills = tuple(_fill_from_row(row) for row in rows)
        # Do not borrow fills from the opposite hedge side; missing side cannot
        # prove ownership and the domain journal remains fail-closed.
        fills = tuple(
            fill
            for fill in fills
            if fill.raw_position_side is not None
            and str(fill.raw_position_side).upper() == key.position_side.value
        )
        cut = await tx.load_recovery(scope=request.scope, as_of=datetime.now(UTC))
        return PositionRepairFacts(cut, head, fills, owned_order_ids)

    async def persist_repair(self, repair: PositionRepair) -> PositionRepairReceipt:
        tx = self._transaction
        request = repair.request
        if not repair.needs_write:
            return PositionRepairReceipt(
                request.scope,
                repair.expected_head_revision,
                repair.projection_version,
                False,
            )
        for fill in repair.facts.fills:
            await tx.record_trade(
                key=request.key,
                stream_id=request.scope.stream_id,
                stream_epoch=request.scope.stream_epoch,
                trade=ExecutionTradeIdentity(
                    trade_id=fill.trade_id,
                    order_id=fill.order_id,
                    quantity=fill.quantity,
                    price=fill.price,
                    side=fill.side,
                    payload_digest=_trade_payload_digest(fill),
                    first_seen_at=fill.trade_at,
                ),
            )
        await tx.persist_facts(
            scope=request.scope,
            facts=repair.facts,
            revision=repair.revision,
            delta=repair.delta,
        )
        revision = await tx.persist_head(
            key=request.key,
            stream_id=request.scope.stream_id,
            stream_epoch=request.scope.stream_epoch,
            expected_revision=repair.expected_head_revision,
            projection_version=repair.projection_version,
            state_payload=repair.head_payload,
            updated_at=datetime.now(UTC),
        )
        return PositionRepairReceipt(
            request.scope, revision, repair.projection_version, True
        )


class PostgresPositionRepairUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._execution = AsyncPostgresExecutionUnitOfWork(
            sessions,
            journal_store=PostgresAccountJournalStore(),
            command_repository=PostgresCommandRepository(sessions),
            reservation_repository=AsyncPostgresPositionReservationRepository(sessions),
        )

    @asynccontextmanager
    async def transaction(
        self, key: PositionKey
    ) -> AsyncIterator[PostgresPositionRepairTransaction]:
        async with self._execution.transaction(key) as tx:
            yield PostgresPositionRepairTransaction(tx)
