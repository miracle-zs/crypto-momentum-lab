"""Restore durable execution authority before exposing live submission.

The caller owns the exchange client and database sessions, and registers the
returned coordinator with its shutdown lifecycle.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeBoundaryCallback,
    OrderEventCallback,
    OrderExchangeClient,
    OrderExchangeSubmitGuard,
    OrderExecutionStateMachine,
    OrderPreSubmissionCallback,
    OrderStateRepository,
    SubmitPolicy,
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


@dataclass(frozen=True, slots=True)
class LiveExecutionCallbacks:
    on_event: OrderEventCallback
    on_before_submit: OrderPreSubmissionCallback
    on_before_exchange_submit: OrderExchangeSubmitGuard
    on_exchange_request: ExchangeBoundaryCallback
    on_exchange_response: ExchangeBoundaryCallback


@dataclass(frozen=True, slots=True)
class LiveExecutionRuntime:
    book: ExecutionBook
    coordinator: OrderExecutionCoordinator


async def build_live_execution_runtime(
    *,
    sessions: async_sessionmaker[AsyncSession],
    exchange: OrderExchangeClient,
    order_repository: OrderStateRepository,
    account_label: str,
    strategy_name: str,
    callbacks: LiveExecutionCallbacks,
) -> LiveExecutionRuntime:
    backend = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: datetime.now(tz=UTC),
        on_event=callbacks.on_event,
        on_before_submit=callbacks.on_before_submit,
        on_before_exchange_submit=callbacks.on_before_exchange_submit,
        on_exchange_request=callbacks.on_exchange_request,
        on_exchange_response=callbacks.on_exchange_response,
        serialize_commands=False,
    )
    reservations = AsyncPostgresPositionReservationRepository(
        sessions, strategy_name=strategy_name,
    )
    domain_coordinator = ExecutionCoordinator()
    commands = PostgresCommandRepository(sessions)
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        sessions,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    book = ExecutionBook(
        coordinator=domain_coordinator,
        reservation_repository=reservations,
        command_repository=commands,
        execution_unit_of_work=unit_of_work,
    )
    # Recovery failure must prevent exposing a submission coordinator.
    await book.restore(account_label=account_label)
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label=account_label,
        environment="live",
        reservation_repository=reservations,
        domain_coordinator=domain_coordinator,
        execution_book=book,
    )
    return LiveExecutionRuntime(book=book, coordinator=coordinator)
