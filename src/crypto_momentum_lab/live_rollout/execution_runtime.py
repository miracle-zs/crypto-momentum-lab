from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeBoundaryCallback,
    OrderExchangeClient,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionRepository,
)
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot
from crypto_momentum_lab.domain.runtime import (
    RuntimePlan,
    RuntimePlanCompiler,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderEventCallback,
    OrderEventRepository,
    OrderExecutionStateMachine,
    OrderPlanRepository,
    OrderPreSubmissionCallback,
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

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.runtime_config import LiveRuntimeConfig


@dataclass(frozen=True, slots=True)
class LiveExecutionCallbacks:
    on_event: OrderEventCallback
    on_before_submit: OrderPreSubmissionCallback
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
    order_repository: OrderPlanRepository,
    event_repository: OrderEventRepository,
    account_label: str,
    strategy_name: str,
    callbacks: LiveExecutionCallbacks,
    submission_repository: OrderSubmissionRepository | None = None,
    submission_clock: Callable[[], datetime] | None = None,
) -> LiveExecutionRuntime:
    backend = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_repository,
        event_repository=event_repository,
        live_submit_enabled=True,
        clock=lambda: datetime.now(tz=UTC),
        on_event=callbacks.on_event,
        on_before_submit=callbacks.on_before_submit,
        on_exchange_request=callbacks.on_exchange_request,
        on_exchange_response=callbacks.on_exchange_response,
        serialize_commands=False,
    )
    reservations = AsyncPostgresPositionReservationRepository(
        sessions,
        strategy_name=strategy_name,
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
        submission_repository=submission_repository,
        submission_clock=submission_clock,
    )
    return LiveExecutionRuntime(book=book, coordinator=coordinator)


def compile_live_runtime_plan(
    *,
    config: LiveRuntimeConfig,
    target_notional: Decimal,
    risk_config: RiskConfigSnapshot,
    account_label: str,
    strategy_name: str,
) -> RuntimePlan:
    """Compile the live trading policy from strategy and risk settings."""
    plan_overrides = {
        "target_notional": target_notional,
        "order_type": (
            "market" if getattr(config.strategy, "market_orders", False) else "limit"
        ),
        "max_open_positions": risk_config.max_open_positions,
        "max_account_drawdown": getattr(risk_config, "max_account_drawdown", "0.10"),
        "max_gross_notional": risk_config.max_gross_notional,
        "max_order_notional": getattr(
            risk_config, "max_order_notional", target_notional
        ),
        # Live exits belong to LiveExitManager (closed candle + grace).
        # Never inject a second, time-based exit through the entry policy.
        "max_holding_seconds": None,
    }
    return RuntimePlanCompiler.compile(
        environment="live",
        account_label=account_label,
        strategy_name=strategy_name,
        overrides=plan_overrides,
        strict=True,
    )
