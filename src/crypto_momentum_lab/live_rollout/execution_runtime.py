from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.decision.decision_engine import EffectivePolicy
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeBoundaryCallback,
    OrderExchangeClient,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionRepository,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationRegistry,
)
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.domain.strategy.sizing import FixedNotionalSizingModel
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderEventCallback,
    OrderEventRepository,
    OrderExecutionStateMachine,
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
class LiveExecutionRuntime:
    book: ExecutionBook
    coordinator: OrderExecutionCoordinator


async def build_live_execution_runtime(
    *,
    sessions: async_sessionmaker[AsyncSession],
    exchange: OrderExchangeClient,
    event_repository: OrderEventRepository,
    account_label: str,
    strategy_name: str,
    on_event: OrderEventCallback,
    on_before_submit: OrderPreSubmissionCallback,
    on_exchange_request: ExchangeBoundaryCallback,
    on_exchange_response: ExchangeBoundaryCallback,
    submission_repository: OrderSubmissionRepository,
    submission_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> LiveExecutionRuntime:
    backend = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=event_repository,
        live_submit_enabled=True,
        clock=lambda: datetime.now(tz=UTC),
        on_event=on_event,
        on_before_submit=on_before_submit,
        on_exchange_request=on_exchange_request,
        on_exchange_response=on_exchange_response,
    )
    reservations = AsyncPostgresPositionReservationRepository(
        sessions,
        strategy_name=strategy_name,
    )
    reservation_registry = ReservationRegistry()
    commands = PostgresCommandRepository(sessions)
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        sessions,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    book = ExecutionBook(
        coordinator=reservation_registry,
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
        execution_book=book,
        submission_repository=submission_repository,
        submission_clock=submission_clock,
    )
    return LiveExecutionRuntime(book=book, coordinator=coordinator)


def build_live_policy(
    *,
    config: LiveRuntimeConfig,
    target_notional: Decimal,
    risk_config: RiskConfigSnapshot,
    strategy_name: str,
) -> EffectivePolicy:
    """Build the policy consumed by live decisions, without deployment metadata."""
    if target_notional <= 0:
        raise ValueError("target_notional must be positive")
    order_type = config.strategy.entry_order_type
    # Preserve the durable policy identity used by existing decision records.
    identity = hashlib.sha256(
        json.dumps(
            {
                "strategy_name": strategy_name,
                "entry_threshold": None,
                "order_type": order_type.value,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()[:16]
    return EffectivePolicy(
        policy_id=identity,
        strategy_name=strategy_name,
        policy_version=1,
        entry_threshold=None,
        order_type=order_type,
        target_notional=target_notional,
        max_open_positions=risk_config.max_open_positions,
        max_concurrency_per_symbol=config.execution.max_concurrency_per_symbol,
        sizing_model=FixedNotionalSizingModel(
            target_notional=target_notional,
            max_leverage=Decimal("5.0"),
            max_slippage_budget_bps=Decimal("10.0"),
            resize_tolerance=Decimal("0.05"),
        ),
        # Closed candles and grace exits have their own live owner.
        exit_policy=PositionExitPolicy(
            max_holding_seconds=None,
            mode=PositionExitMode.CANDLE_15M,
        ),
    )
