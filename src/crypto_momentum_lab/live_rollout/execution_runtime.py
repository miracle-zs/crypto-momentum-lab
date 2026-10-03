from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.command_repository import CommandRepository
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeBoundaryCallback,
    OrderExchangeClient,
    OrderExchangeSubmitGuard,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    FinalSubmissionAdmission,
    OrderSubmissionRepository,
)
from crypto_momentum_lab.domain.execution.ports import ExecutionUnitOfWorkPort
from crypto_momentum_lab.domain.execution.reservation_repository import (
    ReservationRepository,
)
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, TradingLease
from crypto_momentum_lab.domain.runtime import (
    CapabilityEvidence,
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
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
)
from crypto_momentum_lab.live_rollout.gates import order_state_is_uncertain
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.runtime_config import LiveRuntimeConfig


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
    order_repository: OrderPlanRepository,
    event_repository: OrderEventRepository,
    account_label: str,
    strategy_name: str,
    callbacks: LiveExecutionCallbacks,
    submission_repository: OrderSubmissionRepository | None = None,
    submission_admission: FinalSubmissionAdmission | None = None,
    submission_clock: Callable[[], datetime] | None = None,
) -> LiveExecutionRuntime:
    backend = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_repository,
        event_repository=event_repository,
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
    unit_of_work: ExecutionUnitOfWorkPort = AsyncPostgresExecutionUnitOfWork(
        sessions,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    book_reservations: ReservationRepository = reservations
    book_commands: CommandRepository = commands
    book = ExecutionBook(
        coordinator=domain_coordinator,
        reservation_repository=book_reservations,
        command_repository=book_commands,
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
        submission_admission=submission_admission,
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
    git_commit_hash: str,
    migration_revision: str,
    active_lease: TradingLease | None,
) -> RuntimePlan:
    """Compile deterministic live runtime plan with safety overrides."""
    plan_overrides = {
        "target_notional": target_notional,
        "order_type": (
            "market"
            if getattr(config.strategy, "market_orders", False)
            else "limit"
        ),
        "max_open_positions": risk_config.max_open_positions,
        "max_account_drawdown": getattr(
            risk_config, "max_account_drawdown", "0.10"
        ),
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
        git_commit=git_commit_hash,
        schema_version=migration_revision,
        runtime_generation=git_commit_hash,
        fencing_epoch=int(getattr(active_lease, "fencing_token", 1) or 1),
        observed_database_revision=migration_revision,
        overrides=plan_overrides,
        strict=True,
    )


def build_capability_evidence_provider(
    *,
    account_label: str,
    runtime_plan: RuntimePlan,
    get_active_lease: Callable[[], TradingLease | None],
    is_entry_enabled: Callable[[], bool],
    get_context: Callable[[], object | None],
    get_market_age: Callable[[], float | None],
    has_api_key: Callable[[], bool],
    get_approval: Callable[[], LiveOperatorApproval | None] | None = None,
) -> Callable[[OrderExecutionPlan, datetime], CapabilityEvidence]:
    """Build a closure that evaluates real-time system capability evidence."""

    def _provide_capability_evidence(
        order_plan: OrderExecutionPlan,
        checked_at: datetime,
    ) -> CapabilityEvidence:
        market_age_val = get_market_age()
        market_age = (
            max(0.0, float(market_age_val))
            if market_age_val is not None
            else float("inf")
        )

        ctx = get_context()
        if ctx is None:
            is_concordant = False
            unresolved_count = 1
        else:
            is_concordant = True
            order_sym = getattr(order_plan, "symbol", "")
            unmanaged = getattr(ctx, "unmanaged_position_symbols", ())
            if isinstance(unmanaged, (set, frozenset, tuple, list)) and order_sym in unmanaged:
                is_concordant = False
            pending = getattr(ctx, "pending_position_symbols", ())
            if isinstance(pending, (set, frozenset, tuple, list)) and order_sym in pending:
                is_concordant = False
            unresolved = getattr(ctx, "unresolved_orders", ())
            if not isinstance(unresolved, (set, frozenset, tuple, list)):
                unresolved = ()
            curr_cid = getattr(order_plan, "client_order_id", None)
            unresolved_count = sum(
                1
                for o in unresolved
                if getattr(getattr(o, "plan", None), "symbol", None) == order_sym
                and getattr(getattr(o, "plan", None), "client_order_id", None)
                != curr_cid
                and order_state_is_uncertain(getattr(o, "state", None))
            )

        now_utc = (
            checked_at if checked_at.tzinfo else checked_at.replace(tzinfo=UTC)
        )
        if get_approval is not None:
            approval = get_approval()
            if approval is not None:
                is_app_valid = (
                    approval.approval_text == LIVE_APPROVAL_CONFIRMATION
                    and (approval.expires_at is None or approval.expires_at > now_utc)
                    and (approval.account_label == account_label)
                    and approval.strategy_name == runtime_plan.effective_policy.strategy_name
                )
            else:
                is_app_valid = False
        elif is_entry_enabled is not None:
            is_app_valid = is_entry_enabled()
        else:
            is_app_valid = False

        lease = get_active_lease()
        lease_exp = getattr(lease, "expires_at", None)
        is_lease_valid = (
            (
                lease_exp is not None
                and (
                    lease_exp
                    if lease_exp.tzinfo
                    else lease_exp.replace(tzinfo=UTC)
                )
                > now_utc
            )
            if lease
            else False
        )
        is_identity_ok = bool(account_label and has_api_key())

        return CapabilityEvidence(
            evidence_version=f"ev_{account_label}_{checked_at.isoformat()}",
            market_freshness_seconds=market_age,
            is_account_concordant=is_concordant,
            is_account_identity_verified=is_identity_ok,
            unresolved_inflight_orders_count=unresolved_count,
            is_approval_valid=is_app_valid,
            is_lease_active=is_lease_valid,
            is_emergency_authorized=False,
            is_universe_ready=is_app_valid,
            is_collector_healthy=True,
            plan_hash=runtime_plan.plan_hash,
            runtime_generation=runtime_plan.runtime_generation,
            fencing_epoch=runtime_plan.fencing_epoch,
            declared_schema_compatibility=runtime_plan.declared_schema_compatibility,
            observed_database_revision=runtime_plan.observed_database_revision,
            observed_at=checked_at,
        )

    return _provide_capability_evidence
