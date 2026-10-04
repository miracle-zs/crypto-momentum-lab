"""Assembly of database engines, sessions, and repositories for live rollout."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.order_adoption_repository import (
    PostgresOrderAdoptionRepository,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_telemetry_repository import (
    PostgresRuntimeTelemetryRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_checkpoint_database_engine,
    create_execution_database_engine,
    create_market_database_engine,
    create_observability_database_engine,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.runtime_session import (
        ResourceOwnershipRegistry,
    )


@dataclass(frozen=True, slots=True)
class LiveDatabaseEngines:
    execution_engine: AsyncEngine
    market_engine: AsyncEngine
    observability_engine: AsyncEngine
    checkpoint_engine: AsyncEngine
    heartbeat_engine: AsyncEngine


@dataclass(frozen=True, slots=True)
class LiveSessionFactories:
    execution_factory: async_sessionmaker[AsyncSession]
    market_factory: async_sessionmaker[AsyncSession]
    observability_factory: async_sessionmaker[AsyncSession]
    checkpoint_factory: async_sessionmaker[AsyncSession]
    heartbeat_factory: async_sessionmaker[AsyncSession]


@dataclass(frozen=True, slots=True)
class LiveRepositories:
    live_repository: PostgresLiveRolloutRepository
    risk_repository: PostgresRiskRepository
    heartbeat_live_repository: PostgresLiveRolloutRepository
    heartbeat_risk_repository: PostgresRiskRepository
    order_adoption_repository: PostgresOrderAdoptionRepository
    order_read_repository: PostgresOrderReadRepository
    order_event_repository: PostgresOrderEventRepository
    submission_repository: PostgresOrderSubmissionRepository
    checkpoint_repository: PostgresPaperDaemonRepository
    telemetry_repository: PostgresRuntimeTelemetryRepository


@dataclass(frozen=True, slots=True)
class LivePersistenceAssembly:
    engines: LiveDatabaseEngines
    factories: LiveSessionFactories
    repositories: LiveRepositories


def assemble_live_persistence(
    *,
    execution_database_url: str,
    market_database_url: str,
    observability_database_url: str,
    account_label: str,
    strategy_name: str,
    ownership_registry: ResourceOwnershipRegistry,
) -> LivePersistenceAssembly:
    """Assemble all live database engines, sessions, and repositories.

    Maintains exact connection pool isolation across execution, market,
    observability, checkpoint, and heartbeat concerns.
    """
    execution_engine = create_execution_database_engine(execution_database_url)
    ownership_registry.register("execution_engine", execution_engine.dispose)

    market_engine = create_market_database_engine(market_database_url)
    ownership_registry.register("market_engine", market_engine.dispose)

    observability_engine = create_observability_database_engine(
        observability_database_url
    )
    ownership_registry.register(
        "observability_engine", observability_engine.dispose
    )

    checkpoint_engine = create_checkpoint_database_engine(
        observability_database_url
    )
    ownership_registry.register("checkpoint_engine", checkpoint_engine.dispose)

    # Lease liveness is a control-plane concern. Give it one isolated
    # connection with a short driver timeout so a slow market/reconcile
    # query cannot consume the pool needed by the heartbeat.
    heartbeat_engine = create_execution_database_engine(
        execution_database_url,
        pool_size=1,
        max_overflow=0,
        pool_timeout_seconds=3,
        command_timeout_seconds=5,
    )
    ownership_registry.register("heartbeat_engine", heartbeat_engine.dispose)

    execution_factory = async_sessionmaker(
        execution_engine,
        expire_on_commit=False,
    )
    market_factory = async_sessionmaker(
        market_engine,
        expire_on_commit=False,
    )
    observability_factory = async_sessionmaker(
        observability_engine,
        expire_on_commit=False,
    )
    checkpoint_factory = async_sessionmaker(
        checkpoint_engine,
        expire_on_commit=False,
    )
    heartbeat_factory = async_sessionmaker(
        heartbeat_engine,
        expire_on_commit=False,
    )

    live_repository = PostgresLiveRolloutRepository(
        execution_factory,
        strategy_scope=("live", account_label, strategy_name),
    )
    risk_repository = PostgresRiskRepository(execution_factory)
    heartbeat_live_repository = PostgresLiveRolloutRepository(heartbeat_factory)
    heartbeat_risk_repository = PostgresRiskRepository(heartbeat_factory)
    order_adoption_repository = PostgresOrderAdoptionRepository(execution_factory)
    order_read_repository = PostgresOrderReadRepository(execution_factory)
    order_event_repository = PostgresOrderEventRepository(execution_factory)
    submission_repository = PostgresOrderSubmissionRepository(execution_factory)
    checkpoint_repository = PostgresPaperDaemonRepository(checkpoint_factory)
    telemetry_repository = PostgresRuntimeTelemetryRepository(
        observability_factory
    )

    engines = LiveDatabaseEngines(
        execution_engine=execution_engine,
        market_engine=market_engine,
        observability_engine=observability_engine,
        checkpoint_engine=checkpoint_engine,
        heartbeat_engine=heartbeat_engine,
    )
    factories = LiveSessionFactories(
        execution_factory=execution_factory,
        market_factory=market_factory,
        observability_factory=observability_factory,
        checkpoint_factory=checkpoint_factory,
        heartbeat_factory=heartbeat_factory,
    )
    repositories = LiveRepositories(
        live_repository=live_repository,
        risk_repository=risk_repository,
        heartbeat_live_repository=heartbeat_live_repository,
        heartbeat_risk_repository=heartbeat_risk_repository,
        order_adoption_repository=order_adoption_repository,
        order_read_repository=order_read_repository,
        order_event_repository=order_event_repository,
        submission_repository=submission_repository,
        checkpoint_repository=checkpoint_repository,
        telemetry_repository=telemetry_repository,
    )
    return LivePersistenceAssembly(
        engines=engines,
        factories=factories,
        repositories=repositories,
    )
