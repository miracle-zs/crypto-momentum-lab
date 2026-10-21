from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import (
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.live_rollout.runtime_manifest import LiveRuntimeAccount
from crypto_momentum_lab.next_gen.dashboard import NextGenDashboardQueries
from crypto_momentum_lab.operator_dashboard import (
    overview_queries as _overview_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    telemetry_queries as _telemetry_queries,
)
from crypto_momentum_lab.operator_dashboard.collector_status import (
    DEFAULT_RESEARCH_COLLECTOR_ROOT,
)
from crypto_momentum_lab.operator_dashboard.performance_queries import (
    PerformanceQueries,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    DecisionSLOResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    ResearchCollectorResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    SystemOverviewResponse,
    SystemPerformanceResponse,
    SystemReadinessResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
)
from crypto_momentum_lab.persistence.postgres.models import (
    LiveSessionTransitionRow,
)

DecisionSLOQueries = _telemetry_queries.DecisionSLOQueries
class DashboardQueries:
    """Stable dashboard read facade over cohesive query families.

    The HTTP app depends on this small, testable surface; specialized query
    objects remain internal so endpoint wiring does not couple to storage
    details. Methods with their own assembly logic stay here deliberately.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        stale_after_seconds: float = 120.0,
        research_collector_root: Path = DEFAULT_RESEARCH_COLLECTOR_ROOT,
        nextgen_accounts: tuple[LiveRuntimeAccount, ...],
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._stale_after_seconds = stale_after_seconds
        self._research_collector_root = research_collector_root
        self._telemetry_queries = DecisionSLOQueries(
            session_factory,
            clock=self._clock,
        )
        self._overview_queries = _overview_queries.OverviewQueries(
            session_factory,
            clock=self._clock,
            stale_after_seconds=self._stale_after_seconds,
            research_collector_root=self._research_collector_root,
        )
        self._performance_queries = PerformanceQueries(
            session_factory,
            clock=self._clock,
            decision_slo_queries=self._telemetry_queries,
        )

        self._nextgen_queries = NextGenDashboardQueries(
            session_factory,
            nextgen_accounts,
            clock=self._clock,
        )

    async def health(self) -> dict[str, str]:
        return await self._overview_queries.health()

    async def operational_health(self) -> dict[str, Any]:
        readiness = await self.readiness()
        ready = readiness.status is OperationalStatus.READY
        return {
            "scope": "nextgen",
            "overall_status": "healthy" if ready else "degraded",
            "is_execution_ready": ready,
            "source_as_of": readiness.observed_at.isoformat(),
            "evaluated_at": readiness.observed_at.isoformat(),
            "dimensions": [],
            "details": {
                "accounts": {
                    account.account_label: account.readiness
                    for account in readiness.accounts
                },
            },
            "account_capabilities": {
                account.account_label: (
                    None
                    if account.runtime_tradeability is None
                    else account.runtime_tradeability.model_dump()
                )
                for account in readiness.accounts
            },
        }

    async def readiness(self) -> SystemReadinessResponse:
        return await self._nextgen_queries.readiness(await self.health())

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse:
        return await self._telemetry_queries.decision_slo(window)

    async def performance(
        self,
        window: str = "24h",
    ) -> SystemPerformanceResponse:
        result = await self._performance_queries.performance(window)
        result.execution_engine = "nextgen"
        market = await self._nextgen_queries.market_performance()
        market.missing_agg_trade_count = result.market_data.missing_agg_trade_count
        market.quality_events_count_1h = result.market_data.quality_events_count_1h
        result.market_data = market
        return result

    async def research_collector(self) -> ResearchCollectorResponse:
        return await self._overview_queries.research_collector()

    async def live_accounts(self) -> LiveAccountsResponse:
        return await self._nextgen_queries.live_accounts()

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse:
        return await self._nextgen_queries.live_account_metrics(equity_range)

    async def overview(self) -> SystemOverviewResponse:
        overview = await self._overview_queries.overview()
        fleet = await self._nextgen_queries.live_accounts()
        overview.account_statuses = fleet.accounts
        overview.active_halt_count = await self._nextgen_queries.active_halt_count()
        observed = [a.runtime_observed_at for a in fleet.accounts]
        runner = _overview_queries.service(
            "strategy-runner",
            self._clock(),
            min(observed) if observed and all(observed) else None,
            180,
        )
        runner.details = {
            "engine": "nextgen",
            "status": fleet.status.value,
            "started_at": (
                max(a.runtime_started_at for a in fleet.accounts).isoformat()
                if fleet.accounts and all(a.runtime_started_at for a in fleet.accounts)
                else None
            ),
            "accounts": {a.account_label: a.readiness for a in fleet.accounts},
            "entry_gate_open": all(
                a.runtime_tradeability and a.runtime_tradeability.entry_gate_open
                for a in fleet.accounts
            ),
            "entry_gate_reasons": sorted(
                {
                    a.runtime_tradeability.entry_gate_reason
                    for a in fleet.accounts
                    if a.runtime_tradeability
                    and not a.runtime_tradeability.entry_gate_open
                }
            ),
        }
        overview.services.insert(1, runner)
        return overview

    async def universe(self) -> UniverseStatusResponse:
        return await self._overview_queries.universe()

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
        environment: str | None = None,
    ) -> AccountOverviewResponse:
        return await self._nextgen_queries.account(
            equity_range=equity_range,
            account_label=account_label,
            environment=environment,
        )

    async def risk_execution(self) -> RiskExecutionResponse:
        return await self._nextgen_queries.risk_execution()

    async def reports(self) -> RunReportSummaryResponse:
        async with self._session_factory() as session:
            live = (
                await session.scalars(
                    select(LiveSessionTransitionRow)
                    .order_by(LiveSessionTransitionRow.occurred_at.desc())
                    .limit(10)
                )
            ).all()
        return RunReportSummaryResponse(
            status=OperationalStatus.READY if live else OperationalStatus.NO_DATA,
            live_sessions=[
                {
                    "session_id": row.session_id,
                    "state": row.state,
                    "occurred_at": row.occurred_at.isoformat(),
                }
                for row in live
            ],
        )
