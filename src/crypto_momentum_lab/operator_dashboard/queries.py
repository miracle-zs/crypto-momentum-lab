import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import (
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.operator_dashboard import (
    account_queries as _account_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    common_equity as _common_equity,
)
from crypto_momentum_lab.operator_dashboard import (
    live_account_metrics_queries as _live_account_metrics_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    overview_queries as _overview_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    paper_account_queries as _paper_account_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    paper_equity_queries as _paper_equity_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    risk_execution_queries as _risk_execution_queries,
)
from crypto_momentum_lab.operator_dashboard import (
    telemetry_queries as _telemetry_queries,
)
from crypto_momentum_lab.operator_dashboard.collector_status import (
    DEFAULT_RESEARCH_COLLECTOR_ROOT,
)
from crypto_momentum_lab.operator_dashboard.performance_builder import (
    build_performance_summary_dict,
)
from crypto_momentum_lab.operator_dashboard.performance_queries import (
    PerformanceQueries,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    DecisionSLOResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    ResearchCollectorResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    SystemPerformanceResponse,
    SystemReadinessResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    CashFlowCorrectionRow,
    LiveSessionTransitionRow,
)

_EQUITY_WINDOW = timedelta(hours=24)
_EQUITY_BUCKET_SECONDS = 6 * 60
_EQUITY_MAX_POINTS = 240
_LIVE_SIGNAL_MAX_ROWS = 30
_PAPER_HISTORY_RECENT_LIMIT = 500
_COMMON_EQUITY_BUCKET_SECONDS = 15 * 60
FIXED_COMMON_EQUITY_START_AT = datetime(2026, 8, 21, 2, 45, tzinfo=UTC)
DecisionSLOQueries = _telemetry_queries.DecisionSLOQueries
RiskExecutionQueries = _risk_execution_queries.RiskExecutionQueries
LiveCashFlowAdjustment = _common_equity.LiveCashFlowAdjustment
_as_utc = _common_equity.as_utc
LiveAccountMetricsQueries = _live_account_metrics_queries.LiveAccountMetricsQueries
PaperAccountQueries = _paper_account_queries.PaperAccountQueries
_paper_exit_details = _paper_account_queries._paper_exit_details
PaperEquityQueries = _paper_equity_queries.PaperEquityQueries
LiveAccountQueries = _account_queries.LiveAccountQueries


DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS: tuple[LiveCashFlowAdjustment, ...] = ()


def parse_live_cash_flow_adjustments(
    value: str | None = None,
) -> tuple[LiveCashFlowAdjustment, ...]:
    """Parse dashboard-only cash-flow corrections without exposing credentials."""
    raw_value = (
        os.environ.get("CML_DASHBOARD_LIVE_CASH_FLOWS_JSON", "")
        if value is None
        else value
    )
    if not raw_value.strip():
        return DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(
            "CML_DASHBOARD_LIVE_CASH_FLOWS_JSON must be valid JSON"
        ) from error
    if not isinstance(payload, list):
        raise ValueError("CML_DASHBOARD_LIVE_CASH_FLOWS_JSON must be a JSON list")

    adjustments: list[LiveCashFlowAdjustment] = []
    for index, item in enumerate(payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"cash-flow entry {index} must be a JSON object")
        account_label = item.get("account_label")
        effective_at = item.get("effective_at")
        amount = item.get("amount")
        cash_flow_type = item.get("cash_flow_type", "deposit")
        if not isinstance(account_label, str) or not account_label.strip():
            raise ValueError(f"cash-flow entry {index} has no account_label")
        if not isinstance(effective_at, str) or not effective_at.strip():
            raise ValueError(f"cash-flow entry {index} has no effective_at")
        if not isinstance(cash_flow_type, str) or not cash_flow_type.strip():
            raise ValueError(f"cash-flow entry {index} has no cash_flow_type")
        try:
            parsed_at = datetime.fromisoformat(
                effective_at.strip().replace("Z", "+00:00")
            )
            if parsed_at.tzinfo is None:
                raise ValueError("effective_at must include a timezone")
            parsed_amount = Decimal(str(amount))
        except (TypeError, ValueError, ArithmeticError) as error:
            raise ValueError(f"invalid cash-flow entry {index}") from error
        if not parsed_amount.is_finite():
            raise ValueError(f"cash-flow entry {index} amount must be finite")
        adjustments.append(
            LiveCashFlowAdjustment(
                account_label=account_label.strip(),
                effective_at=parsed_at.astimezone(UTC),
                amount=parsed_amount,
                cash_flow_type=cash_flow_type.strip(),
            )
        )
    return tuple(sorted(adjustments, key=lambda item: item.effective_at))


def parse_common_equity_start_at(value: str | None = None) -> datetime:
    """Return the production comparison origin, which is intentionally fixed."""
    raw_value = (
        os.environ.get("CML_DASHBOARD_COMMON_EQUITY_START_AT", "")
        if value is None
        else value
    )
    if not raw_value.strip():
        return FIXED_COMMON_EQUITY_START_AT
    try:
        parsed_at = datetime.fromisoformat(raw_value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT must be an ISO-8601 timestamp"
        ) from error
    if parsed_at.tzinfo is None:
        raise ValueError("CML_DASHBOARD_COMMON_EQUITY_START_AT must include a timezone")
    parsed_at = parsed_at.astimezone(UTC)
    if parsed_at != FIXED_COMMON_EQUITY_START_AT:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT is fixed at 2026-08-21T02:45:00Z"
        )
    return FIXED_COMMON_EQUITY_START_AT


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
        paper_run_ids: frozenset[str] | None = None,
        live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment] | None = None,
        common_equity_start_at: datetime | None = None,
        research_collector_root: Path = DEFAULT_RESEARCH_COLLECTOR_ROOT,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._stale_after_seconds = stale_after_seconds
        self._paper_run_ids = paper_run_ids
        self._live_cash_flow_adjustments = tuple(
            DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS
            if live_cash_flow_adjustments is None
            else live_cash_flow_adjustments
        )
        self._common_equity_start_at = (
            FIXED_COMMON_EQUITY_START_AT
            if common_equity_start_at is None
            else _as_utc(common_equity_start_at)
        )
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
        self._risk_execution_queries = RiskExecutionQueries(
            session_factory,
            environment="live",
            market_environment=os.environ.get("CML_MARKET_ENVIRONMENT", "research"),
        )
        self._live_account_metrics_queries = LiveAccountMetricsQueries(
            session_factory,
            clock=self._clock,
        )
        self._paper_account_queries = PaperAccountQueries(
            session_factory,
            clock=self._clock,
            stale_after_seconds=self._stale_after_seconds,
            paper_run_ids=self._paper_run_ids,
        )
        self._paper_equity_queries = PaperEquityQueries(
            session_factory,
            clock=self._clock,
            select_paper_runs=self._paper_account_queries.selected_paper_runs,
            paper_exit_details=_paper_exit_details,
            live_cash_flow_adjustments=self._live_cash_flow_adjustments,
            common_equity_start_at=self._common_equity_start_at,
        )
        self._account_queries = LiveAccountQueries(
            session_factory,
            clock=self._clock,
        )
        self._performance_queries = PerformanceQueries(
            session_factory,
            clock=self._clock,
            decision_slo_queries=self._telemetry_queries,
        )

    async def health(self) -> dict[str, str]:
        return await self._overview_queries.health()

    async def operational_health(self) -> dict[str, Any]:
        return await self._overview_queries.operational_health()

    async def readiness(self) -> SystemReadinessResponse:

        return await self._overview_queries.readiness()

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse:
        return await self._telemetry_queries.decision_slo(window)

    async def performance(
        self,
        window: str = "24h",
    ) -> SystemPerformanceResponse:
        return await self._performance_queries.performance(window)

    async def research_collector(self) -> ResearchCollectorResponse:
        return await self._overview_queries.research_collector()

    async def live_accounts(self) -> LiveAccountsResponse:
        return await self._overview_queries.live_accounts()

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse:
        return await self._live_account_metrics_queries.live_account_metrics(
            equity_range
        )

    async def overview(self) -> SystemOverviewResponse:
        return await self._overview_queries.overview()

    async def universe(self) -> UniverseStatusResponse:
        return await self._overview_queries.universe()

    async def paper_accounts(self) -> PaperAccountsResponse:
        return await self._paper_account_queries.paper_accounts()

    async def paper_account_equity(self) -> PaperAccountsEquityResponse:
        return await self._paper_equity_queries.paper_account_equity()

    async def paper_account(self, run_id: str) -> StrategyRunResponse:
        return await self._paper_account_queries.strategy_run(run_id=run_id)

    async def paper_history(
        self,
        run_id: str,
        *,
        full: bool = False,
    ) -> PaperAccountHistoryResponse:
        return await self._paper_account_queries.paper_history(
            run_id,
            full=full,
        )

    async def strategy_run(
        self,
        run_id: str | None = None,
        *,
        equity_window_end: datetime | None = None,
        _session: AsyncSession | None = None,
    ) -> StrategyRunResponse:
        return await self._paper_account_queries.strategy_run(
            run_id=run_id,
            equity_window_end=equity_window_end,
            session=_session,
        )

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
        environment: str | None = None,
    ) -> AccountOverviewResponse:
        return await self._account_queries.account(
            equity_range=equity_range,
            account_label=account_label,
            environment=environment,
        )

    async def risk_execution(self) -> RiskExecutionResponse:
        return await self._risk_execution_queries.risk_execution()

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


    async def account_performance(
        self,
        account_label: str = "primary",
        window_hours: int = 24,
        environment: str = "live",
        asset: str = "USDT",
        end_time: datetime | None = None,
        is_empty_proven: bool = False,
    ) -> dict[str, object]:
        """Compute authoritative account performance metrics using
        AccountPerformanceCalculator.
        """
        now = self._clock() if end_time is None else end_time
        start_time = now - timedelta(hours=window_hours)
        async with self._session_factory() as session:
            snaps_list = list(
                (
                    await session.scalars(
                        select(AccountBalanceSnapshotRow)
                        .where(
                            AccountBalanceSnapshotRow.account_label == account_label,
                            AccountBalanceSnapshotRow.environment == environment,
                            AccountBalanceSnapshotRow.asset == asset,
                            AccountBalanceSnapshotRow.observed_at >= start_time,
                            AccountBalanceSnapshotRow.observed_at <= now,
                        )
                        .order_by(AccountBalanceSnapshotRow.observed_at)
                    )
                ).all()
            )
            if not snaps_list:
                return {"status": "no_data", "account_label": account_label}

            # If earliest snapshot does not bracket start_time,
            # fetch the closest preceding snapshot
            if snaps_list[0].observed_at > start_time:
                prev_snap = (
                    await session.scalars(
                        select(AccountBalanceSnapshotRow)
                        .where(
                            AccountBalanceSnapshotRow.account_label == account_label,
                            AccountBalanceSnapshotRow.environment == environment,
                            AccountBalanceSnapshotRow.asset == asset,
                            AccountBalanceSnapshotRow.observed_at < start_time,
                        )
                        .order_by(AccountBalanceSnapshotRow.observed_at.desc())
                        .limit(1)
                    )
                ).first()
                if prev_snap is not None:
                    snaps_list.insert(0, prev_snap)

            cf_rows = (
                await session.scalars(
                    select(CashFlowCorrectionRow)
                    .where(
                        CashFlowCorrectionRow.account_label == account_label,
                        CashFlowCorrectionRow.effective_at >= start_time,
                        CashFlowCorrectionRow.effective_at <= now,
                    )
                    .order_by(CashFlowCorrectionRow.effective_at)
                )
            ).all()

            effective_start = snaps_list[0].observed_at
            effective_end = snaps_list[-1].observed_at

            return build_performance_summary_dict(
                account_label=account_label,
                equity_rows=snaps_list,
                cf_rows=cf_rows,
                start_time=effective_start,
                end_time=effective_end,
                max_equity_gap=None,
                environment=environment,
                asset=asset,
                is_empty_proven=is_empty_proven,
            )
