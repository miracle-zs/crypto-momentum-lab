import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import (
    Select,
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
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
)
from crypto_momentum_lab.persistence.postgres.models import (
    LiveSessionTransitionRow,
    ShadowSessionRow,
    StrategyRuntimeCheckpointRow,
)

_EQUITY_WINDOW = timedelta(hours=24)
_EQUITY_BUCKET_SECONDS = 6 * 60
_EQUITY_MAX_POINTS = 240
_LIVE_SIGNAL_MAX_ROWS = 30
_PAPER_HISTORY_RECENT_LIMIT = 500
_COMMON_EQUITY_BUCKET_SECONDS = 15 * 60
FIXED_COMMON_EQUITY_START_AT = datetime(2026, 8, 21, 2, 45, tzinfo=UTC)
DecisionSLOQueries = _telemetry_queries.DecisionSLOQueries
_decision_slo_response = _telemetry_queries._decision_slo_response
RiskExecutionQueries = _risk_execution_queries.RiskExecutionQueries
_split_exchange_orders = _risk_execution_queries.split_exchange_orders
_exchange_order = _risk_execution_queries.exchange_order
LiveCashFlowAdjustment = _common_equity.LiveCashFlowAdjustment
_as_utc = _common_equity.as_utc
_EquityObservation = _common_equity.EquityObservation
_common_equity_interval_seconds = _common_equity.common_equity_interval_seconds
_build_common_equity_curve = _common_equity.build_common_equity_curve
_build_common_equity_result = _common_equity.build_common_equity_result
_live_account_equity_point = _common_equity.live_account_equity_point
_live_cash_flow_payload = _common_equity.live_cash_flow_payload
_common_equity_note = _common_equity.common_equity_note
_paper_equity_observations = _common_equity.paper_equity_observations
_paper_equity_observations_from_values = (
    _common_equity.paper_equity_observations_from_values
)
_live_equity_observations = _common_equity.live_equity_observations
_live_aggregated_equity_observations = (
    _common_equity.live_aggregated_equity_observations
)
_apply_live_cash_flow_adjustments = (
    _common_equity.apply_live_cash_flow_adjustments
)
LiveAccountMetricsQueries = _live_account_metrics_queries.LiveAccountMetricsQueries
_AccountEquityPoint = _live_account_metrics_queries.AccountEquityPoint
_account_equity_range = _live_account_metrics_queries.account_equity_range
_account_equity_statement = _live_account_metrics_queries.account_equity_statement
_account_margin_statement = _live_account_metrics_queries.account_margin_statement
_live_account_metrics_window_start = (
    _live_account_metrics_queries.live_account_metrics_window_start
)
_live_account_metric_points = _live_account_metrics_queries.live_account_metric_points
_latest_live_account_process_statement = (
    _overview_queries.latest_live_account_process_statement
)
_account_label_sort_key = _overview_queries.account_label_sort_key
_live_account_status = _overview_queries.live_account_status
_live_account_fleet_status = _overview_queries.live_account_fleet_status
_live_account_summaries = _overview_queries.live_account_summaries
_service = _overview_queries.service
_live_observation = _overview_queries.live_observation
_age = _overview_queries.age
_universe_entry = _overview_queries.universe_entry
_universe_membership = _overview_queries.universe_membership
PaperAccountQueries = _paper_account_queries.PaperAccountQueries
_PaperEquitySummaryPoint = _paper_account_queries._PaperEquitySummaryPoint
_downsample_equity_snapshots = _paper_account_queries._downsample_equity_snapshots
_is_dashboard_paper_run = _paper_account_queries._is_dashboard_paper_run
_paper_account_summary = _paper_account_queries._paper_account_summary
_paper_exit_details = _paper_account_queries._paper_exit_details
_paper_exit_label = _paper_account_queries._paper_exit_label
_json_mapping = _paper_account_queries._json_mapping
_json_value = _paper_account_queries._json_value
PaperEquityQueries = _paper_equity_queries.PaperEquityQueries
_PaperEquityPoint = _paper_equity_queries._PaperEquityPoint
_paper_run_values = _paper_equity_queries._paper_run_values
_paper_first_equity_statement = _paper_equity_queries._paper_first_equity_statement
_paper_latest_equity_statement = _paper_equity_queries._paper_latest_equity_statement
_paper_common_equity_statement = (
    _paper_equity_queries._paper_common_equity_statement
)
_paper_equity_statement = _paper_equity_queries._paper_equity_statement
_live_common_equity_statement = _paper_equity_queries._live_common_equity_statement
LiveAccountQueries = _account_queries.LiveAccountQueries
_AccountFillAggregate = _account_queries._AccountFillAggregate
_aggregate_account_fills = _account_queries._aggregate_account_fills
_live_strategy_signal = _account_queries._live_strategy_signal
_order_intent_reason = _account_queries._order_intent_reason


DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS = (
    LiveCashFlowAdjustment(
        account_label="primary",
        effective_at=datetime(
            2026,
            8,
            21,
            9,
            41,
            19,
            895915,
            tzinfo=UTC,
        ),
        amount=Decimal("200"),
        cash_flow_type="deposit",
    ),
)


def _latest_checkpoint_at_statement() -> Select[tuple[datetime]]:
    return (
        select(StrategyRuntimeCheckpointRow.saved_at)
        .order_by(StrategyRuntimeCheckpointRow.saved_at.desc())
        .limit(1)
    )


def _checkpoint_times_statement(
    run_ids: Sequence[str],
) -> Select[tuple[str, datetime]]:
    return select(
        StrategyRuntimeCheckpointRow.run_id,
        StrategyRuntimeCheckpointRow.saved_at,
    ).where(StrategyRuntimeCheckpointRow.run_id.in_(run_ids))


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
        raise ValueError(
            "CML_DASHBOARD_LIVE_CASH_FLOWS_JSON must be a JSON list"
        )

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
        parsed_at = datetime.fromisoformat(
            raw_value.strip().replace("Z", "+00:00")
        )
    except ValueError as error:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT must be an ISO-8601 timestamp"
        ) from error
    if parsed_at.tzinfo is None:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT must include a timezone"
        )
    parsed_at = parsed_at.astimezone(UTC)
    if parsed_at != FIXED_COMMON_EQUITY_START_AT:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT is fixed at "
            "2026-08-21T02:45:00Z"
        )
    return FIXED_COMMON_EQUITY_START_AT


class DashboardQueries:
    _live_account_summaries = staticmethod(
        _overview_queries.live_account_summaries
    )

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
        stale_after_seconds: float = 120.0,
        paper_run_ids: frozenset[str] | None = None,
        live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment]
        | None = None,
        common_equity_start_at: datetime | None = None,
        research_collector_root: Path = DEFAULT_RESEARCH_COLLECTOR_ROOT,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(tz=UTC))
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
        self._risk_execution_queries = RiskExecutionQueries(session_factory)
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

    async def health(self) -> dict[str, str]:
        return await self._overview_queries.health()

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse:
        return await self._telemetry_queries.decision_slo(window)

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
            shadow = (
                await session.scalars(
                    select(ShadowSessionRow)
                    .order_by(ShadowSessionRow.started_at.desc())
                    .limit(10)
                )
            ).all()
            live = (
                await session.scalars(
                    select(LiveSessionTransitionRow)
                    .order_by(LiveSessionTransitionRow.occurred_at.desc())
                    .limit(10)
                )
            ).all()
        return RunReportSummaryResponse(
            status=OperationalStatus.READY
            if shadow or live
            else OperationalStatus.NO_DATA,
            shadow_sessions=[
                {
                    "run_id": row.run_id,
                    "strategy_name": row.strategy_name,
                    "state": row.state,
                    "started_at": row.started_at.isoformat(),
                }
                for row in shadow
            ],
            live_sessions=[
                {
                    "session_id": row.session_id,
                    "state": row.state,
                    "occurred_at": row.occurred_at.isoformat(),
                }
                for row in live
            ],
        )
