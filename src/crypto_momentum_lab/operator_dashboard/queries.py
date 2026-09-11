import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast

from sqlalchemy import (
    Select,
    func,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
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
    LiveAccountSummaryResponse,
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
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountFillEventRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    LiveStrategySignalRow,
    OrderIntentExecutionRow,
    ShadowSessionRow,
    StrategyLiveStateRow,
    StrategyRuntimeCheckpointRow,
    TradingLeaseRow,
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


@dataclass(slots=True)
class _AccountFillAggregate:
    symbol: str
    order_id: str
    side: str
    strategy_name: str | None
    quantity: Decimal = Decimal("0")
    notional: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    trade_at: datetime | None = None
    fill_count: int = 0
    fee_assets: set[str] = field(default_factory=set)
    reduce_only: bool = False
    close_reason: str | None = None


def _aggregate_account_fills(
    rows: Sequence[AccountFillEventRow],
    strategy_by_order: dict[str, str],
    order_metadata_by_order: Mapping[str, Mapping[str, JsonValue]] | None = None,
    *,
    limit: int = 20,
) -> list[dict[str, JsonValue]]:
    """Collapse exchange partial fills into one order-level dashboard row."""
    grouped: dict[tuple[str, str, str], _AccountFillAggregate] = {}
    for row in rows:
        key = (row.order_id, row.symbol, row.side)
        aggregate = grouped.get(key)
        if aggregate is None:
            metadata = (order_metadata_by_order or {}).get(row.order_id, {})
            close_reason = metadata.get("close_reason")
            aggregate = _AccountFillAggregate(
                symbol=row.symbol,
                order_id=row.order_id,
                side=row.side,
                strategy_name=strategy_by_order.get(row.order_id),
                reduce_only=bool(metadata.get("reduce_only", False)),
                close_reason=(
                    close_reason if isinstance(close_reason, str) else None
                ),
            )
            grouped[key] = aggregate
        aggregate.fee_assets.add(row.fee_asset)
        aggregate.quantity += row.quantity
        aggregate.notional += row.price * row.quantity
        aggregate.realized_pnl += row.realized_pnl
        aggregate.fee += row.fee
        aggregate.trade_at = (
            row.trade_at
            if aggregate.trade_at is None
            else max(aggregate.trade_at, row.trade_at)
        )
        aggregate.fill_count += 1

    ordered = sorted(
        grouped.values(),
        key=lambda aggregate: aggregate.trade_at or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )[:limit]
    return [
        {
            "symbol": aggregate.symbol,
            "order_id": aggregate.order_id,
            "side": aggregate.side,
            "price": str(
                aggregate.notional / aggregate.quantity
                if aggregate.quantity
                else Decimal("0")
            ),
            "quantity": str(aggregate.quantity),
            "realized_pnl": str(aggregate.realized_pnl),
            "fee": str(aggregate.fee),
            "fee_asset": " / ".join(sorted(aggregate.fee_assets)),
            "trade_at": (
                None
                if aggregate.trade_at is None
                else aggregate.trade_at.isoformat()
            ),
            "fill_count": aggregate.fill_count,
            "strategy_name": aggregate.strategy_name,
            "reduce_only": aggregate.reduce_only,
            "close_reason": aggregate.close_reason,
        }
        for aggregate in ordered
    ]


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
        equity_window, equity_bucket_seconds = _account_equity_range(equity_range)
        equity_window_end = self._clock()
        equity_window_start = equity_window_end - equity_window
        process: ExecutionAccountProcessStateRow | None = None
        reconciliation: AccountReconciliationRunRow | None = None
        account_config: AccountConfigSnapshotRow | None = None
        balances: Sequence[AccountBalanceSnapshotRow] = ()
        equity_rows: Sequence[_AccountEquityPoint] = ()
        positions: Sequence[AccountPositionSnapshotRow] = ()
        orders: Sequence[AccountOpenOrderRow] = ()
        fills: Sequence[AccountFillEventRow] = ()
        live_signals: Sequence[LiveStrategySignalRow] = ()
        execution_orders: Sequence[ExchangeOrderRow] = ()
        intent_rows: Sequence[OrderIntentExecutionRow] = ()
        available_accounts: list[LiveAccountSummaryResponse] = []
        async with self._session_factory() as session:
            live_processes = (
                await session.scalars(_latest_live_account_process_statement())
            ).all()
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            leases = (
                await session.scalars(
                    select(TradingLeaseRow)
                    .where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > equity_window_end,
                    )
                )
            ).all()
            available_accounts = self._live_account_summaries(
                live_processes,
                strategy_states,
                leases,
            )
            process_query = select(ExecutionAccountProcessStateRow)
            if environment is None:
                process_query = process_query.where(
                    ExecutionAccountProcessStateRow.environment == "live"
                )
            if account_label is not None:
                process_query = process_query.where(
                    ExecutionAccountProcessStateRow.account_label == account_label
                )
            if environment is not None:
                process_query = process_query.where(
                    ExecutionAccountProcessStateRow.environment == environment
                )
            process = await session.scalar(
                process_query.order_by(
                    ExecutionAccountProcessStateRow.occurred_at.desc()
                ).limit(1)
            )
            if process is not None:
                environment = process.environment
                account_label = process.account_label
                live_signals = (
                    await session.scalars(
                        select(LiveStrategySignalRow)
                        .where(
                            LiveStrategySignalRow.account_label == account_label
                        )
                        .order_by(
                            LiveStrategySignalRow.detected_at.desc(),
                            LiveStrategySignalRow.recorded_at.desc(),
                        )
                        .limit(_LIVE_SIGNAL_MAX_ROWS)
                    )
                ).all()
                equity_rows = [
                    _AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            _account_equity_statement(
                                environment=environment,
                                account_label=account_label,
                                asset="USDT",
                                window_start=equity_window_start,
                                window_end=equity_window_end,
                                interval_seconds=equity_bucket_seconds,
                            )
                        )
                    ).all()
                ]
                account_config = await session.scalar(
                    select(AccountConfigSnapshotRow)
                    .where(
                        AccountConfigSnapshotRow.environment == environment,
                        AccountConfigSnapshotRow.account_label == account_label,
                    )
                    .order_by(AccountConfigSnapshotRow.observed_at.desc())
                    .limit(1)
                )
                reconciliation = await session.scalar(
                    select(AccountReconciliationRunRow)
                    .where(
                        AccountReconciliationRunRow.environment == environment,
                        AccountReconciliationRunRow.account_label == account_label,
                        AccountReconciliationRunRow.status == "ready",
                    )
                    .order_by(AccountReconciliationRunRow.observed_at.desc())
                    .limit(1)
                )
                balance_at = await session.scalar(
                    select(func.max(AccountBalanceSnapshotRow.observed_at)).where(
                        AccountBalanceSnapshotRow.environment == environment,
                        AccountBalanceSnapshotRow.account_label == account_label,
                    )
                )
                if balance_at is not None:
                    balances = (
                        await session.scalars(
                            select(AccountBalanceSnapshotRow).where(
                                AccountBalanceSnapshotRow.environment == environment,
                                AccountBalanceSnapshotRow.account_label
                                == account_label,
                                AccountBalanceSnapshotRow.observed_at == balance_at,
                            )
                        )
                    ).all()
                if reconciliation is not None and reconciliation.position_count > 0:
                    position_at = await session.scalar(
                        select(
                            func.max(AccountPositionSnapshotRow.observed_at)
                        ).where(
                            AccountPositionSnapshotRow.environment == environment,
                            AccountPositionSnapshotRow.account_label == account_label,
                        )
                    )
                    if position_at is not None:
                        positions = (
                            await session.scalars(
                                select(AccountPositionSnapshotRow).where(
                                    AccountPositionSnapshotRow.environment
                                    == environment,
                                    AccountPositionSnapshotRow.account_label
                                    == account_label,
                                    AccountPositionSnapshotRow.observed_at
                                    == position_at,
                                    AccountPositionSnapshotRow.position_amt != 0,
                                )
                            )
                        ).all()
                orders = (
                    await session.scalars(
                        select(AccountOpenOrderRow)
                        .where(
                            AccountOpenOrderRow.environment == environment,
                            AccountOpenOrderRow.account_label == account_label,
                        )
                        .order_by(AccountOpenOrderRow.observed_at.desc())
                        .limit(20)
                    )
                ).all()
                fills = (
                    await session.scalars(
                        select(AccountFillEventRow)
                        .where(
                            AccountFillEventRow.environment == environment,
                            AccountFillEventRow.account_label == account_label,
                        )
                        .order_by(AccountFillEventRow.trade_at.desc())
                        .limit(200)
                    )
                ).all()
                execution_orders = (
                    await session.scalars(
                        select(ExchangeOrderRow)
                        .order_by(ExchangeOrderRow.updated_at.desc())
                        .limit(200)
                    )
                ).all()
                intent_ids = {row.intent_id for row in execution_orders}
                if intent_ids:
                    intent_rows = (
                        await session.scalars(
                            select(OrderIntentExecutionRow).where(
                                OrderIntentExecutionRow.intent_id.in_(intent_ids)
                            )
                        )
                    ).all()

        execution_by_client = {
            row.client_order_id: row for row in execution_orders
        }
        intent_by_id = {row.intent_id: row for row in intent_rows}
        strategy_by_order = {
            row.exchange_order_id: intent_by_id[row.intent_id].strategy_name
            for row in execution_orders
            if row.exchange_order_id is not None
            and row.intent_id in intent_by_id
        }
        order_metadata_by_order = {
            row.exchange_order_id: {
                "reduce_only": row.reduce_only,
                "close_reason": (
                    _order_intent_reason(intent_by_id[row.intent_id].details)
                    if row.reduce_only
                    else None
                ),
            }
            for row in execution_orders
            if row.exchange_order_id is not None and row.intent_id in intent_by_id
        }
        strategy_by_symbol: dict[str, str] = {}
        for order in execution_orders:
            intent = intent_by_id.get(order.intent_id)
            if (
                intent is not None
                and order.state == ExchangeOrderState.FILLED.value
                and not order.reduce_only
            ):
                strategy_by_symbol.setdefault(order.symbol, intent.strategy_name)

        recent_trades = _aggregate_account_fills(
            fills,
            strategy_by_order,
            order_metadata_by_order,
        )

        reconciliation_payload: dict[str, JsonValue] = (
            {}
            if reconciliation is None
            else {
                "status": reconciliation.status,
                "observed_at": reconciliation.observed_at.isoformat(),
                "balance_count": reconciliation.balance_count,
                "position_count": reconciliation.position_count,
                "open_order_count": reconciliation.open_order_count,
                "fill_count": reconciliation.fill_count,
                "mismatch_count": reconciliation.mismatch_count,
            }
        )
        account_config_payload: dict[str, JsonValue] = (
            {}
            if account_config is None
            else {
                "multi_assets_mode": account_config.multi_assets_mode,
                "hedge_mode": account_config.hedge_mode,
                "fee_tier": account_config.fee_tier,
                "observed_at": account_config.observed_at.isoformat(),
            }
        )
        usdt = next((row for row in balances if row.asset == "USDT"), None)
        total_unrealized = sum(
            (row.unrealized_pnl for row in balances),
            start=Decimal("0"),
        )
        total_notional = sum(
            (abs(row.notional) for row in positions),
            start=Decimal("0"),
        )
        observed_at = None if process is None else process.occurred_at
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN
            if process is None
            else OperationalStatus.READY
            if process.state == "ready_readonly"
            else OperationalStatus.HALTED,
            observed_at=observed_at,
            environment=None if process is None else process.environment,
            account_label=None if process is None else process.account_label,
            account_config=account_config_payload,
            reconciliation=reconciliation_payload,
            equity_range=cast(
                Literal["24h", "7d", "30d", "1y"],
                equity_range,
            ),
            equity_window_start=equity_window_start,
            equity_window_end=equity_window_end,
            equity_sample_interval_seconds=equity_bucket_seconds,
            equity_curve=[
                _live_account_equity_point(row)
                for row in sorted(
                    equity_rows,
                    key=lambda row: row.observed_at,
                )
            ],
            available_accounts=available_accounts,
            summary={
                "usdt_wallet_balance": (
                    None if usdt is None else str(usdt.wallet_balance)
                ),
                "usdt_available_balance": (
                    None if usdt is None else str(usdt.available_balance)
                ),
                "total_unrealized_pnl": str(total_unrealized),
                "gross_position_notional": str(total_notional),
                "position_count": len(positions),
                "open_order_count": len(orders),
                "recent_trade_count": len(recent_trades),
                "recent_fill_count": len(fills),
            },
            balances=[
                {
                    "asset": row.asset,
                    "wallet_balance": str(row.wallet_balance),
                    "available_balance": str(row.available_balance),
                    "unrealized_pnl": str(row.unrealized_pnl),
                }
                for row in balances
            ],
            positions=[
                {
                    "symbol": row.symbol,
                    "position_side": row.position_side,
                    "position_amt": str(row.position_amt),
                    "entry_price": str(row.entry_price),
                    "notional": str(row.notional),
                    "unrealized_pnl": str(row.unrealized_pnl),
                    "leverage": row.leverage,
                    "mark_price": str(row.mark_price),
                    "margin_type": row.margin_type,
                    "strategy_name": strategy_by_symbol.get(row.symbol),
                    "entry_notional": str(
                        abs(row.position_amt * row.entry_price)
                    ),
                }
                for row in positions
            ],
            open_orders=[
                {
                    "symbol": row.symbol,
                    "client_order_id": row.client_order_id,
                    "side": row.side,
                    "order_type": row.order_type,
                    "price": str(row.price),
                    "original_quantity": str(row.original_quantity),
                    "executed_quantity": str(row.executed_quantity),
                    "remaining_quantity": str(
                        max(
                            Decimal("0"),
                            row.original_quantity - row.executed_quantity,
                        )
                    ),
                    "status": row.status,
                    "reduce_only": row.reduce_only,
                    "observed_at": row.observed_at.isoformat(),
                    "strategy_name": (
                        None
                        if (internal := execution_by_client.get(row.client_order_id))
                        is None
                        or (intent := intent_by_id.get(internal.intent_id)) is None
                        else intent.strategy_name
                    ),
                }
                for row in orders
            ],
            fills=recent_trades,
            live_signals=[_live_strategy_signal(row) for row in live_signals],
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

def _order_intent_reason(details: object) -> str | None:
    if not isinstance(details, dict):
        return None
    reason = details.get("reason")
    return reason if isinstance(reason, str) and reason else None


def _live_strategy_signal(row: LiveStrategySignalRow) -> dict[str, JsonValue]:
    return {
        "observation_id": row.observation_id,
        "signal_id": row.signal_id,
        "candidate_id": row.candidate_id,
        "run_id": row.run_id,
        "account_label": row.account_label,
        "strategy_name": row.strategy_name,
        "strategy_version": row.strategy_version,
        "config_hash": row.config_hash,
        "code_commit": row.code_commit,
        "signal_kind": row.signal_kind,
        "symbol": row.symbol,
        "side": row.side,
        "detected_at": row.detected_at.isoformat(),
        "source_state_at": row.source_state_at.isoformat(),
        "recorded_at": row.recorded_at.isoformat(),
        "reason": row.reason,
        "schema_version": row.schema_version,
        "quote_volume_24h": (
            None if row.quote_volume_24h is None else str(row.quote_volume_24h)
        ),
        "quote_volume_24h_quote_asset": row.quote_volume_24h_quote_asset,
        "quote_volume_24h_source": row.quote_volume_24h_source,
        "quote_volume_24h_source_at": (
            None
            if row.quote_volume_24h_source_at is None
            else row.quote_volume_24h_source_at.isoformat()
        ),
        "quote_volume_24h_fetched_at": (
            None
            if row.quote_volume_24h_fetched_at is None
            else row.quote_volume_24h_fetched_at.isoformat()
        ),
        "quote_volume_24h_age_ms": row.quote_volume_24h_age_ms,
        "features": _json_mapping(row.features),
        "reference_prices": _json_mapping(row.reference_prices),
        "market_context": _json_mapping(row.market_context),
        "filter_context": _json_mapping(row.filter_context),
        "candidate_context": _json_mapping(row.candidate_context),
        "account_context": _json_mapping(row.account_context),
    }
