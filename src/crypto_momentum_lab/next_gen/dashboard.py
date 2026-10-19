"""Read-only current trading views over NextGen orders and batches."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.live_rollout.runtime_manifest import LiveRuntimeAccount
from crypto_momentum_lab.next_gen.db_models import LiveOrderRow, LivePositionBatchRow
from crypto_momentum_lab.operator_dashboard.live_account_metrics_queries import (
    account_equity_range,
)
from crypto_momentum_lab.operator_dashboard.overview_queries import (
    live_account_fleet_status,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    LiveAccountMetricPointResponse,
    LiveAccountMetricsAccountResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    MarketDataPerformanceResponse,
    RiskExecutionResponse,
    StreamReadinessDetailResponse,
    SystemReadinessResponse,
    TradeabilityDetailResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import (
    RiskHaltRow,
    RuntimeMarketState15sRow,
    StrategyRuntimeEventRow,
)

TERMINAL_STATUSES = ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
EquityRange = Literal["24h", "7d", "30d", "1y"]


class NextGenDashboardQueries:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        accounts: tuple[LiveRuntimeAccount, ...],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = session_factory
        self._accounts = {account.label: account for account in accounts}
        self._clock = clock

    async def _status(
        self, account_label: str
    ) -> tuple[LiveAccountSummaryResponse, StrategyRuntimeEventRow | None]:
        async with self._sessions() as session:
            row = await session.scalar(
                select(StrategyRuntimeEventRow)
                .where(
                    StrategyRuntimeEventRow.run_id == f"nextgen:live:{account_label}",
                    StrategyRuntimeEventRow.event_type == "nextgen_runtime_status",
                )
                .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                .limit(1)
            )
        if row is None:
            return LiveAccountSummaryResponse(
                account_label=account_label,
                environment="live",
                status=OperationalStatus.UNKNOWN,
                readiness="UNKNOWN",
                strategy_name=self._accounts[account_label].strategy,
            ), None
        fresh = 0 <= (self._clock() - row.occurred_at).total_seconds() <= 180
        payload = cast(dict[str, Any], row.details)
        status = (
            (
                OperationalStatus.READY
                if payload["ready"]
                else OperationalStatus.DEGRADED
            )
            if fresh
            else OperationalStatus.STALE
        )
        if fresh and payload["halt_active"]:
            status = OperationalStatus.HALTED
        return LiveAccountSummaryResponse(
            account_label=account_label,
            environment="live",
            status=status,
            readiness="READY" if status is OperationalStatus.READY else status.value,
            observed_at=row.occurred_at,
            strategy_name=payload["strategy_name"],
            strategy_state=payload["mode"],
            runtime_observed_at=row.occurred_at,
            runtime_started_at=payload.get("started_at"),
            runtime_tradeability=TradeabilityDetailResponse(
                mode=payload["mode"],
                entry_gate_open=fresh and payload["entry_gate_open"],
                entry_gate_reason="ready"
                if fresh and payload["entry_gate_open"]
                else "scheduled_risk_window"
                if fresh and payload.get("scheduled_entry_blocked") is True
                else "runtime_not_ready",
                exit_gate_open=fresh and payload["exit_gate_open"],
                exit_gate_reason="ready"
                if fresh and payload["exit_gate_open"]
                else "runtime_not_ready",
            ),
        ), row

    async def live_accounts(self) -> LiveAccountsResponse:
        accounts = [(await self._status(label))[0] for label in self._accounts]
        return LiveAccountsResponse(
            status=live_account_fleet_status(accounts), accounts=accounts
        )

    async def market_performance(self) -> MarketDataPerformanceResponse:
        samples = []
        for label in self._accounts:
            _, row = await self._status(label)
            if row is None or not row.details.get("market_data"):
                continue
            sample = cast(dict[str, Any], row.details["market_data"])
            received_at = datetime.fromisoformat(sample["received_at"])
            if 0 <= (self._clock() - received_at).total_seconds() <= 120:
                samples.append((received_at, sample))
        if not samples:
            return MarketDataPerformanceResponse(status=OperationalStatus.NO_DATA)
        received_at, sample = max(samples, key=lambda item: item[0])
        return MarketDataPerformanceResponse(
            status=OperationalStatus.READY,
            observed_at=datetime.fromisoformat(sample["bucket_end"]),
            market_delay_ms=round(float(sample["delay_ms"]), 1),
            latency_source="strategy_receive",
            latency_observed_at=received_at,
        )

    async def readiness(self, liveness: dict[str, str]) -> SystemReadinessResponse:
        fleet = await self.live_accounts()
        ready = fleet.status is OperationalStatus.READY
        can_enter = (
            ready
            and bool(fleet.accounts)
            and all(
                account.runtime_tradeability is not None
                and account.runtime_tradeability.entry_gate_open
                for account in fleet.accounts
            )
        )
        scheduled_block = any(
            account.runtime_tradeability is not None
            and account.runtime_tradeability.entry_gate_reason
            == "scheduled_risk_window"
            for account in fleet.accounts
        )
        can_exit = bool(fleet.accounts) and all(
            account.runtime_tradeability is not None
            and account.runtime_tradeability.exit_gate_open
            for account in fleet.accounts
        )
        return SystemReadinessResponse(
            status=fleet.status,
            observed_at=self._clock(),
            liveness=liveness,
            tradeability=TradeabilityDetailResponse(
                mode="RUNNING" if ready else "DEGRADED",
                entry_gate_open=can_enter,
                entry_gate_reason="ready"
                if can_enter
                else "scheduled_risk_window"
                if scheduled_block
                else "account_runtime_not_ready",
                exit_gate_open=can_exit,
                exit_gate_reason="ready" if can_exit else "account_runtime_not_ready",
            ),
            stream_readiness=StreamReadinessDetailResponse(
                overall="READY" if ready else "RECOVERING",
                streams={
                    account.account_label: account.readiness
                    for account in fleet.accounts
                },
            ),
            accounts=fleet.accounts,
        )

    async def _equity_points(
        self, account_label: str, equity_range: str
    ) -> tuple[datetime, datetime, int, list[LiveAccountMetricPointResponse]]:
        window, interval = account_equity_range(equity_range)
        end = self._clock()
        start = end - window
        # At most 241 indexed probes; do not scan every heartbeat in a year.
        statement = text("""
            SELECT point.occurred_at, point.details
            FROM generate_series(
                CAST(:start_at AS timestamptz), CAST(:end_at AS timestamptz),
                make_interval(secs => :interval)
            ) AS buckets(bucket)
            JOIN LATERAL (
                SELECT occurred_at, details FROM strategy_runtime_events
                WHERE run_id = :run_id AND event_type = 'nextgen_runtime_status'
                  AND occurred_at >= buckets.bucket
                  AND occurred_at < buckets.bucket + make_interval(secs => :interval)
                  AND occurred_at <= :end_at
                ORDER BY occurred_at DESC LIMIT 1
            ) AS point ON TRUE
            ORDER BY point.occurred_at
        """)
        async with self._sessions() as session:
            rows = (
                (
                    await session.execute(
                        statement,
                        {
                            "start_at": start,
                            "end_at": end,
                            "interval": interval,
                            "run_id": f"nextgen:live:{account_label}",
                        },
                    )
                )
                .mappings()
                .all()
            )
        points = []
        peak = None
        initial = None
        for row in rows:
            payload = row["details"]
            wallet = payload["balances"].get("USDT")
            unrealized = payload["unrealized_pnl"]
            if wallet is None or unrealized is None:
                continue
            equity = Decimal(wallet) + Decimal(unrealized)
            peak = equity if peak is None else max(peak, equity)
            initial = equity if initial is None else initial
            points.append(
                LiveAccountMetricPointResponse(
                    observed_at=row["occurred_at"],
                    equity=str(equity),
                    margin_used=None,
                    equity_change_ratio=None
                    if initial == 0
                    else str((equity - initial) / initial),
                    drawdown=str(equity - peak),
                    drawdown_ratio=None if peak == 0 else str((equity - peak) / peak),
                )
            )
        return start, end, interval, points

    async def live_account_metrics(
        self, equity_range: str = "24h"
    ) -> LiveAccountMetricsResponse:
        accounts = []
        for label in self._accounts:
            summary, _ = await self._status(label)
            start, end, interval, points = await self._equity_points(
                label, equity_range
            )
            accounts.append(
                LiveAccountMetricsAccountResponse(
                    account_label=label,
                    environment="live",
                    status=summary.status,
                    metrics_curve=points,
                )
            )
        return LiveAccountMetricsResponse(
            status=(await self.live_accounts()).status,
            equity_range=cast(EquityRange, equity_range),
            equity_window_start=start,
            equity_window_end=end,
            equity_sample_interval_seconds=interval,
            accounts=accounts,
        )

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
        environment: str | None = None,
    ) -> AccountOverviewResponse:
        label = next(iter(self._accounts)) if account_label is None else account_label
        if label not in self._accounts or environment not in (None, "live"):
            raise ValueError("Unknown NextGen live account")
        config = self._accounts[label]
        summary, status_row = await self._status(label)
        async with self._sessions() as session:
            batches = (
                await session.scalars(
                    select(LivePositionBatchRow)
                    .where(
                        LivePositionBatchRow.environment == "live",
                        LivePositionBatchRow.account_label == label,
                        LivePositionBatchRow.quantity > 0,
                    )
                    .order_by(LivePositionBatchRow.entry_bar_time)
                )
            ).all()
            orders = (
                await session.scalars(
                    select(LiveOrderRow)
                    .where(
                        LiveOrderRow.environment == "live",
                        LiveOrderRow.account_label == label,
                    )
                    .order_by(LiveOrderRow.updated_at.desc())
                    .limit(200)
                )
            ).all()
            # The live order list must not depend on the history-page limit.
            open_orders = (
                await session.scalars(
                    select(LiveOrderRow).where(
                        LiveOrderRow.environment == "live",
                        LiveOrderRow.account_label == label,
                        LiveOrderRow.status.not_in(TERMINAL_STATUSES),
                    )
                )
            ).all()
            marks = (
                await session.scalars(
                    select(RuntimeMarketState15sRow)
                    .distinct(
                        RuntimeMarketState15sRow.symbol,
                    )
                    .where(
                        RuntimeMarketState15sRow.environment == "research",
                        RuntimeMarketState15sRow.symbol.in_(
                            {batch.symbol for batch in batches}
                        ),
                        RuntimeMarketState15sRow.bucket_end <= self._clock(),
                        RuntimeMarketState15sRow.bucket_end
                        >= self._clock() - timedelta(seconds=60),
                    )
                    .order_by(
                        RuntimeMarketState15sRow.symbol,
                        RuntimeMarketState15sRow.bucket_end.desc(),
                    )
                )
            ).all()
            signals = (
                await session.scalars(
                    select(StrategyRuntimeEventRow)
                    .where(
                        StrategyRuntimeEventRow.run_id == f"nextgen:live:{label}",
                        StrategyRuntimeEventRow.event_type == "nextgen_entry_signal",
                    )
                    .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                    .limit(30)
                )
            ).all()
        price_by_symbol = {
            row.symbol: row.mark_price for row in marks if row.mark_price is not None
        }
        payload: dict[str, Any] | None = (
            None if status_row is None else cast(dict[str, Any], status_row.details)
        )
        balances: list[dict[str, Any]] = (
            []
            if payload is None
            else [
                {
                    "asset": asset,
                    "wallet_balance": value,
                    "available_balance": None,
                    "unrealized_pnl": payload["unrealized_pnl"]
                    if asset == "USDT"
                    else None,
                }
                for asset, value in payload["balances"].items()
            ]
        )
        positions: list[dict[str, Any]] = []
        for batch in batches:
            mark = price_by_symbol.get(batch.symbol)
            signed = (
                batch.quantity if batch.position_side == "LONG" else -batch.quantity
            )
            positions.append(
                {
                    "batch_id": batch.batch_id,
                    "symbol": batch.symbol,
                    "position_side": batch.position_side,
                    "position_amt": str(signed),
                    "entry_price": str(batch.entry_price),
                    "entry_notional": str(batch.quantity * batch.entry_price),
                    "notional": None if mark is None else str(signed * mark),
                    "mark_price": None if mark is None else str(mark),
                    "unrealized_pnl": None
                    if mark is None
                    else str(signed * (mark - batch.entry_price)),
                    "leverage": config.execution_inputs.entry_leverage,
                    "margin_type": config.execution_inputs.margin_type,
                    "strategy_name": config.strategy,
                }
            )
        start, end, interval, points = await self._equity_points(label, equity_range)
        return AccountOverviewResponse(
            status=summary.status,
            observed_at=summary.observed_at,
            environment="live",
            account_label=label,
            account_config={
                "engine": "nextgen",
                "strategy_name": config.strategy,
                "strategy_config_hash": config.strategy_config_hash,
            },
            balances=balances,
            positions=positions,
            open_orders=[self._order(row) for row in open_orders],
            fills=[
                {
                    **self._order(row),
                    "quantity": str(row.filled_quantity),
                    "price": str(row.average_price),
                    "trade_at": None,
                    "order_updated_at": row.updated_at.isoformat(),
                    "aggregation": "order",
                    "fee": None,
                    "realized_pnl": None,
                }
                for row in orders
                if row.filled_quantity > 0
            ],
            live_signals=cast(Any, [row.details for row in signals]),
            summary={
                "position_count": len(batches),
                "open_order_count": len(open_orders),
                "recent_trade_count": sum(row.filled_quantity > 0 for row in orders),
                "usdt_wallet_balance": None
                if payload is None
                else payload["balances"].get("USDT"),
                "total_unrealized_pnl": None
                if payload is None
                else payload["unrealized_pnl"],
                "gross_position_notional": (
                    str(
                        sum(
                            (abs(Decimal(p["notional"])) for p in positions),
                            Decimal("0"),
                        )
                    )
                    if all(p["notional"] is not None for p in positions)
                    else None
                ),
            },
            available_accounts=(await self.live_accounts()).accounts,
            equity_range=cast(EquityRange, equity_range),
            equity_window_start=start,
            equity_window_end=end,
            equity_sample_interval_seconds=interval,
            equity_curve=[
                {"observed_at": point.observed_at.isoformat(), "equity": point.equity}
                for point in points
            ],
        )

    @staticmethod
    def _order(row: LiveOrderRow) -> dict[str, Any]:
        return {
            "symbol": row.symbol,
            "client_order_id": row.client_order_id,
            "order_id": row.exchange_order_id,
            "batch_id": row.batch_id,
            "side": row.side,
            "position_side": row.position_side,
            "is_exit": (row.position_side == "LONG" and row.side == "SELL")
            or (row.position_side == "SHORT" and row.side == "BUY"),
            "order_type": row.order_type,
            "status": row.status,
            "price": None if row.price is None else str(row.price),
            "original_quantity": str(row.quantity),
            "executed_quantity": str(row.filled_quantity),
            "remaining_quantity": str(row.quantity - row.filled_quantity),
            "observed_at": row.updated_at.isoformat(),
        }

    async def risk_execution(self) -> RiskExecutionResponse:
        async with self._sessions() as session:
            rows = (
                await session.scalars(
                    select(LiveOrderRow)
                    .where(
                        LiveOrderRow.environment == "live",
                        LiveOrderRow.account_label.in_(self._accounts),
                        LiveOrderRow.status.not_in(TERMINAL_STATUSES),
                    )
                    .order_by(LiveOrderRow.updated_at.desc())
                )
            ).all()
            halts = (
                await session.scalars(
                    select(RiskHaltRow).where(
                        RiskHaltRow.environment == "live",
                        RiskHaltRow.account_label.in_(self._accounts),
                        RiskHaltRow.active.is_(True),
                    )
                )
            ).all()
        return RiskExecutionResponse(
            status=(await self.live_accounts()).status,
            active_halts=[
                {
                    "account_label": row.account_label,
                    "reason": row.reason,
                    "created_at": row.created_at.isoformat(),
                }
                for row in halts
            ],
            latest_risk_decisions=[],
            exchange_orders=[self._order(row) for row in rows],
            ambiguous_orders=[
                self._order(row) for row in rows if row.status == "RECONCILING"
            ],
            observed_at=self._clock(),
            source_status="nextgen",
        )
