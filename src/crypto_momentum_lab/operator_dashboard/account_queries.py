"""Live-account detail read model for the operator dashboard.

This module owns the account-detail join: current process state, account
configuration, reconciliation, bounded equity, positions, orders, fills,
signals, and intent metadata are assembled into one response.  The live
metrics time-series domain remains in ``live_account_metrics_queries``.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.live_account_metrics_queries import (
    AccountEquityPoint,
    account_equity_range,
    account_equity_statement,
)
from crypto_momentum_lab.operator_dashboard.overview_queries import (
    latest_live_account_process_statement,
    live_account_summaries,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    LiveAccountSummaryResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountFillEventRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveStrategySignalRow,
    OrderIntentExecutionRow,
    StrategyLiveStateRow,
    TradingLeaseRow,
)

_LIVE_SIGNAL_MAX_ROWS = 30


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


def _order_intent_reason(details: object) -> str | None:
    if not isinstance(details, dict):
        return None
    reason = details.get("reason")
    return reason if isinstance(reason, str) and reason else None


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
        key=lambda aggregate: aggregate.trade_at
        or datetime.min.replace(tzinfo=UTC),
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


class LiveAccountQueries:
    """Read and assemble the live-account detail response."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
        environment: str | None = None,
    ) -> AccountOverviewResponse:
        equity_window, equity_bucket_seconds = account_equity_range(equity_range)
        equity_window_end = self._clock()
        equity_window_start = equity_window_end - equity_window
        process: ExecutionAccountProcessStateRow | None = None
        reconciliation: AccountReconciliationRunRow | None = None
        account_config: AccountConfigSnapshotRow | None = None
        balances: Sequence[AccountBalanceSnapshotRow] = ()
        equity_rows: Sequence[AccountEquityPoint] = ()
        positions: Sequence[AccountPositionSnapshotRow] = ()
        orders: Sequence[AccountOpenOrderRow] = ()
        fills: Sequence[AccountFillEventRow] = ()
        live_signals: Sequence[LiveStrategySignalRow] = ()
        execution_orders: Sequence[ExchangeOrderRow] = ()
        intent_rows: Sequence[OrderIntentExecutionRow] = ()
        available_accounts: list[LiveAccountSummaryResponse] = []
        async with self._session_factory() as session:
            live_processes = (
                await session.scalars(latest_live_account_process_statement())
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
            available_accounts = live_account_summaries(
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
                    AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            account_equity_statement(
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
                        select(func.max(AccountPositionSnapshotRow.observed_at)).where(
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
                {
                    "observed_at": row.observed_at.isoformat(),
                    "balance": str(row.wallet_balance),
                    "equity": str(row.wallet_balance + row.unrealized_pnl),
                    "realized_pnl": None,
                    "unrealized_pnl": str(row.unrealized_pnl),
                }
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
                    "entry_notional": str(abs(row.position_amt * row.entry_price)),
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


def _json_mapping(value: dict[str, object]) -> dict[str, JsonValue]:
    return {key: _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, Decimal | datetime):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


__all__ = [
    "LiveAccountQueries",
    "_AccountFillAggregate",
    "_aggregate_account_fills",
    "_live_strategy_signal",
    "_order_intent_reason",
]
