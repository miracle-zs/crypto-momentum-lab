import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.apps.shadow_operation.main import (
    _latest_account_state,
    _latest_risk_config,
    _load_trading_rules,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.live_rollout import LiveOperatorApproval
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, StrategyLiveState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.state_machine import SubmitPolicy
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)


class PostgresLiveContextProvider:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        account_label: str,
        run_id: str,
        strategy_name: str,
        strategy_config_hash: str,
        git_commit_hash: str,
        migration_revision: str,
        lease_owner: str,
        approval_id: str,
        lease_ttl_seconds: int = 300,
        lease_renew_before_seconds: int = 120,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        if not 0 < lease_renew_before_seconds < lease_ttl_seconds:
            raise ValueError(
                "lease_renew_before_seconds must be between zero and lease TTL"
            )
        self._sessions = session_factory
        self._account_label = account_label
        self._run_id = run_id
        self._strategy_name = strategy_name
        self._strategy_config_hash = strategy_config_hash
        self._git_commit_hash = git_commit_hash
        self._migration_revision = migration_revision
        self._lease_owner = lease_owner
        self._approval_id = approval_id
        self._lease_ttl_seconds = lease_ttl_seconds
        self._lease_renew_before_seconds = lease_renew_before_seconds
        self._risk_repository = PostgresRiskRepository(session_factory)
        self._live_repository = PostgresLiveRolloutRepository(session_factory)
        self._order_repository = PostgresOrderRepository(session_factory)
        self._cached_bucket_start: datetime | None = None
        self._cached_context: LiveDaemonRuntimeContext | None = None
        self._cached_rules: dict[str, SymbolTradingRules] = {}

    async def __call__(self, state: MarketState15s) -> LiveDaemonRuntimeContext:
        now = datetime.now(tz=UTC)
        if (
            self._cached_bucket_start == state.bucket_start
            and self._cached_context is not None
        ):
            symbol_rules = self._cached_rules.get(state.symbol)
            if symbol_rules is None:
                loaded_rules = await _load_trading_rules(
                    self._sessions,
                    {state.symbol},
                )
                symbol_rules = loaded_rules[state.symbol]
                self._cached_rules[state.symbol] = symbol_rules
            return replace(
                self._cached_context,
                now=now,
                gate_context=replace(self._cached_context.gate_context, now=now),
                trading_rules={state.symbol: symbol_rules},
            )
        approval = await self._live_repository.load_active_approval(
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            now=now,
        )
        if approval is not None and approval.approval_id != self._approval_id:
            approval = None
        risk_config = await _latest_risk_config(
            self._sessions,
            self._account_label,
        )
        lease = await self._risk_repository.load_active_lease(
            "live",
            self._account_label,
            now,
        )
        if (
            lease is not None
            and lease.owner == self._lease_owner
            and lease.expires_at
            <= now + timedelta(seconds=self._lease_renew_before_seconds)
        ):
            lease = await self._risk_repository.renew_lease(
                lease.lease_id,
                self._lease_owner,
                now + timedelta(seconds=self._lease_ttl_seconds),
            )
        halts = await self._risk_repository.load_active_halts(
            "live",
            self._account_label,
        )
        unresolved = await self._order_repository.load_unresolved_orders(
            self._run_id
        )
        account_state = await _latest_account_state(
            self._sessions,
            self._account_label,
        )
        (
            account_observed_at,
            position_symbols,
            unrealized,
            gross,
            managed_positions,
            unmanaged_symbols,
        ) = await self._account_position_view(unresolved)
        realized = await self._daily_realized_pnl(now)
        rules = await _load_trading_rules(self._sessions, {state.symbol})
        last_entries = await self._last_entry_times()
        strategy_state = await self._strategy_live_state()
        unresolved_states = tuple(item.state for item in unresolved)
        gate_context = LiveGateContext(
            now=now,
            live_submit_enabled=True,
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            strategy_config_hash=self._strategy_config_hash,
            git_commit_hash=self._git_commit_hash,
            database_migration_revision=self._migration_revision,
            required_lease_owner=self._lease_owner,
            requested_submit_policy=SubmitPolicy.LIVE_SUBMIT,
            active_lease=lease,
            risk_config=risk_config,
            approval=approval,
            account_state=account_state,
            active_halts=halts,
            unresolved_order_states=unresolved_states,
        )
        context = LiveDaemonRuntimeContext(
            now=now,
            gate_context=gate_context,
            active_lease=lease,
            account_state=account_state,
            account_observed_at=account_observed_at,
            open_position_symbols=position_symbols,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            gross_exposure=gross,
            active_halts=halts,
            unresolved_order_states=unresolved_states,
            risk_config=risk_config,
            strategy_state=strategy_state,
            trading_rules=rules,
            last_entry_at_by_symbol=last_entries,
            managed_positions=managed_positions,
            unmanaged_position_symbols=unmanaged_symbols,
            unresolved_orders=unresolved,
        )
        self._cached_bucket_start = state.bucket_start
        self._cached_context = context
        self._cached_rules[state.symbol] = rules[state.symbol]
        return context

    def invalidate_cache(self) -> None:
        """Force the next state to reload account and risk state."""
        self._cached_bucket_start = None
        self._cached_context = None
        self._cached_rules.clear()

    async def _account_position_view(
        self,
        unresolved: tuple[PersistedExchangeOrder, ...],
    ) -> tuple[
        datetime | None,
        frozenset[str],
        Decimal,
        Decimal,
        tuple[ManagedLivePosition, ...],
        frozenset[str],
    ]:
        async with self._sessions() as session:
            process_at = await session.scalar(
                select(ExecutionAccountProcessStateRow.occurred_at)
                .where(
                    ExecutionAccountProcessStateRow.environment == "live",
                    ExecutionAccountProcessStateRow.account_label
                    == self._account_label,
                )
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            reconciliation = await session.scalar(
                select(AccountReconciliationRunRow)
                .where(
                    AccountReconciliationRunRow.environment == "live",
                    AccountReconciliationRunRow.account_label
                    == self._account_label,
                    AccountReconciliationRunRow.status == "ready",
                )
                .order_by(AccountReconciliationRunRow.observed_at.desc())
                .limit(1)
            )
            rows: list[AccountPositionSnapshotRow] = []
            if reconciliation is not None and reconciliation.position_count > 0:
                latest_observed_at = await session.scalar(
                    select(func.max(AccountPositionSnapshotRow.observed_at)).where(
                        AccountPositionSnapshotRow.environment == "live",
                        AccountPositionSnapshotRow.account_label
                        == self._account_label,
                    )
                )
                if latest_observed_at is None:
                    raise RuntimeError(
                        "ready account reconciliation is missing position rows"
                    )
                rows = list(
                    (
                        await session.scalars(
                            select(AccountPositionSnapshotRow).where(
                                AccountPositionSnapshotRow.environment == "live",
                                AccountPositionSnapshotRow.account_label
                                == self._account_label,
                                AccountPositionSnapshotRow.observed_at
                                == latest_observed_at,
                            )
                        )
                    ).all()
                )
            orders = list(
                (
                    await session.scalars(
                        select(ExchangeOrderRow)
                        .where(ExchangeOrderRow.run_id == self._run_id)
                        .order_by(ExchangeOrderRow.updated_at.desc())
                        .limit(1000)
                    )
                ).all()
            )
        active = [row for row in rows if row.position_amt != 0]
        managed, unmanaged = _classify_live_positions(
            active,
            orders,
            unresolved,
        )
        return (
            process_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
            managed,
            unmanaged,
        )

    async def _daily_realized_pnl(self, now: datetime) -> Decimal:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        async with self._sessions() as session:
            rows = (
                await session.scalars(
                    select(AccountFillEventRow).where(
                        AccountFillEventRow.environment == "live",
                        AccountFillEventRow.account_label == self._account_label,
                        AccountFillEventRow.trade_at >= day_start,
                    )
                )
            ).all()
        return sum((row.realized_pnl for row in rows), start=Decimal("0"))

    async def _last_entry_times(self) -> dict[str, datetime]:
        async with self._sessions() as session:
            rows = (
                await session.scalars(
                    select(ExchangeOrderRow)
                    .where(
                        ExchangeOrderRow.run_id == self._run_id,
                        ExchangeOrderRow.state == ExchangeOrderState.FILLED.value,
                    )
                    .order_by(ExchangeOrderRow.updated_at.desc())
                    .limit(500)
                )
            ).all()
        entries: dict[str, datetime] = {}
        for row in rows:
            if not row.reduce_only:
                entries.setdefault(row.symbol, row.updated_at)
        return entries

    async def _strategy_live_state(self) -> StrategyLiveState:
        async with self._sessions() as session:
            control_state = await session.scalar(
                select(LiveSessionTransitionRow.state)
                .where(
                    LiveSessionTransitionRow.session_id == self._run_id,
                    LiveSessionTransitionRow.state.in_(("live_enabled", "draining")),
                )
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
            state = await session.scalar(
                select(LiveSessionTransitionRow.state)
                .where(LiveSessionTransitionRow.session_id == self._run_id)
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
        return _resolve_strategy_live_state(control_state, state)


def _classify_live_positions(
    positions: list[AccountPositionSnapshotRow],
    orders: list[ExchangeOrderRow],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
) -> tuple[tuple[ManagedLivePosition, ...], frozenset[str]]:
    filled_orders = [
        row for row in orders if row.state == ExchangeOrderState.FILLED.value
    ]
    managed: list[ManagedLivePosition] = []
    unmanaged: set[str] = set()
    for position in positions:
        try:
            position_side = FuturesPositionSide(position.position_side)
        except ValueError:
            unmanaged.add(position.symbol)
            continue
        side = _strategy_side(position, position_side)
        opening = next(
            (
                order
                for order in filled_orders
                if not order.reduce_only
                and order.symbol == position.symbol
                and FuturesPositionSide(order.position_side) is position_side
                and _opening_order_matches_side(order.side, side)
            ),
            None,
        )
        if opening is None or position.entry_price <= 0:
            unmanaged.add(position.symbol)
            continue
        closing_filled = any(
            order.reduce_only
            and order.symbol == position.symbol
            and FuturesPositionSide(order.position_side) is position_side
            and order.updated_at >= opening.updated_at
            for order in filled_orders
        )
        active_market_exit = any(
            item.plan.symbol == position.symbol
            and item.plan.reduce_only
            and item.plan.position_side is position_side
            and item.plan.order_type == "MARKET"
            and not item.state.terminal
            for item in unresolved
        )
        active_exit_orders = [
            item
            for item in unresolved
            if item.plan.symbol == position.symbol
            and item.plan.reduce_only
            and item.plan.position_side is position_side
            and not item.state.terminal
        ]
        recovery_order = next(
            (
                item
                for item in active_exit_orders
                if item.plan.order_type == "LIMIT"
            ),
            None,
        )
        managed.append(
            ManagedLivePosition(
                symbol=position.symbol,
                side=side,
                position_side=position_side,
                quantity=abs(position.position_amt),
                entry_price=position.entry_price,
                opened_at=opening.updated_at,
                closing_order_filled=closing_filled or active_market_exit,
                recovery_order_client_id=(
                    None
                    if recovery_order is None
                    else recovery_order.plan.client_order_id
                ),
                recovery_order_created_at=(
                    None
                    if recovery_order is None
                    else recovery_order.plan.created_at
                ),
                recovery_order_plan=(
                    None if recovery_order is None else recovery_order.plan
                ),
            )
        )
    return (
        tuple(sorted(managed, key=lambda item: (item.symbol, item.position_side))),
        frozenset(unmanaged),
    )


def _strategy_side(
    position: AccountPositionSnapshotRow,
    position_side: FuturesPositionSide,
) -> StrategySide:
    if position_side is FuturesPositionSide.LONG:
        return StrategySide.LONG
    if position_side is FuturesPositionSide.SHORT:
        return StrategySide.SHORT
    return StrategySide.LONG if position.position_amt > 0 else StrategySide.SHORT


def _opening_order_matches_side(order_side: str, side: StrategySide) -> bool:
    return (order_side == "BUY") is (side is StrategySide.LONG)


def _resolve_strategy_live_state(
    control_state: str | None,
    latest_state: str | None,
) -> StrategyLiveState:
    if control_state == "draining":
        return StrategyLiveState.DRAINING
    if latest_state == "halted":
        return StrategyLiveState.HALTED
    return StrategyLiveState.ACTIVE


async def poll_live_market_states(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    max_runtime_seconds: float,
    poll_interval_seconds: float,
    batch_size: int = 500,
    cursor: RuntimeStateCursor | None = None,
) -> AsyncIterator[MarketState15s]:
    active_cursor = cursor or RuntimeStateCursor(
        bucket_start=datetime.now(tz=UTC),
        symbol="",
    )
    deadline = time.monotonic() + max_runtime_seconds
    while time.monotonic() < deadline:
        batch = await repository.load_after(
            environment=environment,
            cursor=active_cursor,
            limit=batch_size,
        )
        if not batch:
            await asyncio.sleep(poll_interval_seconds)
            continue
        for state in batch:
            yield state
            active_cursor = RuntimeStateCursor(
                bucket_start=state.bucket_start,
                symbol=state.symbol,
            )


def live_limits_from_approval(
    *,
    approval: LiveOperatorApproval,
    risk_config: RiskConfigSnapshot,
) -> tuple[Decimal, int, Decimal, Decimal]:
    return (
        min(approval.approved_notional_cap, risk_config.max_order_notional),
        min(approval.approved_max_open_positions, risk_config.max_open_positions),
        min(approval.approved_max_daily_loss, risk_config.max_daily_loss),
        risk_config.max_gross_notional,
    )
