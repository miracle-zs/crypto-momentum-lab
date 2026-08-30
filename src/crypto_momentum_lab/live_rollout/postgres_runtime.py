import asyncio
import time
from collections.abc import AsyncIterator, Mapping
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
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    StrategyLiveState,
    TradingLease,
)
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
    ExchangeFillRow,
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
    _TRADING_RULE_CACHE_SECONDS = 300
    # Account events invalidate this snapshot immediately. A short positive
    # TTL lets consecutive market buckets reuse the same account/risk view
    # instead of issuing the full nine-query context load every 15 seconds.
    _CONTEXT_CACHE_SECONDS = 30

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        execution_session_factory: async_sessionmaker[AsyncSession] | None = None,
        market_session_factory: async_sessionmaker[AsyncSession] | None = None,
        account_label: str,
        run_id: str,
        strategy_name: str,
        strategy_config_hash: str,
        git_commit_hash: str,
        migration_revision: str,
        lease_owner: str,
        approval_id: str,
    ) -> None:
        execution_sessions = execution_session_factory or session_factory
        if execution_sessions is None:
            raise ValueError(
                "session_factory or execution_session_factory is required"
            )
        self._sessions = execution_sessions
        self._market_sessions = market_session_factory or execution_sessions
        self._account_label = account_label
        self._run_id = run_id
        self._strategy_name = strategy_name
        self._strategy_config_hash = strategy_config_hash
        self._git_commit_hash = git_commit_hash
        self._migration_revision = migration_revision
        self._lease_owner = lease_owner
        self._approval_id = approval_id
        self._risk_repository = PostgresRiskRepository(execution_sessions)
        self._live_repository = PostgresLiveRolloutRepository(execution_sessions)
        self._order_repository = PostgresOrderRepository(execution_sessions)
        self._cached_bucket_start: datetime | None = None
        self._cached_context: LiveDaemonRuntimeContext | None = None
        self._cached_loaded_at: datetime | None = None
        self._cache_epoch = 0
        self._cached_rules: dict[str, SymbolTradingRules] = {}
        self._cached_rules_at: dict[str, datetime] = {}
        self._context_load_lock = asyncio.Lock()
        self._rules_load_lock = asyncio.Lock()
        self._rules_load_tasks: dict[
            str,
            asyncio.Task[SymbolTradingRules],
        ] = {}

    async def __call__(self, state: MarketState15s) -> LiveDaemonRuntimeContext:
        now = datetime.now(tz=UTC)
        cache_epoch = getattr(self, "_cache_epoch", 0)
        cached_context = getattr(self, "_cached_context", None)
        cached_bucket_start = getattr(self, "_cached_bucket_start", None)
        if cached_context is not None and _context_cache_can_be_reused(
            state=state,
            cached_bucket_start=cached_bucket_start,
            cached_loaded_at=getattr(self, "_cached_loaded_at", None),
            now=now,
            max_age_seconds=self._CONTEXT_CACHE_SECONDS,
        ):
            symbol_rules = await self._load_symbol_rules(state.symbol, now)
            current_context = self._cached_context
            current_bucket_start = self._cached_bucket_start
            if (
                getattr(self, "_cache_epoch", 0) == cache_epoch
                and current_context is not None
                and current_bucket_start is not None
                and _context_cache_can_be_reused(
                    state=state,
                    cached_bucket_start=current_bucket_start,
                    cached_loaded_at=getattr(self, "_cached_loaded_at", None),
                    now=now,
                    max_age_seconds=self._CONTEXT_CACHE_SECONDS,
                )
            ):
                return replace(
                    current_context,
                    now=now,
                    gate_context=replace(current_context.gate_context, now=now),
                    trading_rules={state.symbol: symbol_rules},
                )

        async with self._context_load_guard():
            # Another live lane may have refreshed the cache while this call
            # waited for the single-flight lock. Never issue the full account
            # query set when a newer snapshot is already available.
            now = datetime.now(tz=UTC)
            cache_epoch = getattr(self, "_cache_epoch", 0)
            cached_context = getattr(self, "_cached_context", None)
            cached_bucket_start = getattr(self, "_cached_bucket_start", None)
            if cached_context is not None and _context_cache_can_be_reused(
                state=state,
                cached_bucket_start=cached_bucket_start,
                cached_loaded_at=getattr(self, "_cached_loaded_at", None),
                now=now,
                max_age_seconds=self._CONTEXT_CACHE_SECONDS,
            ):
                symbol_rules = await self._load_symbol_rules(state.symbol, now)
                current_context = self._cached_context
                current_bucket_start = self._cached_bucket_start
                if (
                    getattr(self, "_cache_epoch", 0) == cache_epoch
                    and current_context is not None
                    and current_bucket_start is not None
                    and _context_cache_can_be_reused(
                        state=state,
                        cached_bucket_start=current_bucket_start,
                        cached_loaded_at=getattr(self, "_cached_loaded_at", None),
                        now=now,
                        max_age_seconds=self._CONTEXT_CACHE_SECONDS,
                    )
                ):
                    return replace(
                        current_context,
                        now=now,
                        gate_context=replace(
                            current_context.gate_context,
                            now=now,
                        ),
                        trading_rules={state.symbol: symbol_rules},
                    )
            return await self._load_context(state)

    def _context_load_guard(self) -> asyncio.Lock:
        lock = getattr(self, "_context_load_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._context_load_lock = lock
        return lock

    async def _load_context(
        self,
        state: MarketState15s,
    ) -> LiveDaemonRuntimeContext:
        now = datetime.now(tz=UTC)
        cached_context = self._cached_context
        if (
            self._cached_bucket_start == state.bucket_start
            and cached_context is not None
        ):
            symbol_rules = await self._load_symbol_rules(state.symbol, now)
            current_context = self._cached_context
            if (
                self._cached_bucket_start == state.bucket_start
                and current_context is not None
            ):
                return replace(
                    current_context,
                    now=now,
                    gate_context=replace(current_context.gate_context, now=now),
                    trading_rules={state.symbol: symbol_rules},
                )
            # An account event may invalidate the cache while symbol rules are
            # loading. Fall through and reload the full account/risk context;
            # returning the captured context here could make an entry decision
            # from stale positions or gate state.
        cache_epoch = getattr(self, "_cache_epoch", 0)
        approval_task = asyncio.create_task(
            self._live_repository.load_active_approval(
                account_label=self._account_label,
                strategy_name=self._strategy_name,
                now=now,
            )
        )
        risk_config_task = asyncio.create_task(
            _latest_risk_config(
                self._sessions,
                self._account_label,
            )
        )
        lease_task = asyncio.create_task(
            self._risk_repository.load_active_lease(
                "live",
                self._account_label,
                now,
            )
        )
        halts_task = asyncio.create_task(
            self._risk_repository.load_active_halts(
                "live",
                self._account_label,
            )
        )
        unresolved_and_positions_task = asyncio.create_task(
            self._load_unresolved_and_position_view()
        )
        account_state_task = asyncio.create_task(
            _latest_account_state(
                self._sessions,
                self._account_label,
            )
        )
        realized_task = asyncio.create_task(self._daily_realized_pnl(now))
        symbol_rules_task = asyncio.create_task(
            self._load_symbol_rules(state.symbol, now)
        )
        strategy_state_task = asyncio.create_task(self._strategy_live_state())
        await asyncio.gather(
            approval_task,
            risk_config_task,
            lease_task,
            halts_task,
            unresolved_and_positions_task,
            account_state_task,
            realized_task,
            symbol_rules_task,
            strategy_state_task,
        )
        approval = approval_task.result()
        risk_config = risk_config_task.result()
        lease = lease_task.result()
        halts = halts_task.result()
        unresolved_and_positions = unresolved_and_positions_task.result()
        account_state = account_state_task.result()
        realized = realized_task.result()
        symbol_rules = symbol_rules_task.result()
        strategy_state = strategy_state_task.result()
        if approval is not None and approval.approval_id != self._approval_id:
            approval = None
        (
            account_observed_at,
            position_symbols,
            unrealized,
            gross,
            managed_positions,
            unmanaged_symbols,
        ) = unresolved_and_positions[1]
        unresolved = unresolved_and_positions[0]
        rules = {state.symbol: symbol_rules}
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
            managed_positions=managed_positions,
            unmanaged_position_symbols=unmanaged_symbols,
            unresolved_orders=unresolved,
        )
        if (
            self._cache_epoch == cache_epoch
            and (
                self._cached_bucket_start is None
                or state.bucket_start >= self._cached_bucket_start
            )
        ):
            self._cached_bucket_start = state.bucket_start
            self._cached_context = context
            self._cached_loaded_at = now
        return context

    def update_lease(self, lease: TradingLease) -> None:
        """Publish a heartbeat renewal into the cached runtime context.

        Lease renewal is owned by the independent heartbeat task.  Updating
        the cache here keeps the gate on the hot market-state path consistent
        with the lease that was just committed to PostgreSQL.
        """

        if lease.owner != self._lease_owner or self._cached_context is None:
            return
        self._cached_context = replace(
            self._cached_context,
            active_lease=lease,
            gate_context=replace(
                self._cached_context.gate_context,
                active_lease=lease,
            ),
        )

    def invalidate_cache(self) -> None:
        """Force the next state to reload account and risk state.

        Trading rules are market metadata, not account/risk state.  Keeping
        their short-lived cache intact is important because account events
        can invalidate this provider several times while a delayed market
        state is still waiting to be evaluated.
        """
        self._cache_epoch = getattr(self, "_cache_epoch", 0) + 1
        self._cached_bucket_start = None
        self._cached_context = None
        self._cached_loaded_at = None

    def invalidate_trading_rules(self) -> None:
        """Force the next symbol-rule lookup to reload market metadata."""
        self._cached_rules.clear()
        self._cached_rules_at.clear()

    def _rules_load_guard(self) -> asyncio.Lock:
        lock = getattr(self, "_rules_load_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._rules_load_lock = lock
        return lock

    async def _load_symbol_rules(
        self,
        symbol: str,
        now: datetime,
    ) -> SymbolTradingRules:
        cached = self._cached_rules.get(symbol)
        cached_at = self._cached_rules_at.get(symbol)
        if (
            cached is not None
            and cached_at is not None
            and (now - cached_at).total_seconds()
            < self._TRADING_RULE_CACHE_SECONDS
        ):
            return cached

        async with self._rules_load_guard():
            # A different lane may have loaded this symbol while this call
            # waited for the rules lock.
            cached = self._cached_rules.get(symbol)
            cached_at = self._cached_rules_at.get(symbol)
            if (
                cached is not None
                and cached_at is not None
                and (now - cached_at).total_seconds()
                < self._TRADING_RULE_CACHE_SECONDS
            ):
                return cached

            tasks = getattr(self, "_rules_load_tasks", None)
            if tasks is None:
                tasks = {}
                self._rules_load_tasks = tasks
            task = tasks.get(symbol)
            if task is None:
                task = asyncio.create_task(
                    self._load_symbol_rules_uncached(symbol, now)
                )
                tasks[symbol] = task

        try:
            # One cancelled caller must not cancel the shared database load;
            # the next lane should be able to await the same task.
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self._rules_load_guard():
                    tasks = getattr(self, "_rules_load_tasks", None)
                    if tasks is not None and tasks.get(symbol) is task:
                        tasks.pop(symbol, None)

    async def _load_symbol_rules_uncached(
        self,
        symbol: str,
        now: datetime,
    ) -> SymbolTradingRules:
        market_sessions = getattr(self, "_market_sessions", None)
        if market_sessions is None:
            market_sessions = self._sessions
        loaded_rules = await _load_trading_rules(market_sessions, {symbol})
        symbol_rules = loaded_rules[symbol]
        self._cached_rules[symbol] = symbol_rules
        self._cached_rules_at[symbol] = now
        return symbol_rules

    async def _load_unresolved_and_position_view(
        self,
    ) -> tuple[
        tuple[PersistedExchangeOrder, ...],
        tuple[
            datetime | None,
            frozenset[str],
            Decimal,
            Decimal,
            tuple[ManagedLivePosition, ...],
            frozenset[str],
        ],
    ]:
        unresolved = await self._order_repository.load_unresolved_orders(
            self._run_id
        )
        return unresolved, await self._account_position_view(unresolved)

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
            entry_exchange_order_ids = tuple(
                sorted(
                    {
                        row.exchange_order_id
                        for row in orders
                        if row.exchange_order_id is not None
                        and not row.reduce_only
                    }
                )
            )
            entry_client_order_ids = tuple(
                sorted(
                    {
                        row.client_order_id
                        for row in orders
                        if not row.reduce_only
                    }
                )
            )
            entry_fill_times: dict[str, datetime] = {}
            if entry_exchange_order_ids:
                account_fills = (
                    await session.scalars(
                        select(AccountFillEventRow).where(
                            AccountFillEventRow.environment == "live",
                            AccountFillEventRow.account_label
                            == self._account_label,
                            AccountFillEventRow.order_id.in_(
                                entry_exchange_order_ids
                            ),
                        )
                    )
                ).all()
                for account_fill in account_fills:
                    _record_earliest_fill(
                        entry_fill_times,
                        account_fill.order_id,
                        account_fill.trade_at,
                    )
            if entry_client_order_ids:
                exchange_fills = (
                    await session.scalars(
                        select(ExchangeFillRow).where(
                            ExchangeFillRow.client_order_id.in_(
                                entry_client_order_ids
                            )
                        )
                    )
                ).all()
                for exchange_fill in exchange_fills:
                    _record_earliest_fill(
                        entry_fill_times,
                        exchange_fill.client_order_id,
                        exchange_fill.filled_at,
                    )
        active = [row for row in rows if row.position_amt != 0]
        managed, unmanaged = _classify_live_positions(
            active,
            orders,
            unresolved,
            entry_fill_times=entry_fill_times,
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
            realized = await session.scalar(
                select(
                    func.coalesce(
                        func.sum(AccountFillEventRow.realized_pnl),
                        Decimal("0"),
                    )
                ).where(
                    AccountFillEventRow.environment == "live",
                    AccountFillEventRow.account_label == self._account_label,
                    AccountFillEventRow.trade_at >= day_start,
                )
            )
        return Decimal("0") if realized is None else realized

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
    *,
    entry_fill_times: Mapping[str, datetime] | None = None,
) -> tuple[tuple[ManagedLivePosition, ...], frozenset[str]]:
    fill_times = entry_fill_times or {}
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
        opening_candidates = [
            order
            for order in orders
            if not order.reduce_only
            and order.symbol == position.symbol
            and FuturesPositionSide(order.position_side) is position_side
            and _opening_order_matches_side(order.side, side)
            and (
                order.state == ExchangeOrderState.FILLED.value
                or _entry_fill_at(order, fill_times) is not None
                or (
                    getattr(order, "executed_quantity", Decimal("0"))
                    or Decimal("0")
                )
                > 0
            )
        ]
        opening = max(
            opening_candidates,
            key=lambda order: (
                _entry_fill_at(order, fill_times) or order.updated_at,
                order.updated_at,
                order.created_at,
            ),
            default=None,
        )
        if opening is None or position.entry_price <= 0:
            unmanaged.add(position.symbol)
            continue
        opening_fill_at = _entry_fill_at(opening, fill_times)
        # Associate a close with the current position episode by the time the
        # order actually filled when that timestamp is available.  A pending
        # limit add-on may be created before an older exit fills; comparing
        # only order.created_at would incorrectly attach that exit to the
        # later position episode.  The created-at fallback preserves the
        # legacy behavior for rows that predate fill-time persistence.
        closing_filled = any(
            order.reduce_only
            and order.symbol == position.symbol
            and FuturesPositionSide(order.position_side) is position_side
            and (
                (
                    opening_fill_at is not None
                    and order.updated_at >= opening_fill_at
                )
                or (
                    opening_fill_at is None
                    and order.created_at >= opening.created_at
                )
            )
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
                opened_at=_entry_fill_at(opening, fill_times) or opening.updated_at,
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


def _entry_fill_at(
    order: ExchangeOrderRow | None,
    fill_times: Mapping[str, datetime],
) -> datetime | None:
    if order is None:
        return None
    for identifier in (
        getattr(order, "exchange_order_id", None),
        getattr(order, "client_order_id", None),
    ):
        if identifier is not None:
            fill_at = fill_times.get(identifier)
            if fill_at is not None:
                return fill_at
    return None


def _record_earliest_fill(
    fill_times: dict[str, datetime],
    identifier: str | None,
    filled_at: datetime,
) -> None:
    if identifier is None:
        return
    previous = fill_times.get(identifier)
    if previous is None or filled_at < previous:
        fill_times[identifier] = filled_at


def _context_cache_can_be_reused(
    *,
    state: MarketState15s,
    cached_bucket_start: datetime | None,
    cached_loaded_at: datetime | None,
    now: datetime,
    max_age_seconds: int,
) -> bool:
    if cached_bucket_start is not None and state.bucket_start <= cached_bucket_start:
        return True
    if cached_loaded_at is None:
        return False
    age_seconds = (now - cached_loaded_at).total_seconds()
    return 0 <= age_seconds < max_age_seconds


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
    max_state_lag_seconds: float = 45.0,
    lag_check_interval_seconds: float = 1.0,
) -> AsyncIterator[MarketState15s]:
    if max_state_lag_seconds <= 0:
        raise ValueError("max_state_lag_seconds must be positive")
    if lag_check_interval_seconds <= 0:
        raise ValueError("lag_check_interval_seconds must be positive")
    active_cursor = cursor or RuntimeStateCursor(
        bucket_start=datetime.now(tz=UTC),
        symbol="",
    )
    deadline = time.monotonic() + max_runtime_seconds
    next_lag_check_at = 0.0
    while time.monotonic() < deadline:
        monotonic_now = time.monotonic()
        if monotonic_now >= next_lag_check_at:
            latest_bucket = await repository.load_latest_bucket(
                environment=environment
            )
            if (
                latest_bucket is not None
                and active_cursor.bucket_start is not None
                and (
                    latest_bucket - active_cursor.bucket_start
                ).total_seconds()
                > max_state_lag_seconds
            ):
                # A live worker must never submit an entry for an old signal.
                # Reposition just before the newest closed bucket; the daemon
                # will reset per-symbol strategy state across this gap while
                # still evaluating exits against the newest market state.
                active_cursor = RuntimeStateCursor(
                    bucket_start=latest_bucket - timedelta(microseconds=1),
                    symbol="",
                )
            next_lag_check_at = monotonic_now + lag_check_interval_seconds
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
) -> tuple[Decimal | None, int | None, Decimal | None, Decimal | None]:
    return (
        _minimum_optional_decimal_limit(
            approval.approved_notional_cap,
            risk_config.max_order_notional,
        ),
        _minimum_optional_integer_limit(
            approval.approved_max_open_positions,
            risk_config.max_open_positions,
        ),
        _minimum_optional_decimal_limit(
            approval.approved_max_daily_loss,
            risk_config.max_daily_loss,
        ),
        risk_config.max_gross_notional,
    )


def _minimum_optional_decimal_limit(
    left: Decimal | None,
    right: Decimal | None,
) -> Decimal | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _minimum_optional_integer_limit(
    left: int | None,
    right: int | None,
) -> int | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)
