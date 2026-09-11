import asyncio
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
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
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.exits import (
    ManagedLivePosition,
    ManagedLivePositionBatch,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_account_state as _latest_account_state,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_risk_config as _latest_risk_config,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_trading_rules as _load_trading_rules,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class _OrderIdentityMetadata:
    events_by_client_order_id: Mapping[
        str,
        tuple[ExchangeOrderEventRow, ...],
    ]
    account_fills: tuple[AccountFillEventRow, ...]


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
        self._realtime_account_snapshot: AccountSnapshot | None = None
        self._realtime_account_state: ExecutionAccountStatus | None = None
        self._realtime_account_sequence = 0
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
        """Load a context and refuse to return one invalidated in-flight.

        Account events and lease heartbeats can invalidate the provider while
        the parallel database reads below are waiting.  A stale context is
        unsafe for both entry and exit decisions, so retry a bounded number of
        times and fail closed if the inputs never become stable.
        """
        for _attempt in range(3):
            context = await self._load_context_once(state)
            if self.is_context_current(context):
                return context
        raise RuntimeError("live context changed during load")

    async def _load_context_once(
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
        realtime_account_snapshot = getattr(
            self,
            "_realtime_account_snapshot",
            None,
        )
        realtime_account_state = getattr(
            self,
            "_realtime_account_state",
            None,
        )
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
        if realtime_account_snapshot is None:
            unresolved_and_positions_task = asyncio.create_task(
                self._load_unresolved_and_position_view()
            )
        else:
            unresolved_and_positions_task = asyncio.create_task(
                self._load_unresolved_and_position_view(
                    account_snapshot=realtime_account_snapshot,
                )
            )
        account_state_task: asyncio.Task[ExecutionAccountStatus] | None = None
        if realtime_account_state is None:
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
        context_tasks: list[asyncio.Task[object]] = [
            approval_task,
            risk_config_task,
            lease_task,
            halts_task,
            unresolved_and_positions_task,
            realized_task,
            symbol_rules_task,
            strategy_state_task,
        ]
        if account_state_task is not None:
            context_tasks.append(account_state_task)
        await asyncio.gather(*context_tasks)
        approval = approval_task.result()
        risk_config = risk_config_task.result()
        lease = lease_task.result()
        halts = halts_task.result()
        unresolved_and_positions = unresolved_and_positions_task.result()
        if realtime_account_state is not None:
            account_state = realtime_account_state
        else:
            assert account_state_task is not None
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
            pending_symbols,
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
            pending_position_symbols=pending_symbols,
            unmanaged_position_symbols=unmanaged_symbols,
            unresolved_orders=unresolved,
            account_snapshot=realtime_account_snapshot,
            account_snapshot_version=(
                getattr(self, "_realtime_account_sequence", 0)
                if realtime_account_snapshot is not None
                else None
            ),
            context_epoch=cache_epoch,
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

    def is_context_current(self, context: LiveDaemonRuntimeContext) -> bool:
        """Return whether a context still matches the live provider inputs."""
        context_epoch = context.context_epoch
        current_epoch = getattr(self, "_cache_epoch", 0)
        if context_epoch is not None and context_epoch != current_epoch:
            return False
        if context.account_snapshot is not None:
            return context.account_snapshot_version == getattr(
                self,
                "_realtime_account_sequence",
                0,
            )
        return True

    def update_account_snapshot(
        self,
        snapshot: AccountSnapshot,
        *,
        sequence: int,
        account_state: ExecutionAccountStatus,
    ) -> None:
        """Publish the latest Hub snapshot into the live context seam.

        The snapshot is already merged by the execution-account daemon.  This
        method only validates its scope, swaps the in-memory view, and
        invalidates the context cache; it performs no database I/O.
        """
        if snapshot.config.environment != "live":
            raise ValueError("live account snapshot must use live environment")
        if snapshot.config.account_label != self._account_label:
            raise ValueError(
                "account snapshot account label does not match live context"
            )
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence <= 0
        ):
            raise ValueError("account snapshot sequence must be positive")
        if not isinstance(account_state, ExecutionAccountStatus):
            raise TypeError("account_state must be an ExecutionAccountStatus")
        self._realtime_account_snapshot = snapshot
        self._realtime_account_state = account_state
        self._realtime_account_sequence = sequence
        self.invalidate_cache()

    def invalidate_account_snapshot(self) -> None:
        """Drop a stale Hub projection while a full recovery is in flight."""
        self._realtime_account_snapshot = None
        self._realtime_account_state = None
        self._realtime_account_sequence = 0
        self.invalidate_cache()

    def update_lease(self, lease: TradingLease) -> None:
        """Publish a heartbeat renewal into the cached runtime context.

        Lease renewal is owned by the independent heartbeat task.  Updating
        the cache here keeps the gate on the hot market-state path consistent
        with the lease that was just committed to PostgreSQL.
        """

        if lease.owner != self._lease_owner:
            return
        # Invalidate in-flight loads as well as the cached object.  Updating
        # only ``_cached_context`` allows a load that captured an older lease
        # to repopulate the cache after this callback returns.
        self.invalidate_cache()

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
        *,
        account_snapshot: AccountSnapshot | None = None,
    ) -> tuple[
        tuple[PersistedExchangeOrder, ...],
        tuple[
            datetime | None,
            frozenset[str],
            Decimal,
            Decimal,
            tuple[ManagedLivePosition, ...],
            frozenset[str],
            frozenset[str],
        ],
    ]:
        unresolved = await self._order_repository.load_unresolved_orders(
            self._run_id
        )
        return unresolved, await self._account_position_view(
            unresolved,
            account_snapshot=account_snapshot,
        )

    async def _account_position_view(
        self,
        unresolved: tuple[PersistedExchangeOrder, ...],
        *,
        account_snapshot: AccountSnapshot | None = None,
    ) -> tuple[
        datetime | None,
        frozenset[str],
        Decimal,
        Decimal,
        tuple[ManagedLivePosition, ...],
        frozenset[str],
        frozenset[str],
    ]:
        if account_snapshot is not None:
            return await self._account_position_view_from_snapshot(
                unresolved,
                account_snapshot,
            )
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
            active = [row for row in rows if row.position_amt != 0]
            orders: list[ExchangeOrderRow] = []
            entry_fill_times: dict[str, datetime] = {}
            entry_fill_values: dict[str, tuple[Decimal, Decimal]] = {}
            account_fill_quantities: dict[str, Decimal] = {}
            order_identity_events: Mapping[
                str,
                tuple[ExchangeOrderEventRow, ...],
            ] = {}
            if active:
                active_symbols = tuple(sorted({row.symbol for row in active}))
                orders = list(
                    (
                        await session.scalars(
                            select(ExchangeOrderRow)
                            .where(
                                ExchangeOrderRow.run_id == self._run_id,
                                ExchangeOrderRow.symbol.in_(active_symbols),
                            )
                            .order_by(ExchangeOrderRow.updated_at.desc())
                            .limit(1000)
                        )
                    ).all()
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
                order_identity_metadata = await _load_order_identity_metadata(
                    session,
                    orders,
                    account_label=self._account_label,
                )
                order_identity_events = (
                    order_identity_metadata.events_by_client_order_id
                )
                for account_fill in order_identity_metadata.account_fills:
                    _record_earliest_fill(
                        entry_fill_times,
                        account_fill.order_id,
                        account_fill.trade_at,
                    )
                    _record_fill_value(
                        entry_fill_values,
                        account_fill.order_id,
                        account_fill.quantity,
                        account_fill.price,
                    )
                    _record_fill_quantity(
                        account_fill_quantities,
                        account_fill.order_id,
                        account_fill.quantity,
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
                        _record_fill_value(
                            entry_fill_values,
                            exchange_fill.client_order_id,
                            exchange_fill.quantity,
                            exchange_fill.price,
                        )
        active = [row for row in rows if row.position_amt != 0]
        exit_batch_ids, legacy_exit_order_ids = (
            await _load_exit_batch_bindings(self._sessions, orders)
        )
        managed, pending, unmanaged = _classify_live_positions_detailed(
            active,
            orders,
            unresolved,
            entry_fill_times=entry_fill_times,
            entry_fill_prices=_average_fill_prices(entry_fill_values),
            exit_batch_ids=exit_batch_ids,
            legacy_exit_order_ids=legacy_exit_order_ids,
            order_identity_events=order_identity_events,
            account_fill_quantities=account_fill_quantities,
        )
        return (
            process_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
            managed,
            pending,
            unmanaged,
        )

    async def _account_position_view_from_snapshot(
        self,
        unresolved: tuple[PersistedExchangeOrder, ...],
        snapshot: AccountSnapshot,
    ) -> tuple[
        datetime | None,
        frozenset[str],
        Decimal,
        Decimal,
        tuple[ManagedLivePosition, ...],
        frozenset[str],
        frozenset[str],
    ]:
        """Build the hot account view from the Hub's complete snapshot.

        The only remaining database reads are execution ownership/fill
        metadata needed to distinguish managed positions from external ones.
        Account balances, positions, and account process state are all taken
        from ``snapshot``.
        """
        rows: list[AccountPositionSnapshot] = list(snapshot.positions)
        active = [row for row in rows if row.position_amt != 0]
        orders: list[ExchangeOrderRow] = []
        entry_fill_times: dict[str, datetime] = {}
        entry_fill_values: dict[str, tuple[Decimal, Decimal]] = {}
        account_fill_quantities: dict[str, Decimal] = {}
        order_identity_events: Mapping[
            str,
            tuple[ExchangeOrderEventRow, ...],
        ] = {}
        if active:
            async with self._sessions() as session:
                active_symbols = tuple(sorted({row.symbol for row in active}))
                orders = list(
                    (
                        await session.scalars(
                            select(ExchangeOrderRow)
                            .where(
                                ExchangeOrderRow.run_id == self._run_id,
                                ExchangeOrderRow.symbol.in_(active_symbols),
                            )
                            .order_by(ExchangeOrderRow.updated_at.desc())
                            .limit(1000)
                        )
                    ).all()
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
                order_identity_metadata = await _load_order_identity_metadata(
                    session,
                    orders,
                    account_label=self._account_label,
                )
                order_identity_events = (
                    order_identity_metadata.events_by_client_order_id
                )
                for account_fill in order_identity_metadata.account_fills:
                    _record_earliest_fill(
                        entry_fill_times,
                        account_fill.order_id,
                        account_fill.trade_at,
                    )
                    _record_fill_value(
                        entry_fill_values,
                        account_fill.order_id,
                        account_fill.quantity,
                        account_fill.price,
                    )
                    _record_fill_quantity(
                        account_fill_quantities,
                        account_fill.order_id,
                        account_fill.quantity,
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
                        _record_fill_value(
                            entry_fill_values,
                            exchange_fill.client_order_id,
                            exchange_fill.quantity,
                            exchange_fill.price,
                        )
        exit_batch_ids, legacy_exit_order_ids = (
            await _load_exit_batch_bindings(self._sessions, orders)
        )
        managed, pending, unmanaged = _classify_live_positions_detailed(
            active,
            orders,
            unresolved,
            entry_fill_times=entry_fill_times,
            entry_fill_prices=_average_fill_prices(entry_fill_values),
            exit_batch_ids=exit_batch_ids,
            legacy_exit_order_ids=legacy_exit_order_ids,
            order_identity_events=order_identity_events,
            account_fill_quantities=account_fill_quantities,
        )
        return (
            snapshot.config.observed_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
            managed,
            pending,
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


async def _load_order_identity_metadata(
    session: AsyncSession,
    orders: Sequence[ExchangeOrderRow],
    *,
    account_label: str,
) -> _OrderIdentityMetadata:
    """Load the event/fill evidence needed to split legacy order attempts.

    ``exchange_orders`` is keyed by client order ID, but older execution paths
    reused that ID for more than one exchange order.  The event journal keeps
    the exchange identities, and account fills provide the authoritative
    quantity for each identity.  Runtime position reconstruction must use both
    before it decides where an exit boundary belongs.
    """

    client_order_ids = tuple(
        sorted(
            {
                order.client_order_id
                for order in orders
                if order.client_order_id
            }
        )
    )
    if not client_order_ids:
        return _OrderIdentityMetadata({}, ())
    event_rows = tuple(
        (
            await session.scalars(
                select(ExchangeOrderEventRow).where(
                    ExchangeOrderEventRow.client_order_id.in_(
                        client_order_ids
                    )
                )
            )
        ).all()
    )
    events_by_client: dict[str, list[ExchangeOrderEventRow]] = {}
    exchange_order_ids: set[str] = set()
    for event in event_rows:
        events_by_client.setdefault(event.client_order_id, []).append(event)
        if event.exchange_order_id:
            exchange_order_ids.add(event.exchange_order_id)
    row_exchange_order_ids = {
        order.exchange_order_id
        for order in orders
        if order.exchange_order_id
    }
    exchange_order_ids.update(row_exchange_order_ids)
    if not exchange_order_ids:
        return _OrderIdentityMetadata(
            {key: tuple(value) for key, value in events_by_client.items()},
            (),
        )
    account_fills = tuple(
        (
            await session.scalars(
                select(AccountFillEventRow).where(
                    AccountFillEventRow.environment == "live",
                    AccountFillEventRow.account_label == account_label,
                    AccountFillEventRow.order_id.in_(tuple(exchange_order_ids)),
                )
            )
        ).all()
    )
    return _OrderIdentityMetadata(
        {key: tuple(value) for key, value in events_by_client.items()},
        account_fills,
    )


@dataclass(frozen=True, slots=True)
class _PositionOrder:
    symbol: str
    position_side: FuturesPositionSide
    side: str
    reduce_only: bool
    order_type: str
    quantity: Decimal
    executed_quantity: Decimal
    state: ExchangeOrderState
    client_order_id: str | None
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime
    price: Decimal | None
    plan: OrderExecutionPlan | None = None
    exit_batch_id: str | None = None
    legacy_exit_attribution: bool = False


@dataclass(slots=True)
class _PositionBatchAccumulator:
    batch_id: str
    opened_at: datetime
    entry_quantity: Decimal
    entry_notional: Decimal
    exit_order_submitted_at: datetime | None = None
    exit_orders: list[_PositionOrder] = field(default_factory=list)
    exit_filled_quantity: Decimal = Decimal("0")
    legacy_exit_attribution: bool = False


_EXIT_SUBMITTED_STATES = frozenset(
    {
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.PARTIALLY_FILLED,
        ExchangeOrderState.FILLED,
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.ABSENT_RECONCILED,
        ExchangeOrderState.EXPIRED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }
)
_PENDING_ENTRY_STATES = frozenset(
    {
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }
)
_PENDING_POSITION_MAX_AGE_SECONDS = 60


def _classify_live_positions(
    positions: Sequence[AccountPositionSnapshot | AccountPositionSnapshotRow],
    orders: list[ExchangeOrderRow],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
    *,
    entry_fill_times: Mapping[str, datetime] | None = None,
    entry_fill_prices: Mapping[str, Decimal] | None = None,
    exit_batch_ids: Mapping[str, str] | None = None,
    legacy_exit_order_ids: frozenset[str] = frozenset(),
    order_identity_events: Mapping[
        str,
        Sequence[ExchangeOrderEventRow],
    ] | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
) -> tuple[tuple[ManagedLivePosition, ...], frozenset[str]]:
    """Keep the historical two-value classification API for callers/tests."""
    managed, _pending, unmanaged = _classify_live_positions_detailed(
        positions,
        orders,
        unresolved,
        entry_fill_times=entry_fill_times,
        entry_fill_prices=entry_fill_prices,
        exit_batch_ids=exit_batch_ids,
        legacy_exit_order_ids=legacy_exit_order_ids,
        order_identity_events=order_identity_events,
        account_fill_quantities=account_fill_quantities,
    )
    return managed, unmanaged


def _classify_live_positions_detailed(
    positions: Sequence[AccountPositionSnapshot | AccountPositionSnapshotRow],
    orders: list[ExchangeOrderRow],
    unresolved: tuple[PersistedExchangeOrder, ...] = (),
    *,
    entry_fill_times: Mapping[str, datetime] | None = None,
    entry_fill_prices: Mapping[str, Decimal] | None = None,
    exit_batch_ids: Mapping[str, str] | None = None,
    legacy_exit_order_ids: frozenset[str] = frozenset(),
    order_identity_events: Mapping[
        str,
        Sequence[ExchangeOrderEventRow],
    ] | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
) -> tuple[
    tuple[ManagedLivePosition, ...],
    frozenset[str],
    frozenset[str],
]:
    fill_times = entry_fill_times or {}
    fill_prices = entry_fill_prices or {}
    identity_events = order_identity_events or {}
    fill_quantities = account_fill_quantities or {}
    ambiguous_identity_ids = frozenset(
        client_order_id
        for client_order_id, events in identity_events.items()
        if _legacy_order_identity_is_ambiguous(events)
    )
    unresolved_identity_ids = frozenset(
        client_order_id
        for client_order_id in ambiguous_identity_ids
        if not _legacy_order_identity_is_reconstructible(
            identity_events[client_order_id],
            fill_quantities,
        )
    )
    if ambiguous_identity_ids:
        log.warning(
            "live_legacy_order_identity_conflict",
            ambiguous_client_order_ids=sorted(ambiguous_identity_ids),
            reconstructed_client_order_ids=sorted(
                ambiguous_identity_ids - unresolved_identity_ids
            ),
            unresolved_client_order_ids=sorted(unresolved_identity_ids),
        )
    position_orders = _normalise_position_orders(
        orders,
        unresolved,
        order_identity_events=identity_events,
        account_fill_quantities=fill_quantities,
    )
    if exit_batch_ids or legacy_exit_order_ids:
        position_orders = tuple(
            replace(
                order,
                exit_batch_id=(
                    None
                    if exit_batch_ids is None
                    else exit_batch_ids.get(order.client_order_id or "")
                ),
                legacy_exit_attribution=(
                    order.client_order_id in legacy_exit_order_ids
                ),
            )
            for order in position_orders
        )
    managed: list[ManagedLivePosition] = []
    pending: set[str] = set()
    unmanaged: set[str] = set()
    for position in positions:
        try:
            position_side = FuturesPositionSide(position.position_side)
        except (TypeError, ValueError):
            unmanaged.add(position.symbol)
            continue
        side = _strategy_side(position, position_side)
        matching_orders = [
            order
            for order in position_orders
            if order.symbol == position.symbol
            and order.position_side is position_side
        ]
        if any(
            order.client_order_id in unresolved_identity_ids
            for order in matching_orders
        ):
            log.critical(
                "live_position_batch_attribution_blocked",
                symbol=position.symbol,
                account_label=getattr(position, "account_label", None),
                ambiguous_client_order_ids=sorted(
                    {
                        order.client_order_id
                        for order in matching_orders
                        if order.client_order_id in unresolved_identity_ids
                    }
                ),
                reason="legacy_order_identity_not_reconstructible",
            )
            unmanaged.add(position.symbol)
            continue
        opening_candidates = [
            order
            for order in matching_orders
            if not order.reduce_only
            and _opening_order_matches_side(order.side, side)
            and _is_entry_fill_observed(order, fill_times)
        ]
        opening = max(
            opening_candidates,
            key=lambda order: (
                _order_entry_time(order, fill_times),
                order.updated_at,
                order.created_at,
            ),
            default=None,
        )
        if opening is None or position.entry_price <= 0:
            if _has_recent_pending_entry_order(
                position,
                matching_orders,
                fill_times,
                side=side,
            ):
                pending.add(position.symbol)
                continue
            unmanaged.add(position.symbol)
            continue
        opened_at = _order_entry_time(opening, fill_times)
        reduce_only_orders = [
            order
            for order in matching_orders
            if order.reduce_only
            and not _opening_order_matches_side(order.side, side)
        ]
        closing_filled_quantity = sum(
            (
                _exit_fill_quantity(order)
                for order in reduce_only_orders
                if order.created_at >= opened_at
            ),
            start=Decimal("0"),
        )
        # The account snapshot can still show a position briefly after a full
        # reduce-only fill.  Use strict equality here: a larger filled amount
        # can belong to a closed add-on lot while an older lot remains open.
        closing_filled = closing_filled_quantity == abs(position.position_amt)
        batches = _build_position_batches(
            position=position,
            side=side,
            position_side=position_side,
            matching_orders=matching_orders,
            fill_times=fill_times,
            fill_prices=fill_prices,
        )
        if not batches and not closing_filled:
            # The account snapshot can arrive before the new entry's order
            # state/fill metadata.  In that window the only confirmed batch
            # may be an older batch whose reduce-only exit already consumed
            # it.  Falling back to that accumulator would assign the current
            # position quantity and the old entry timestamp to the new lot,
            # which can trigger an immediate candle-timeout exit.  Keep the
            # symbol fail-closed until the next reconciliation observes the
            # new entry fill instead of inventing a batch boundary.
            if _has_recent_pending_entry_order(
                position,
                matching_orders,
                fill_times,
                side=side,
            ):
                pending.add(position.symbol)
                continue
            unmanaged.add(position.symbol)
            continue
        aggregate_opened_at = max(
            (batch.opened_at for batch in batches),
            default=opened_at,
        )
        latest_recovery = max(
            (
                batch
                for batch in batches
                if batch.recovery_order_plan is not None
            ),
            key=lambda batch: batch.recovery_order_plan.created_at
            if batch.recovery_order_plan is not None
            else batch.opened_at,
            default=None,
        )
        # Once multiple residual batches exist, the aggregate flag must stay
        # open so the exit manager can evaluate the batches independently.
        # A market order that is active for one batch is carried on that batch
        # view instead of suppressing every batch in the symbol aggregate.
        aggregate_closing_filled = closing_filled and not batches
        managed.append(
            ManagedLivePosition(
                symbol=position.symbol,
                side=side,
                position_side=position_side,
                quantity=abs(position.position_amt),
                entry_price=position.entry_price,
                opened_at=aggregate_opened_at,
                closing_order_filled=aggregate_closing_filled,
                recovery_order_client_id=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_client_id
                ),
                recovery_exit_started_at=(
                    None
                    if latest_recovery is None
                    else latest_recovery.exit_order_submitted_at
                ),
                recovery_order_created_at=(
                    None
                    if latest_recovery is None
                    or latest_recovery.recovery_order_plan is None
                    else latest_recovery.recovery_order_plan.created_at
                ),
                recovery_order_plan=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_plan
                ),
                recovery_order_remaining_quantity=(
                    None
                    if latest_recovery is None
                    else latest_recovery.recovery_order_remaining_quantity
                ),
                batches=batches,
            )
        )
    return (
        tuple(sorted(managed, key=lambda item: (item.symbol, item.position_side))),
        frozenset(pending),
        frozenset(unmanaged),
    )


def _has_recent_pending_entry_order(
    position: AccountPositionSnapshot | AccountPositionSnapshotRow,
    matching_orders: Sequence[_PositionOrder],
    fill_times: Mapping[str, datetime],
    *,
    side: StrategySide,
) -> bool:
    """Identify a bounded order-to-position visibility race.

    An account event can publish a new position before the order state or
    fill ledger transaction is visible to the strategy runtime. Only a
    recent, non-terminal entry order from this run qualifies as pending;
    unknown positions and stale orders remain fail-closed as unmanaged.
    """
    observed_at = getattr(position, "observed_at", None)
    if not isinstance(observed_at, datetime):
        return False
    for order in matching_orders:
        if (
            order.reduce_only
            or not _opening_order_matches_side(order.side, side)
            or order.state not in _PENDING_ENTRY_STATES
            or _is_entry_fill_observed(order, fill_times)
        ):
            continue
        try:
            pending_since = min(order.created_at, order.updated_at)
            age_seconds = (observed_at - pending_since).total_seconds()
        except TypeError:
            return False
        if 0 <= age_seconds <= _PENDING_POSITION_MAX_AGE_SECONDS:
            return True
    return False


async def _load_exit_batch_ids(
    sessions: async_sessionmaker[AsyncSession],
    orders: Sequence[ExchangeOrderRow],
) -> dict[str, str]:
    bindings, _legacy_order_ids = await _load_exit_batch_bindings(
        sessions,
        orders,
    )
    return bindings


async def _load_exit_batch_bindings(
    sessions: async_sessionmaker[AsyncSession],
    orders: Sequence[ExchangeOrderRow],
) -> tuple[dict[str, str], frozenset[str]]:
    intent_clients = {
        order.intent_id: order.client_order_id
        for order in orders if order.reduce_only
    }
    if not intent_clients:
        return {}, frozenset()
    legacy_order_ids = set(intent_clients.values())
    async with sessions() as session:
        rows = (await session.execute(
            select(OrderIntentExecutionRow.intent_id, OrderIntentExecutionRow.details)
            .where(OrderIntentExecutionRow.intent_id.in_(tuple(intent_clients)))
        )).all()
    result: dict[str, str] = {}
    for intent_id, details in rows:
        features = details.get("features", {}) if isinstance(details, dict) else {}
        batch_id = features.get("batch_id") if isinstance(features, dict) else None
        if isinstance(batch_id, str) and batch_id:
            client_order_id = intent_clients[intent_id]
            result[client_order_id] = batch_id
            legacy_order_ids.discard(client_order_id)
    return result, frozenset(legacy_order_ids)


def _normalise_position_orders(
    orders: Sequence[ExchangeOrderRow],
    unresolved: Sequence[PersistedExchangeOrder],
    *,
    order_identity_events: Mapping[
        str,
        Sequence[ExchangeOrderEventRow],
    ] | None = None,
    account_fill_quantities: Mapping[str, Decimal] | None = None,
) -> tuple[_PositionOrder, ...]:
    unresolved_by_client_id = {
        item.plan.client_order_id: item for item in unresolved
    }
    normalised: list[_PositionOrder] = []
    seen_keys: set[str] = set()
    for row in orders:
        client_order_id = _optional_text(getattr(row, "client_order_id", None))
        persisted = (
            None
            if client_order_id is None
            else unresolved_by_client_id.get(client_order_id)
        )
        legacy_events = (
            ()
            if client_order_id is None or order_identity_events is None
            else order_identity_events.get(client_order_id, ())
        )
        expanded_orders = _expand_legacy_order_row(
            row,
            plan=None if persisted is None else persisted.plan,
            fallback_state=None if persisted is None else persisted.state,
            fallback_executed_quantity=(
                None if persisted is None else persisted.executed_quantity
            ),
            events=legacy_events,
            account_fill_quantities=account_fill_quantities or {},
        )
        if expanded_orders is not None:
            for expanded in expanded_orders:
                key = _position_order_key(expanded)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                normalised.append(expanded)
            continue
        order = _position_order_from_row(
            row,
            plan=None if persisted is None else persisted.plan,
            fallback_state=None if persisted is None else persisted.state,
            fallback_executed_quantity=(
                None if persisted is None else persisted.executed_quantity
            ),
        )
        if order is None:
            continue
        key = _position_order_key(order)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        normalised.append(order)
    for item in unresolved:
        order = _position_order_from_plan(item)
        key = _position_order_key(order)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        normalised.append(order)
    return tuple(normalised)


def _legacy_order_identity_is_ambiguous(
    events: Sequence[ExchangeOrderEventRow],
) -> bool:
    exchange_order_ids = {
        event.exchange_order_id
        for event in events
        if event.exchange_order_id
    }
    return len(exchange_order_ids) > 1


def _legacy_order_identity_is_reconstructible(
    events: Sequence[ExchangeOrderEventRow],
    account_fill_quantities: Mapping[str, Decimal],
) -> bool:
    exchange_order_ids = {
        event.exchange_order_id
        for event in events
        if event.exchange_order_id
    }
    by_exchange_order_id = {
        exchange_order_id: Decimal("0")
        for exchange_order_id in exchange_order_ids
    }
    for event in events:
        exchange_order_id = event.exchange_order_id
        if not exchange_order_id:
            continue
        event_quantity = _event_executed_quantity(event)
        if event_quantity is not None:
            by_exchange_order_id[exchange_order_id] = max(
                by_exchange_order_id.get(exchange_order_id, Decimal("0")),
                event_quantity,
            )
    for exchange_order_id, quantity in account_fill_quantities.items():
        if exchange_order_id in by_exchange_order_id:
            by_exchange_order_id[exchange_order_id] = max(
                by_exchange_order_id[exchange_order_id],
                _decimal_or_zero(quantity),
            )
    return bool(by_exchange_order_id) and all(
        quantity > 0 for quantity in by_exchange_order_id.values()
    )


def _expand_legacy_order_row(
    row: ExchangeOrderRow,
    *,
    plan: OrderExecutionPlan | None,
    fallback_state: ExchangeOrderState | None,
    fallback_executed_quantity: Decimal | None,
    events: Sequence[ExchangeOrderEventRow],
    account_fill_quantities: Mapping[str, Decimal],
) -> tuple[_PositionOrder, ...] | None:
    if not _legacy_order_identity_is_ambiguous(events):
        return None
    if not _legacy_order_identity_is_reconstructible(
        events,
        account_fill_quantities,
    ):
        return None
    base = _position_order_from_row(
        row,
        plan=plan,
        fallback_state=fallback_state,
        fallback_executed_quantity=fallback_executed_quantity,
    )
    if base is None:
        return None
    exchange_order_ids = sorted(
        {
            event.exchange_order_id
            for event in events
            if event.exchange_order_id
        }
    )
    expanded: list[_PositionOrder] = []
    for exchange_order_id in exchange_order_ids:
        identity_events = [
            event
            for event in events
            if event.exchange_order_id == exchange_order_id
        ]
        event_quantity = max(
            (
                quantity
                for event in identity_events
                if (quantity := _event_executed_quantity(event)) is not None
            ),
            default=Decimal("0"),
        )
        fill_quantity = _decimal_or_zero(
            account_fill_quantities.get(exchange_order_id)
        )
        quantity = max(event_quantity, fill_quantity)
        if quantity <= 0:
            return None
        created_at = min(event.occurred_at for event in identity_events)
        updated_at = max(event.occurred_at for event in identity_events)
        latest_event = max(identity_events, key=lambda event: event.occurred_at)
        expanded.append(
            replace(
                base,
                exchange_order_id=exchange_order_id,
                quantity=quantity,
                executed_quantity=quantity,
                state=_normalise_order_state(
                    latest_event.state,
                    fallback=base.state,
                ),
                created_at=created_at,
                updated_at=updated_at,
            )
        )
    return tuple(expanded)


def _position_order_from_row(
    row: object,
    *,
    plan: OrderExecutionPlan | None,
    fallback_state: ExchangeOrderState | None,
    fallback_executed_quantity: Decimal | None,
) -> _PositionOrder | None:
    try:
        position_side = FuturesPositionSide(
            getattr(row, "position_side", FuturesPositionSide.BOTH)
        )
    except (TypeError, ValueError):
        return None
    created_at = getattr(row, "created_at", None)
    updated_at = getattr(row, "updated_at", None)
    if created_at is None:
        created_at = plan.created_at if plan is not None else None
    if updated_at is None:
        updated_at = created_at
    if created_at is None or updated_at is None:
        return None
    state = _normalise_order_state(
        getattr(row, "state", None),
        fallback=fallback_state,
    )
    executed_quantity = _decimal_or_zero(
        getattr(row, "executed_quantity", None)
    )
    if fallback_executed_quantity is not None:
        executed_quantity = max(
            executed_quantity,
            _decimal_or_zero(fallback_executed_quantity),
        )
    quantity = _decimal_or_zero(
        getattr(row, "quantity", None)
        if getattr(row, "quantity", None) is not None
        else (None if plan is None else plan.quantity)
    )
    quantity = max(quantity, executed_quantity)
    if quantity <= 0:
        return None
    price_value = getattr(row, "price", None)
    if price_value is None and plan is not None:
        price_value = plan.price
    price = None if price_value is None else _decimal_or_zero(price_value)
    return _PositionOrder(
        symbol=str(getattr(row, "symbol", plan.symbol if plan else "")),
        position_side=position_side,
        side=str(getattr(row, "side", plan.side if plan else "")).upper(),
        reduce_only=bool(
            getattr(row, "reduce_only", plan.reduce_only if plan else False)
        ),
        order_type=str(
            getattr(row, "order_type", plan.order_type if plan else "")
        ).upper(),
        quantity=quantity,
        executed_quantity=executed_quantity,
        state=state,
        client_order_id=_optional_text(
            getattr(row, "client_order_id", plan.client_order_id if plan else None)
        ),
        exchange_order_id=_optional_text(
            getattr(row, "exchange_order_id", None)
        ),
        created_at=created_at,
        updated_at=updated_at,
        price=price,
        plan=plan,
    )


def _position_order_from_plan(item: PersistedExchangeOrder) -> _PositionOrder:
    plan = item.plan
    return _PositionOrder(
        symbol=plan.symbol,
        position_side=FuturesPositionSide(plan.position_side),
        side=plan.side.upper(),
        reduce_only=plan.reduce_only,
        order_type=plan.order_type.upper(),
        quantity=plan.quantity,
        executed_quantity=max(Decimal("0"), item.executed_quantity),
        state=_normalise_order_state(item.state),
        client_order_id=plan.client_order_id,
        exchange_order_id=item.exchange_order_id,
        created_at=plan.created_at,
        updated_at=item.updated_at,
        price=plan.price,
        plan=plan,
    )


def _build_position_batches(
    *,
    position: AccountPositionSnapshot | AccountPositionSnapshotRow,
    side: StrategySide,
    position_side: FuturesPositionSide,
    matching_orders: Sequence[_PositionOrder],
    fill_times: Mapping[str, datetime],
    fill_prices: Mapping[str, Decimal],
) -> tuple[ManagedLivePositionBatch, ...]:
    events: list[tuple[datetime, int, int, str, _PositionOrder]] = []
    for index, order in enumerate(matching_orders):
        if not order.reduce_only and _opening_order_matches_side(order.side, side):
            if _is_entry_fill_observed(order, fill_times):
                events.append(
                    (
                        _order_entry_time(order, fill_times),
                        0,
                        index,
                        "entry",
                        order,
                    )
                )
        elif (
            order.reduce_only
            and not _opening_order_matches_side(order.side, side)
            and order.state in _EXIT_SUBMITTED_STATES
        ):
            events.append(
                (order.created_at, 1, index, "exit", order)
            )
    events.sort(key=lambda event: event[:3])
    accumulators: list[_PositionBatchAccumulator] = []
    current: _PositionBatchAccumulator | None = None
    for event_at, _event_priority, _index, event_kind, order in events:
        if event_kind == "entry":
            entry_quantity = _entry_fill_quantity(order, fill_times)
            if entry_quantity <= 0:
                continue
            entry_price = _entry_price(order, fill_prices, position.entry_price)
            if current is None or current.exit_order_submitted_at is not None:
                current = _PositionBatchAccumulator(
                    batch_id=_batch_id_for_entry(order),
                    opened_at=event_at,
                    entry_quantity=entry_quantity,
                    entry_notional=entry_quantity * entry_price,
                )
                accumulators.append(current)
            else:
                current.entry_quantity += entry_quantity
                current.entry_notional += entry_quantity * entry_price
                current.opened_at = max(current.opened_at, event_at)
            continue
        target = current
        if order.exit_batch_id is not None:
            target = next(
                (
                    batch for batch in accumulators
                    if batch.batch_id == order.exit_batch_id
                ),
                None,
            )

        filled_quantity = _exit_fill_quantity(order)
        remaining_fill = filled_quantity

        def attach(
            batch: _PositionBatchAccumulator,
            quantity: Decimal,
            *,
            exit_order: _PositionOrder = order,
            submitted_at: datetime = event_at,
            legacy_attribution: bool = order.legacy_exit_attribution,
        ) -> None:
            if legacy_attribution:
                batch.legacy_exit_attribution = True
            if batch.exit_order_submitted_at is None:
                batch.exit_order_submitted_at = submitted_at
            batch.exit_orders.append(exit_order)
            batch.exit_filled_quantity += quantity

        if order.legacy_exit_attribution:
            # Legacy rows intentionally keep the old fail-closed semantics:
            # they may describe a historical boundary for the latest
            # accumulator, but their fill must not be redistributed across
            # other batches because the exchange-side lot is unknowable.
            if target is not None:
                available = max(
                    Decimal("0"),
                    target.entry_quantity - target.exit_filled_quantity,
                )
                if filled_quantity <= 0 and available > 0:
                    attach(target, Decimal("0"))
                elif available > 0 and filled_quantity > 0:
                    attach(target, min(available, filled_quantity))
            continue

        if target is not None:
            available = max(
                Decimal("0"),
                target.entry_quantity - target.exit_filled_quantity,
            )
            if filled_quantity <= 0:
                # An active/canceled historical order still creates a batch
                # boundary, but only while that named batch has remaining
                # capacity.  A stale order for a closed batch must not become
                # a recovery boundary for a newer position.
                if available > 0:
                    attach(target, Decimal("0"))
                else:
                    target = None
            elif available > 0:
                allocated = min(available, remaining_fill)
                attach(target, allocated)
                remaining_fill -= allocated
            else:
                target = None

        # Reduce-only orders are executed against the aggregate exchange
        # position.  A historical batch binding can therefore be wrong after
        # an old stale-exit bug.  When the named batch is already exhausted,
        # allocate the filled overflow to the newest surviving batches instead
        # of leaving a phantom old batch that steals the next position's
        # quantity during snapshot reconciliation.
        if remaining_fill > 0 or (filled_quantity <= 0 and target is None):
            fallback_candidates = reversed(accumulators)
            for fallback in fallback_candidates:
                if fallback is target:
                    continue
                available = max(
                    Decimal("0"),
                    fallback.entry_quantity - fallback.exit_filled_quantity,
                )
                if available <= 0:
                    continue
                if filled_quantity <= 0:
                    attach(fallback, Decimal("0"))
                    break
                allocated = min(available, remaining_fill)
                attach(fallback, allocated)
                if order.exit_batch_id is not None:
                    log.warning(
                        "live_exit_batch_binding_reassigned",
                        symbol=order.symbol,
                        client_order_id=order.client_order_id,
                        bound_batch_id=order.exit_batch_id,
                        fallback_batch_id=fallback.batch_id,
                        filled_quantity=str(filled_quantity),
                        reassigned_quantity=str(allocated),
                    )
                remaining_fill -= allocated
                if remaining_fill <= 0:
                    break

    if not accumulators:
        return ()
    batches: list[ManagedLivePositionBatch] = []
    for accumulator in accumulators:
        remaining_quantity = max(
            Decimal("0"),
            accumulator.entry_quantity - accumulator.exit_filled_quantity,
        )
        if remaining_quantity <= 0:
            continue
        entry_price = (
            accumulator.entry_notional / accumulator.entry_quantity
            if accumulator.entry_quantity > 0
            else position.entry_price
        )
        active_limit_orders = [
            order
            for order in accumulator.exit_orders
            if order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "LIMIT"
            and not order.state.terminal
        ]
        active_market_order = any(
            order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "MARKET"
            and not order.state.terminal
            for order in accumulator.exit_orders
        )
        recovery_order = max(
            active_limit_orders,
            key=lambda order: (order.created_at, order.updated_at),
            default=None,
        )
        recovery_remaining = None
        if recovery_order is not None and recovery_order.plan is not None:
            recovery_remaining = max(
                Decimal("0"),
                recovery_order.plan.quantity - recovery_order.executed_quantity,
            )
        batches.append(
            ManagedLivePositionBatch(
                batch_id=accumulator.batch_id,
                quantity=remaining_quantity,
                entry_price=entry_price,
                opened_at=accumulator.opened_at,
                exit_order_submitted_at=accumulator.exit_order_submitted_at,
                recovery_order_client_id=(
                    None
                    if recovery_order is None or recovery_order.plan is None
                    else recovery_order.plan.client_order_id
                ),
                recovery_order_plan=(
                    None
                    if recovery_order is None
                    else recovery_order.plan
                ),
                recovery_order_remaining_quantity=recovery_remaining,
                closing_order_filled=active_market_order,
                legacy_attribution=accumulator.legacy_exit_attribution,
            )
        )
    return _reconcile_batch_quantities(
        batches,
        target_quantity=abs(position.position_amt),
    )


def _reconcile_batch_quantities(
    batches: list[ManagedLivePositionBatch],
    *,
    target_quantity: Decimal,
) -> tuple[ManagedLivePositionBatch, ...]:
    if target_quantity <= 0:
        return ()
    if not batches:
        # There is no surviving confirmed batch to which the snapshot can be
        # attributed.  Reusing the last closed accumulator would turn a
        # snapshot/order synchronization gap into an old exit deadline.
        return ()
    has_legacy_attribution = any(
        batch.legacy_attribution for batch in batches
    )
    if has_legacy_attribution:
        # Pre-binding exit rows cannot identify which lot they consumed.  Do
        # not let their old recovery boundary remain executable.  Keep only
        # batches with durable bindings; if they cannot explain the whole
        # exchange snapshot, fail closed until a later account refresh sees a
        # complete current episode.
        batches = [
            batch for batch in batches if not batch.legacy_attribution
        ]
        if not batches:
            return ()
        clean_quantity = sum(
            (batch.quantity for batch in batches),
            start=Decimal("0"),
        )
        if clean_quantity < target_quantity:
            return ()
        if clean_quantity == target_quantity:
            return tuple(batches)

        # With legacy history present, prefer the newest durably-bound
        # batches.  This is the opposite of the normal lag reconciliation,
        # which preserves older boundaries when all exits are explicit.
        excess = clean_quantity - target_quantity
        legacy_reconciled: list[ManagedLivePositionBatch] = []
        for batch in batches:
            remove = min(excess, batch.quantity)
            remaining = batch.quantity - remove
            excess -= remove
            if remaining > 0:
                legacy_reconciled.append(replace(batch, quantity=remaining))
        return tuple(legacy_reconciled)
    total_quantity = sum((batch.quantity for batch in batches), start=Decimal("0"))
    if total_quantity < target_quantity:
        # Known entry/exit fills are more precise than an account snapshot
        # that may lag those fills.  Never inflate a surviving older batch to
        # the aggregate snapshot quantity; that would duplicate a newer batch
        # already known to have been closed.
        return tuple(batches)
    if total_quantity == target_quantity:
        return tuple(batches)

    excess = total_quantity - target_quantity
    reconciled: list[ManagedLivePositionBatch] = []
    for batch in reversed(batches):
        remove = min(excess, batch.quantity)
        remaining = batch.quantity - remove
        excess -= remove
        if remaining > 0:
            reconciled.append(replace(batch, quantity=remaining))
    reconciled.reverse()
    return tuple(reconciled)


def _is_entry_fill_observed(
    order: _PositionOrder,
    fill_times: Mapping[str, datetime],
) -> bool:
    return (
        order.state in {
            ExchangeOrderState.PARTIALLY_FILLED,
            ExchangeOrderState.FILLED,
        }
        or _entry_fill_at(order, fill_times) is not None
        or order.executed_quantity > 0
    )


def _order_entry_time(
    order: _PositionOrder,
    fill_times: Mapping[str, datetime],
) -> datetime:
    return _entry_fill_at(order, fill_times) or order.updated_at


def _entry_fill_quantity(
    order: _PositionOrder,
    fill_times: Mapping[str, datetime],
) -> Decimal:
    if order.executed_quantity > 0:
        return order.executed_quantity
    if order.state is ExchangeOrderState.FILLED:
        return order.quantity
    if (
        order.state is ExchangeOrderState.PARTIALLY_FILLED
        and _entry_fill_at(order, fill_times) is not None
    ):
        return order.quantity
    return Decimal("0")


def _entry_price(
    order: _PositionOrder,
    fill_prices: Mapping[str, Decimal],
    fallback: Decimal,
) -> Decimal:
    for identifier in (order.exchange_order_id, order.client_order_id):
        if identifier is not None and identifier in fill_prices:
            price = fill_prices[identifier]
            if price > 0:
                return price
    if order.price is not None and order.price > 0:
        return order.price
    return fallback


def _batch_id_for_entry(order: _PositionOrder) -> str:
    identifier = order.client_order_id or order.exchange_order_id
    if identifier is None:
        identifier = (
            f"{order.created_at.isoformat()}:{order.side}:{order.quantity}"
        )
    return f"{order.symbol}:{order.position_side.value}:{identifier}"


def _position_order_key(order: _PositionOrder) -> str:
    if order.exchange_order_id is not None:
        return f"exchange:{order.exchange_order_id}"
    if order.client_order_id is not None:
        return f"client:{order.client_order_id}"
    return (
        f"anonymous:{order.symbol}:{order.position_side.value}:"
        f"{order.side}:{int(order.reduce_only)}:{order.order_type}:"
        f"{order.created_at.isoformat()}:{order.quantity}"
    )


def _normalise_order_state(
    value: object,
    *,
    fallback: ExchangeOrderState | None = None,
) -> ExchangeOrderState:
    if isinstance(value, ExchangeOrderState):
        return value
    if value is not None:
        try:
            return ExchangeOrderState(str(value))
        except ValueError:
            pass
    if fallback is not None:
        return fallback
    return ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION


def _exit_fill_quantity(order: _PositionOrder) -> Decimal:
    if order.state not in _EXIT_SUBMITTED_STATES:
        return Decimal("0")
    if order.executed_quantity > 0:
        return order.executed_quantity
    return order.quantity if order.state is ExchangeOrderState.FILLED else Decimal("0")


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _decimal_or_zero(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")


def _strategy_side(
    position: AccountPositionSnapshot | AccountPositionSnapshotRow,
    position_side: FuturesPositionSide,
) -> StrategySide:
    if position_side is FuturesPositionSide.LONG:
        return StrategySide.LONG
    if position_side is FuturesPositionSide.SHORT:
        return StrategySide.SHORT
    return StrategySide.LONG if position.position_amt > 0 else StrategySide.SHORT


def _opening_order_matches_side(order_side: str, side: StrategySide) -> bool:
    return (order_side == "BUY") is (side is StrategySide.LONG)


def _filled_order_quantity(order: object) -> Decimal:
    executed_quantity = getattr(order, "executed_quantity", None)
    if executed_quantity is not None:
        try:
            executed = Decimal(str(executed_quantity))
        except (ArithmeticError, TypeError, ValueError):
            executed = Decimal("0")
        if executed > 0:
            return executed

    # FILLED rows written before cumulative executed_quantity was persisted
    # still carry the planned quantity, which is the safest fallback for the
    # account-sync-lag suppression path.
    quantity = getattr(order, "quantity", None)
    if quantity is None:
        return Decimal("0")
    try:
        return max(Decimal("0"), Decimal(str(quantity)))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")


def _entry_fill_at(
    order: object | None,
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


def _record_fill_value(
    fill_values: dict[str, tuple[Decimal, Decimal]],
    identifier: str | None,
    quantity: Decimal,
    price: Decimal,
) -> None:
    if identifier is None or quantity <= 0 or price <= 0:
        return
    previous_quantity, previous_notional = fill_values.get(
        identifier,
        (Decimal("0"), Decimal("0")),
    )
    fill_values[identifier] = (
        previous_quantity + quantity,
        previous_notional + quantity * price,
    )


def _record_fill_quantity(
    fill_quantities: dict[str, Decimal],
    identifier: str | None,
    quantity: Decimal,
) -> None:
    if identifier is None or quantity <= 0:
        return
    fill_quantities[identifier] = (
        fill_quantities.get(identifier, Decimal("0")) + quantity
    )


def _event_executed_quantity(
    event: ExchangeOrderEventRow,
) -> Decimal | None:
    details = event.details
    if not isinstance(details, dict):
        return None
    value = details.get("executed_quantity")
    if value is None:
        return None
    quantity = _decimal_or_zero(value)
    return quantity if quantity >= 0 else None


def _average_fill_prices(
    fill_values: Mapping[str, tuple[Decimal, Decimal]],
) -> dict[str, Decimal]:
    return {
        identifier: notional / quantity
        for identifier, (quantity, notional) in fill_values.items()
        if quantity > 0 and notional > 0
    }


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
