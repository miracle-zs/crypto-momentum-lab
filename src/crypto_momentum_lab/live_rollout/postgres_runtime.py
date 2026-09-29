import asyncio
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
    PositionObservation,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.execution.position_batches import (
    ManagedLivePositionBatch,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    CoverageEvidence,
    PositionKey,
    compose_fact_coverage,
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
from crypto_momentum_lab.live_rollout.context import (
    ContextInvalidation,
    ContextInvalidationReason,
    LiveContextReader,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exits import (
    ManagedLivePosition,
    managed_live_positions_from_views,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.live_rollout.order_facts_loader import (
    OrderIdentityMetadata as _OrderIdentityMetadata,
)
from crypto_momentum_lab.live_rollout.order_facts_loader import (
    _fill_raw_payload,
    _load_order_identity_metadata,
    _resolve_symbol_fill_horizon,
)
from crypto_momentum_lab.live_rollout.order_identity import (
    _decimal_or_zero,
    _event_executed_quantity,
    _expand_legacy_order_row,
    _legacy_order_identity_is_ambiguous,
    _legacy_order_identity_is_reconstructible,
    _legacy_order_identity_is_zero_fill_terminal,
    _ms_to_dt,
    _normalise_order_state,
    _optional_text,
    _position_order_from_plan,
    _position_order_from_row,
)
from crypto_momentum_lab.live_rollout.order_identity_adapter import (
    LegacyOrderIdentityAdapter,
)
from crypto_momentum_lab.live_rollout.position_batches import (
    _average_fill_prices,
    _batch_id_for_entry,
    _build_position_batches,
    _entry_fill_at,
    _exit_fill_quantity,
    _is_entry_fill_observed,
    _order_entry_time,
    _position_order_key,
    _record_earliest_fill,
    _record_fill_quantity,
    _record_fill_value,
)
from crypto_momentum_lab.live_rollout.position_classification import (
    _classify_live_positions,
    _classify_live_positions_detailed,
    _filled_order_quantity,
    _has_recent_pending_entry_order,
    _normalise_position_orders,
    _opening_order_matches_side,
    _repair_legacy_exit_batch_bindings,
    _strategy_side,
)
from crypto_momentum_lab.live_rollout.position_self_healing import (
    auto_heal_unmanaged_position,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountFillReconciliationCursorRow,
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
from crypto_momentum_lab.persistence.postgres.position_order_window import (
    _load_order_anchor_events as _load_order_anchor_events,
)
from crypto_momentum_lab.persistence.postgres.position_order_window import (
    _opening_anchors_from_events as _opening_anchors_from_events,
)
from crypto_momentum_lab.persistence.postgres.position_order_window import (
    _OrderAnchorEvent as _OrderAnchorEvent,
)
from crypto_momentum_lab.persistence.postgres.position_order_window import (
    load_position_orders_bounded as _load_position_orders_bounded,
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

_PositionOrder = PositionOrderFact


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
# A full-account Book read is only needed to find Book-only residue, so it runs
# on its own cadence instead of once per market cut.
_BOOK_DRIFT_SCAN_INTERVAL_SECONDS = 300.0


class PostgresLiveContextProvider(LiveContextReader):
    _TRADING_RULE_CACHE_SECONDS = 300
    # Account events invalidate this snapshot immediately. A short positive
    # TTL lets consecutive market buckets reuse the same account/risk view
    # instead of issuing the full nine-query context load every 15 seconds.
    _CONTEXT_CACHE_SECONDS = 30
    _ABNORMAL_CONTEXT_CACHE_SECONDS = 0.5

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
            raise ValueError("session_factory or execution_session_factory is required")
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
        self._execution_book: Any | None = None
        self._cached_book_bucket_end: datetime | None = None
        self._cached_book_result: (
            tuple[frozenset[str], tuple[Any, ...], frozenset[str]] | None
        ) = None
        self._cached_book_unresolved: tuple[Any, ...] | None = None
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
            cached_context=cached_context,
            abnormal_max_age_seconds=self._ABNORMAL_CONTEXT_CACHE_SECONDS,
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
                    cached_context=current_context,
                    abnormal_max_age_seconds=self._ABNORMAL_CONTEXT_CACHE_SECONDS,
                )
            ):
                return await self._with_execution_book(
                    replace(
                    current_context,
                    now=now,
                    gate_context=replace(current_context.gate_context, now=now),
                    trading_rules={state.symbol: symbol_rules},
                    ),
                    state,
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
                cached_context=cached_context,
                abnormal_max_age_seconds=self._ABNORMAL_CONTEXT_CACHE_SECONDS,
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
                        cached_context=current_context,
                        abnormal_max_age_seconds=self._ABNORMAL_CONTEXT_CACHE_SECONDS,
                    )
                ):
                    return await self._with_execution_book(
                        replace(
                        current_context,
                        now=now,
                        gate_context=replace(
                            current_context.gate_context,
                            now=now,
                        ),
                        trading_rules={state.symbol: symbol_rules},
                        ),
                        state,
                    )
            return await self._with_execution_book(
                await self._load_context(state), state
            )

    def set_execution_book(self, execution_book: Any) -> None:
        """Use the restored ExecutionBook as the provider's position source."""
        if execution_book is None:
            raise ValueError("execution_book is required")
        self._execution_book = execution_book
        self.invalidate_cache()

    async def _with_execution_book(
        self,
        context: LiveDaemonRuntimeContext,
        state: MarketState15s,
    ) -> LiveDaemonRuntimeContext:
        book = getattr(self, "_execution_book", None)
        if book is None:
            return context
        if context.open_position_symbols == frozenset():
            # A confirmed flat account has no lots to allocate or exit. Reading
            # every historical Book scope at each market cut can replay hundreds
            # of journals even though none can represent current exposure.
            return replace(context, managed_positions=())
        cached_bucket_end = getattr(self, "_cached_book_bucket_end", None)
        cached_unresolved = getattr(self, "_cached_book_unresolved", None)
        cached_result = getattr(self, "_cached_book_result", None)
        if (
            cached_result is not None
            and cached_bucket_end == state.bucket_end
            and cached_unresolved == context.unresolved_orders
        ):
            visible_position_symbols, managed, unmanaged = cached_result
            return replace(
                context,
                open_position_symbols=visible_position_symbols,
                managed_positions=managed,
                unmanaged_position_symbols=unmanaged,
            )
        # Only scopes that can still represent current exposure need a read.
        # Replaying every historical Book scope at each market cut dominated
        # this path; the drift scan below keeps the Book-only diagnostic. When
        # the account snapshot is unavailable the filter is dropped, so an
        # uncertain account view still reads every scope and fails closed.
        views = await book.list_position_views(
            environment="live",
            account_label=self._account_label,
            event_cut=state.bucket_end,
            symbols=(
                context.open_position_symbols
                if context.account_snapshot is not None
                else None
            ),
        )
        managed = managed_live_positions_from_views(
            views,
            unresolved_orders=context.unresolved_orders,
        )
        account_position_keys = (
            frozenset(
                (position.symbol, position.position_side.upper())
                for position in context.account_snapshot.positions
                if position.position_amt != 0
            )
            if context.account_snapshot is not None
            else None
        )
        # The Book supplies lot identities, but account reconciliation is
        # authoritative for current exposure. Stale Book-only lots must not
        # generate reduce-only orders against an already-flat account.
        managed = tuple(
            position
            for position in managed
            if position.symbol in context.open_position_symbols
            and (
                account_position_keys is None
                or (position.symbol, position.position_side.value)
                in account_position_keys
            )
        )
        active_symbols = frozenset(position.symbol for position in managed)
        await self._observe_book_drift(book=book, context=context, state=state)
        # The account view determines current exposure. Book-only residuals
        # are durable accounting drift, not live positions to exit or subscribe
        # to. A real account position without a matching Book lot remains
        # unmanaged and still triggers the protective halt.
        unmanaged = (
            frozenset(context.unmanaged_position_symbols)
            | context.open_position_symbols
        ) - active_symbols
        if unmanaged and getattr(self, "_sessions", None) is not None:
            healed_any = False
            for sym in sorted(unmanaged):
                try:
                    async with self._sessions() as heal_session:
                        journal_store = getattr(self, "_journal_store", None)
                        if journal_store is None:
                            journal_store = PostgresAccountJournalStore()
                            self._journal_store = journal_store
                        pos_side = FuturesPositionSide.LONG
                        if context.account_snapshot is not None:
                            for p in context.account_snapshot.positions:
                                if p.symbol == sym and p.position_amt != 0:
                                    try:
                                        pos_side = FuturesPositionSide(p.position_side.upper())
                                    except ValueError:
                                        pos_side = FuturesPositionSide.LONG
                                    break
                        active_stream = None
                        if hasattr(book, "get_active_stream"):
                            active_stream = book.get_active_stream("live", self._account_label)
                        healed = await auto_heal_unmanaged_position(
                            session=heal_session,
                            journal_store=journal_store,
                            environment="live",
                            account_label=self._account_label,
                            symbol=sym,
                            position_side=pos_side,
                            active_stream_id=active_stream[0] if active_stream else "account_event_hub",
                            active_stream_epoch=active_stream[1] if active_stream else None,
                        )
                        if healed:
                            if hasattr(book, "reload_position"):
                                key = PositionKey(
                                    environment="live",
                                    account_label=self._account_label,
                                    symbol=sym,
                                    position_side=pos_side,
                                )
                                reloaded = await book.reload_position(key)
                                if reloaded is not None:
                                    healed_any = True
                            else:
                                healed_any = True
                except Exception as heal_err:
                    structlog.get_logger(__name__).error(
                        "auto_heal_unmanaged_position_failed",
                        symbol=sym,
                        account_label=self._account_label,
                        error=str(heal_err),
                    )

            if healed_any:
                views = await book.list_position_views(
                    environment="live",
                    account_label=self._account_label,
                    event_cut=state.bucket_end,
                    symbols=(
                        context.open_position_symbols
                        if context.account_snapshot is not None
                        else None
                    ),
                )
                managed = managed_live_positions_from_views(
                    views,
                    unresolved_orders=context.unresolved_orders,
                )
                managed = tuple(
                    position
                    for position in managed
                    if position.symbol in context.open_position_symbols
                    and (
                        account_position_keys is None
                        or (position.symbol, position.position_side.value)
                        in account_position_keys
                    )
                )
                active_symbols = frozenset(position.symbol for position in managed)
                unmanaged = (
                    frozenset(context.unmanaged_position_symbols)
                    | context.open_position_symbols
                ) - active_symbols
        self._cached_book_bucket_end = state.bucket_end
        self._cached_book_unresolved = context.unresolved_orders
        visible_position_symbols = context.open_position_symbols
        self._cached_book_result = (visible_position_symbols, managed, unmanaged)
        return replace(
            context,
            open_position_symbols=visible_position_symbols,
            managed_positions=managed,
            unmanaged_position_symbols=unmanaged,
        )

    async def _observe_book_drift(
        self,
        *,
        book: Any,
        context: LiveDaemonRuntimeContext,
        state: MarketState15s,
    ) -> None:
        """Report Book-only residue that the account view no longer shows.

        This needs a full-account read, so it runs on its own cadence instead
        of once per market cut; the trade path only reads the scopes that can
        still represent current exposure.
        """
        now = datetime.now(tz=UTC)
        last_scan = getattr(self, "_last_book_drift_scan_at", None)
        if (
            last_scan is not None
            and (now - last_scan).total_seconds() < _BOOK_DRIFT_SCAN_INTERVAL_SECONDS
        ):
            return
        self._last_book_drift_scan_at = now
        drift_views = await book.list_position_views(
            environment="live",
            account_label=self._account_label,
            event_cut=state.bucket_end,
        )
        book_position_symbols = frozenset(
            view.key.symbol
            for view in drift_views
            if view.total_quantity > 0
            or view.unallocated_quantity > 0
            or (
                view.reconciliation_gap is not None
                and view.reconciliation_gap != 0
            )
        )
        stale_book_symbols = book_position_symbols - context.open_position_symbols
        if stale_book_symbols != getattr(self, "_reported_stale_book_symbols", None):
            self._reported_stale_book_symbols = stale_book_symbols
            if stale_book_symbols:
                log.warning(
                    "live_book_positions_absent_from_account_view",
                    account_label=self._account_label,
                    count=len(stale_book_symbols),
                    sample=sorted(stale_book_symbols)[:5],
                )

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
        try:
            await asyncio.gather(*context_tasks)
        except BaseException:
            # gather propagates the first child failure without cancelling
            # its siblings.  These reads each own a database session, so let
            # them continue in the background and they can pile up across
            # market states when one query repeatedly times out.  Cancel and
            # await the whole batch so each session context manager can close
            # before the next context load starts.
            for task in context_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.shield(
                asyncio.gather(*context_tasks, return_exceptions=True)
            )
            raise
        approval = approval_task.result()
        risk_config = risk_config_task.result()
        lease = lease_task.result()
        halts = halts_task.result()
        unresolved_and_positions = unresolved_and_positions_task.result()
        if realtime_account_state is not None:
            account_state = realtime_account_state
        elif account_state_task is not None:
            account_state = account_state_task.result()
        else:
            raise RuntimeError("account readiness source is unavailable")
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
            *rest,
        ) = unresolved_and_positions[1]
        coverage_by_symbol: Mapping[str, CoverageEvidence] = rest[0] if rest else {}
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
            coverage_by_symbol=coverage_by_symbol,
        )
        if self._cache_epoch == cache_epoch and (
            self._cached_bucket_start is None
            or state.bucket_start >= self._cached_bucket_start
        ):
            self._cached_bucket_start = state.bucket_start
            self._cached_context = context
            self._cached_loaded_at = now
        return context

    async def for_state(
        self,
        state: MarketState15s,
    ) -> LiveDaemonRuntimeContext:
        """Alias for __call__ satisfying the LiveContextReader interface."""
        return await self(state)

    def is_current(self, context: LiveDaemonRuntimeContext) -> bool:
        """Alias for is_context_current satisfying the LiveContextReader interface."""
        return self.is_context_current(context)

    def invalidate(self, event: ContextInvalidation | None = None) -> None:
        """Alias for invalidate_cache satisfying the LiveContextReader interface."""
        self.invalidate_cache(event)

    def is_context_current(self, context: LiveDaemonRuntimeContext) -> bool:
        """Return whether a context still matches the live provider inputs."""
        context_epoch = getattr(context, "context_epoch", None)
        current_epoch = getattr(self, "_cache_epoch", 0)
        if context_epoch is not None and context_epoch != current_epoch:
            return False
        realtime_seq = getattr(self, "_realtime_account_sequence", 0)
        snapshot_version = getattr(context, "account_snapshot_version", None)
        if snapshot_version is not None:
            return bool(snapshot_version == realtime_seq)
        if getattr(context, "account_snapshot", None) is not None:
            return getattr(context.account_snapshot, "sequence", 0) == realtime_seq
        if realtime_seq > 0:
            return False
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
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence <= 0:
            raise ValueError("account snapshot sequence must be positive")
        if not isinstance(account_state, ExecutionAccountStatus):
            raise TypeError("account_state must be an ExecutionAccountStatus")
        self._realtime_account_snapshot = snapshot
        self._realtime_account_state = account_state
        self._realtime_account_sequence = sequence
        self.invalidate_cache(
            ContextInvalidation(
                reason=ContextInvalidationReason.ACCOUNT_UPDATE,
                occurred_at=datetime.now(tz=UTC),
                details={"sequence": sequence},
            )
        )

    def invalidate_account_snapshot(self) -> None:
        """Drop a stale Hub projection while a full recovery is in flight."""
        self._realtime_account_snapshot = None
        self._realtime_account_state = None
        self._realtime_account_sequence = 0
        self.invalidate_cache(
            ContextInvalidation(
                reason=ContextInvalidationReason.RECOVERY,
                occurred_at=datetime.now(tz=UTC),
            )
        )

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
        self.invalidate_cache(
            ContextInvalidation(
                reason=ContextInvalidationReason.LEASE_CHANGE,
                occurred_at=datetime.now(tz=UTC),
                details={"owner": lease.owner},
            )
        )

    def invalidate_cache(self, event: ContextInvalidation | None = None) -> None:
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
        self._cached_book_bucket_end = None
        self._cached_book_result = None
        self._cached_book_unresolved = None
        if event is not None:
            log.info(
                "live_context_cache_invalidated",
                account_label=self._account_label,
                run_id=self._run_id,
                reason=event.reason.value,
                epoch=self._cache_epoch,
            )

    @property
    def cached_context(self) -> LiveDaemonRuntimeContext | None:
        """Return the latest cached runtime context if available."""
        return getattr(self, "_cached_context", None)

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
            and (now - cached_at).total_seconds() < self._TRADING_RULE_CACHE_SECONDS
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
                and (now - cached_at).total_seconds() < self._TRADING_RULE_CACHE_SECONDS
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
            Mapping[str, CoverageEvidence],
        ],
    ]:
        unresolved = await self._order_repository.load_unresolved_orders(self._run_id)
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
        Mapping[str, CoverageEvidence],
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
                    AccountReconciliationRunRow.account_label == self._account_label,
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
                        AccountPositionSnapshotRow.account_label == self._account_label,
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
            domain_account_fills: tuple[AccountFillEvent, ...] = ()
            since_time: datetime | None = None
            if active:
                active_symbols = tuple(sorted({row.symbol for row in active}))
                orders = await _load_position_orders_bounded(
                    session,
                    run_id=self._run_id,
                    active_symbols=active_symbols,
                    account_label=self._account_label,
                )
                entry_client_order_ids = tuple(
                    sorted(
                        {row.client_order_id for row in orders if not row.reduce_only}
                    )
                )
                since_time = _resolve_symbol_fill_horizon(orders, active)
                order_identity_metadata = await _load_order_identity_metadata(
                    session,
                    orders,
                    account_label=self._account_label,
                    since=since_time,
                )
                domain_account_fills = order_identity_metadata.domain_account_fills
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
        exit_batch_ids = await _load_exit_batch_bindings(self._sessions, orders)
        coverage_by_symbol: dict[str, CoverageEvidence] = {}
        if active or (
            reconciliation is not None
            and getattr(reconciliation, "status", None) == "ready"
        ):
            async with self._sessions() as cursor_session:
                fill_cursors = (
                    await cursor_session.scalars(
                        select(AccountFillReconciliationCursorRow).where(
                            AccountFillReconciliationCursorRow.environment == "live",
                            AccountFillReconciliationCursorRow.account_label
                            == self._account_label,
                        )
                    )
                ).all()
            coverage_by_symbol = {
                cursor.symbol: _coverage_evidence_from_sources(
                    fill_cursor=cursor,
                    reconciliation=reconciliation,
                )
                for cursor in fill_cursors
            }
        managed, pending, unmanaged = _classify_live_positions_detailed(
            active,
            orders,
            unresolved,
            entry_fill_times=entry_fill_times,
            entry_fill_prices=_average_fill_prices(entry_fill_values),
            exit_batch_ids=exit_batch_ids,
            order_identity_events=order_identity_events,
            account_fill_quantities=account_fill_quantities,
            account_fills=domain_account_fills,
            coverage_by_symbol=coverage_by_symbol,
            build_managed_positions=getattr(self, "_execution_book", None) is None,
        )
        return (
            process_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
            managed,
            pending,
            unmanaged,
            coverage_by_symbol,
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
        Mapping[str, CoverageEvidence],
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
        domain_account_fills: tuple[AccountFillEvent, ...] = ()
        if active:
            async with self._sessions() as session:
                active_symbols = tuple(sorted({row.symbol for row in active}))
                orders = await _load_position_orders_bounded(
                    session,
                    run_id=self._run_id,
                    active_symbols=active_symbols,
                    account_label=self._account_label,
                )
                entry_client_order_ids = tuple(
                    sorted(
                        {row.client_order_id for row in orders if not row.reduce_only}
                    )
                )
                since_time = _resolve_symbol_fill_horizon(orders, active)
                order_identity_metadata = await _load_order_identity_metadata(
                    session,
                    orders,
                    account_label=self._account_label,
                    since=since_time,
                )
                domain_account_fills = order_identity_metadata.domain_account_fills
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
        exit_batch_ids = await _load_exit_batch_bindings(self._sessions, orders)
        coverage_by_symbol: dict[str, CoverageEvidence] = {}
        try:
            async with self._sessions() as session:
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
                if (
                    reconciliation is not None
                    and getattr(reconciliation, "status", None) == "ready"
                ):
                    fill_cursors = (
                        await session.scalars(
                            select(AccountFillReconciliationCursorRow).where(
                                AccountFillReconciliationCursorRow.environment
                                == "live",
                                AccountFillReconciliationCursorRow.account_label
                                == self._account_label,
                            )
                        )
                    ).all()
                    coverage_by_symbol = {
                        cursor.symbol: _coverage_evidence_from_sources(
                            fill_cursor=cursor,
                            reconciliation=reconciliation,
                        )
                        for cursor in fill_cursors
                    }
        except (AssertionError, Exception):
            pass

        managed, pending, unmanaged = _classify_live_positions_detailed(
            active,
            orders,
            unresolved,
            entry_fill_times=entry_fill_times,
            entry_fill_prices=_average_fill_prices(entry_fill_values),
            exit_batch_ids=exit_batch_ids,
            order_identity_events=order_identity_events,
            account_fill_quantities=account_fill_quantities,
            account_fills=domain_account_fills,
            coverage_by_symbol=coverage_by_symbol,
            build_managed_positions=getattr(self, "_execution_book", None) is None,
        )
        return (
            snapshot.config.observed_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
            managed,
            pending,
            unmanaged,
            coverage_by_symbol,
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


async def _load_exit_batch_ids(
    sessions: async_sessionmaker[AsyncSession],
    orders: Sequence[ExchangeOrderRow],
) -> dict[str, str]:
    return await _load_exit_batch_bindings(sessions, orders)


async def _load_exit_batch_bindings(
    sessions: async_sessionmaker[AsyncSession],
    orders: Sequence[ExchangeOrderRow],
) -> dict[str, str]:
    intent_clients = {
        order.intent_id: order.client_order_id for order in orders if order.reduce_only
    }
    if not intent_clients:
        return {}
    async with sessions() as session:
        rows = (
            await session.execute(
                select(
                    OrderIntentExecutionRow.intent_id, OrderIntentExecutionRow.details
                ).where(OrderIntentExecutionRow.intent_id.in_(tuple(intent_clients)))
            )
        ).all()
    result: dict[str, str] = {}
    for intent_id, details in rows:
        features = details.get("features", {}) if isinstance(details, dict) else {}
        batch_id = features.get("batch_id") if isinstance(features, dict) else None
        if isinstance(batch_id, str) and batch_id:
            client_order_id = intent_clients[intent_id]
            result[client_order_id] = batch_id
    return result


def _coverage_evidence_from_sources(
    *,
    fill_cursor: AccountFillReconciliationCursorRow | None,
    reconciliation: AccountReconciliationRunRow | None,
) -> CoverageEvidence:
    """Assemble durable coverage proof from fill cursor + reconciliation.

    Neither source alone can confirm a window: the cursor proves continuous
    fill ingestion, the ready reconciliation is the checkpoint cut.
    """
    cursor_id: str | None = None
    load_start: datetime | None = None
    checked_through: datetime | None = None
    if fill_cursor is not None:
        from_id = getattr(fill_cursor, "from_id", None)
        start_time_ms = getattr(fill_cursor, "start_time_ms", None)
        if from_id is not None:
            cursor_id = f"fill_from_id:{from_id}"
        elif start_time_ms is not None:
            cursor_id = f"fill_start_ms:{start_time_ms}"
        load_start = _ms_to_dt(start_time_ms)
        checked_through = getattr(fill_cursor, "last_checked_at", None)
    checkpoint_id: str | None = None
    checkpoint_cut: datetime | None = None
    if (
        reconciliation is not None
        and getattr(reconciliation, "status", None) == "ready"
    ):
        checkpoint_id = getattr(reconciliation, "reconciliation_id", None)
        checkpoint_cut = getattr(reconciliation, "observed_at", None)
    return CoverageEvidence(
        fill_cursor_id=cursor_id,
        fill_load_start=load_start,
        fill_checked_through=checked_through,
        checkpoint_id=checkpoint_id,
        checkpoint_event_cut=checkpoint_cut,
    )


def _context_cache_can_be_reused(
    *,
    state: MarketState15s,
    cached_bucket_start: datetime | None,
    cached_loaded_at: datetime | None,
    now: datetime,
    max_age_seconds: int,
    cached_context: LiveDaemonRuntimeContext | None = None,
    abnormal_max_age_seconds: float = 0.5,
) -> bool:
    if cached_loaded_at is not None:
        age_seconds = (now - cached_loaded_at).total_seconds()
        if age_seconds < 0:
            return False
        # Abnormal contexts (pending or unmanaged positions) must never bypass TTL
        # and should be refreshed frequently (default 0.5s debounce) to settle quickly.
        if cached_context is not None and (
            cached_context.pending_position_symbols
            or cached_context.unmanaged_position_symbols
        ):
            return age_seconds < abnormal_max_age_seconds
        # Hard TTL ceiling: same-bucket queries must never bypass max_age_seconds
        if age_seconds >= max_age_seconds:
            return False

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
            latest_bucket = await repository.load_latest_bucket(environment=environment)
            if (
                latest_bucket is not None
                and active_cursor.bucket_start is not None
                and (latest_bucket - active_cursor.bucket_start).total_seconds()
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
