"""Runtime context contract shared by live decision lanes.

The context provider is deliberately a small asynchronous seam.  The
PostgreSQL implementation may add cache invalidation and currentness helpers,
but the daemon and decision lanes only require a fresh context for a market
state.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)


@dataclass(frozen=True, slots=True)
class LiveEntryFilterContext:
    """Entry-side market context used by the live EMA filters."""

    entry_price: Decimal | None
    ema5: Decimal | None = None
    ema10: Decimal | None = None
    ema_observed_at: datetime | None = None
    ema_snapshot_id: str | None = None
    ema_config_hash: str | None = None

    def __post_init__(self) -> None:
        if self.ema_observed_at is not None and (
            self.ema_observed_at.tzinfo is None
            or self.ema_observed_at.utcoffset() is None
        ):
            raise ValueError("ema_observed_at must be timezone-aware")
        for value, field_name in (
            (self.ema_snapshot_id, "ema_snapshot_id"),
            (self.ema_config_hash, "ema_config_hash"),
        ):
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be empty")


@dataclass(frozen=True, slots=True)
class LiveDaemonRuntimeContext:
    """Immutable account, risk, and execution view for one market state."""

    now: datetime
    gate_context: LiveGateContext
    active_lease: TradingLease | None
    account_state: ExecutionAccountStatus
    account_observed_at: datetime | None
    open_position_symbols: frozenset[str] | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    gross_exposure: Decimal | None
    active_halts: tuple[RiskHalt, ...]
    unresolved_order_states: tuple[ExchangeOrderState, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    trading_rules: dict[str, SymbolTradingRules]
    managed_positions: tuple[ManagedLivePosition, ...] = ()
    pending_position_symbols: frozenset[str] = frozenset()
    unmanaged_position_symbols: frozenset[str] = frozenset()
    unresolved_orders: tuple[PersistedExchangeOrder, ...] = ()
    account_snapshot: AccountSnapshot | None = None
    account_snapshot_version: int | None = None
    context_epoch: int | None = None


class LiveContextProvider(Protocol):
    """Load the runtime context required by a live decision lane."""

    def __call__(
        self,
        state: MarketState15s,
    ) -> Awaitable[LiveDaemonRuntimeContext]: ...


class LiveContextRuntime:
    """Own context freshness fencing and managed-symbol publication."""

    def __init__(
        self,
        *,
        run_id: str,
        context_provider: LiveContextProvider,
        set_pending_position_symbols: Callable[[Collection[str]], None],
        update_managed_symbols: Callable[
            [Collection[str], Collection[str]], None
        ],
        on_managed_position_symbols: (
            Callable[[frozenset[str]], Awaitable[None]] | None
        ) = None,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._context_provider = context_provider
        self._set_pending_position_symbols = set_pending_position_symbols
        self._update_managed_symbols = update_managed_symbols
        self._on_managed_position_symbols = on_managed_position_symbols
        self._generation = 0
        self._managed_position_symbols: frozenset[str] = frozenset()

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def managed_position_symbols(self) -> frozenset[str]:
        return self._managed_position_symbols

    def is_current(self, context: LiveDaemonRuntimeContext) -> bool:
        checker = getattr(self._context_provider, "is_context_current", None)
        if not callable(checker):
            return True
        try:
            return bool(checker(context))
        except Exception as error:
            _log.warning(
                "live_context_currentness_check_failed",
                run_id=self._run_id,
                error_type=type(error).__name__,
            )
            return False

    def invalidate(self) -> None:
        self._generation += 1
        invalidate = getattr(self._context_provider, "invalidate_cache", None)
        if callable(invalidate):
            invalidate()

    async def publish_managed_position_symbols(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        if not self.is_current(context):
            _log.info(
                "live_managed_position_symbols_stale_context_ignored",
                run_id=self._run_id,
            )
            return
        symbols = frozenset(
            (context.open_position_symbols or frozenset())
            | context.unmanaged_position_symbols
            | context.pending_position_symbols
        )
        self._managed_position_symbols = symbols
        self._set_pending_position_symbols(context.pending_position_symbols)
        managed_order_symbols = frozenset(
            order.plan.symbol.strip().upper()
            for order in context.unresolved_orders
            if order.plan.symbol.strip()
        )
        self._update_managed_symbols(symbols, managed_order_symbols)
        if self._on_managed_position_symbols is not None:
            await self._on_managed_position_symbols(symbols)


_log = structlog.get_logger()
