"""Route account, quote, candle, and grace events into the live exit lanes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_lane import ExitExecutionLane
from crypto_momentum_lab.live_rollout.exit_processor import LiveExitProcessor
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m

log = structlog.get_logger()


class LiveExitEventCoordinator:
    """Own context preparation and routing for non-market exit triggers."""

    def __init__(
        self,
        *,
        run_id: str,
        exit_manager: LiveExitManager | None,
        exit_enabled: Callable[[], bool],
        run_active: Callable[[], bool],
        context_provider: LiveContextProvider,
        sync_pending_entry_plans: Callable[[LiveDaemonRuntimeContext], None],
        publish_managed_position_symbols: Callable[
            [LiveDaemonRuntimeContext], Awaitable[None]
        ],
        invalidate_context_cache: Callable[[], None],
        exit_processor: LiveExitProcessor,
        exit_lane: ExitExecutionLane,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._exit_manager = exit_manager
        self._exit_enabled = exit_enabled
        self._run_active = run_active
        self._context_provider = context_provider
        self._sync_pending_entry_plans = sync_pending_entry_plans
        self._publish_managed_position_symbols = publish_managed_position_symbols
        self._invalidate_context_cache = invalidate_context_cache
        self._exit_processor = exit_processor
        self._exit_lane = exit_lane

    async def process_account_event(
        self,
        state: MarketState15s,
        *,
        quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Refresh account context and route one account-triggered exit."""

        if self._exit_manager is None or not self._exit_enabled():
            return None
        context, failure = await self._load_context(state, invalidate=True)
        if failure is not None:
            return failure
        if self._run_active():
            await self._exit_lane.start()
            if quote is None:
                outcome = await self._exit_lane.submit_account(state, context)
            else:
                # Account events have their own channel; do not let a newer
                # ticker replace this account-triggered quote check.
                outcome = await self._exit_processor.process_quote(
                    quote,
                    state,
                    context,
                )
        else:
            outcome = (
                await self._exit_processor.process_state(state, context)
                if quote is None
                else await self._exit_processor.process_quote(
                    quote,
                    state,
                    context,
                )
            )
        self._invalidate_context_cache()
        if outcome.failure is not None:
            log.error(
                "live_account_event_exit_failed",
                run_id=self._run_id,
                symbol=state.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_market_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> str | None:
        """Route a latest-value quote to the reduce-only exit lane."""

        if self._exit_manager is None or not self._exit_enabled():
            return None
        if state.symbol != quote.symbol:
            return None
        context, failure = await self._load_context(state, invalidate=False)
        if failure is not None:
            return failure
        if self._run_active():
            await self._exit_lane.start()
            await self._exit_lane.submit_quote(quote, state, context)
            return None
        outcome = await self._exit_processor.process_quote(
            quote,
            state,
            context,
        )
        if outcome.failure is not None:
            log.error(
                "live_quote_exit_failed",
                run_id=self._run_id,
                symbol=quote.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        *,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Route one final 15m candle on the independent exit path."""

        if self._exit_manager is None or not self._exit_enabled():
            return None
        state = _market_state_for_closed_candle(
            event.candle,
            received_at=event.received_at,
            quote=latest_quote,
        )
        context, failure = await self._load_context(state, invalidate=False)
        if failure is not None:
            return failure
        outcome = await self._exit_processor.process_closed_candle(
            event,
            state,
            context,
            latest_quote,
        )
        if outcome.failure is not None:
            log.error(
                "live_closed_candle_exit_failed",
                run_id=self._run_id,
                symbol=event.candle.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        *,
        now: datetime,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Route a wall-clock grace timeout through the exit processor."""

        if self._exit_manager is None or not self._exit_enabled():
            return None
        context, failure = await self._load_context(state, invalidate=False)
        if failure is not None:
            return failure
        outcome = await self._exit_processor.process_grace_timeout(
            state,
            now,
            context,
            latest_quote,
        )
        if outcome.failure is not None:
            log.error(
                "live_grace_timeout_exit_failed",
                run_id=self._run_id,
                symbol=state.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def _load_context(
        self,
        state: MarketState15s,
        *,
        invalidate: bool,
    ) -> tuple[LiveDaemonRuntimeContext, str | None]:
        if invalidate:
            self._invalidate_context_cache()
        context = await self._context_provider(state)
        self._sync_pending_entry_plans(context)
        await self._publish_managed_position_symbols(context)
        if state.symbol in context.pending_position_symbols:
            symbols = ",".join(sorted(context.pending_position_symbols))
            return context, f"pending_live_positions:{symbols}"
        if state.symbol in context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return context, f"unmanaged_live_positions:{symbols}"
        return context, None


def _market_state_for_closed_candle(
    candle: ClosedCandle15m,
    *,
    received_at: datetime,
    quote: RealtimeMarketQuote | None,
) -> MarketState15s:
    bid_price = quote.bid_price if quote is not None else None
    ask_price = quote.ask_price if quote is not None else None
    if bid_price is not None and ask_price is not None:
        spread = ask_price - bid_price
        midpoint = (bid_price + ask_price) / Decimal("2")
    else:
        spread = None
        midpoint = candle.close_price
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=candle.symbol,
        bucket_start=candle.candle_end - timedelta(seconds=15),
        bucket_end=candle.candle_end,
        open_price=candle.open_price,
        high_price=None,
        low_price=None,
        close_price=candle.close_price,
        trade_count=0,
        trade_notional=Decimal("0"),
        aggressive_buy_notional=Decimal("0"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=bid_price,
        last_ask_price=ask_price,
        spread=spread,
        midpoint=midpoint,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=midpoint,
        closed_kline_count=1,
        source_event_count=1,
        first_received_at=received_at,
        last_received_at=received_at,
    )


__all__ = ["LiveExitEventCoordinator"]
