"""Runtime wiring for live entry-universe, EMA, and exchange warmups."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import UniverseRankingSnapshot
from crypto_momentum_lab.domain.strategy.entry_policy_compare import (
    universe_snapshot_for_symbols,
)
from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.live_rollout.context import LiveEntryFilterContext
from crypto_momentum_lab.live_rollout.entry_cache import (
    EntryFilterCacheConfig,
    LiveEntryFilterCache,
    LiveEntrySymbolCache,
    LiveEntryUniverseData,
    universe_context_for,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.strategy_runner.candle_source import (
    ClosedCandleEmaProvider,
)

log = structlog.get_logger()

ENTRY_FILTER_PREFETCH_CONCURRENCY = 4

SymbolLoader = Callable[[datetime], Awaitable[frozenset[str]]]
UniverseContextProvider = Callable[
    [str, datetime],
    dict[str, object] | None,
]
UniverseSnapshotProvider = Callable[
    [datetime],
    UniverseRankingSnapshot | None,
]
ReadyCallback = Callable[[bool], None]


class LiveEntryRuntime:
    """Own live entry-pool data, cache selection, and warmup semantics.

    The runtime keeps database and exchange I/O out of entry callbacks.  The
    callbacks exposed to the daemon read the in-memory cache, while this
    object owns initial exchange configuration and background cache choice.
    """

    def __init__(
        self,
        *,
        market_session_factory: async_sessionmaker[AsyncSession],
        client: BinanceUsdMTradeClient,
        ema_provider: ClosedCandleEmaProvider | None,
        positive_gainer_top_count: int | None,
        entry_leverage: int | None,
        margin_type: str | None,
    ) -> None:
        self._client = client
        self._ema_provider = ema_provider
        self._positive_gainer_top_count = positive_gainer_top_count
        self._entry_leverage = entry_leverage
        self._margin_type = margin_type
        self._universe_repository = (
            None
            if positive_gainer_top_count is None
            else PostgresUniverseRepository(market_session_factory)
        )
        self._entry_filter_cache: LiveEntryFilterCache | None = None
        self._entry_symbol_cache: LiveEntrySymbolCache | None = None
        if self._universe_repository is not None:
            universe_loader = self._load_entry_universe_data
            if ema_provider is not None:
                self._entry_filter_cache = LiveEntryFilterCache(
                    ema_provider=ema_provider,
                    universe_loader=universe_loader,
                    config=EntryFilterCacheConfig(
                        refresh_interval_seconds=15.0,
                        prefetch_concurrency=ENTRY_FILTER_PREFETCH_CONCURRENCY,
                    ),
                )
            else:
                self._entry_symbol_cache = LiveEntrySymbolCache(
                    universe_loader=universe_loader,
                    config=EntryFilterCacheConfig(
                        refresh_interval_seconds=15.0,
                        prefetch_concurrency=1,
                    ),
                )

    @property
    def entry_filter_cache_required(self) -> bool:
        return self._entry_filter_cache is not None

    @property
    def entry_symbol_cache_required(self) -> bool:
        return self._entry_symbol_cache is not None

    @property
    def entry_symbol_loader(self) -> SymbolLoader | None:
        if self._entry_filter_cache is not None:
            return self._load_entry_symbols_from_filter_cache
        if self._entry_symbol_cache is not None:
            return self._load_entry_symbols_from_symbol_cache
        if self._universe_repository is not None:
            return self._load_entry_symbols_from_database
        return None

    @property
    def entry_filter_context_loader(
        self,
    ) -> Callable[
        [MarketState15s],
        Awaitable[LiveEntryFilterContext | None],
    ] | None:
        if self._ema_provider is None:
            return None
        return self._load_entry_filter_context

    @property
    def entry_universe_context_provider(self) -> UniverseContextProvider | None:
        if self._positive_gainer_top_count is None:
            return None
        return self._load_entry_universe_context

    @property
    def entry_universe_snapshot_provider(
        self,
    ) -> UniverseSnapshotProvider | None:
        if self._positive_gainer_top_count is None:
            return None
        return self._load_entry_universe_policy_snapshot

    def set_ready_callback(self, callback: ReadyCallback | None) -> None:
        if self._entry_filter_cache is not None:
            self._entry_filter_cache.set_ready_callback(callback)
        if self._entry_symbol_cache is not None:
            self._entry_symbol_cache.set_ready_callback(callback)

    def start(self) -> tuple[asyncio.Task[None] | None, asyncio.Task[None] | None]:
        filter_task = (
            None
            if self._entry_filter_cache is None
            else asyncio.create_task(self._entry_filter_cache.run())
        )
        symbol_task = (
            None
            if self._entry_symbol_cache is None
            else asyncio.create_task(self._entry_symbol_cache.run())
        )
        return filter_task, symbol_task

    async def stop(self) -> None:
        if self._entry_filter_cache is not None:
            await self._entry_filter_cache.stop()
        if self._entry_symbol_cache is not None:
            await self._entry_symbol_cache.stop()

    async def warm_exchange(self, observed_at: datetime) -> None:
        initial_symbols: frozenset[str] = frozenset()
        if self._universe_repository is not None:
            try:
                initial_symbols = await self._load_entry_symbols_from_database(
                    observed_at
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning(
                    "live_entry_symbol_warmup_failed",
                    error_type=type(error).__name__,
                )
        if self._margin_type is not None:
            try:
                await self._client.warm_entry_margin_type(initial_symbols)
                log.info(
                    "live_entry_margin_type_warmed",
                    requested_symbol_count=len(initial_symbols),
                    cached_symbol_count=self._client.configured_margin_type_count,
                    margin_type=self._margin_type,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning(
                    "live_entry_margin_type_warmup_failed",
                    error_type=type(error).__name__,
                )
        if self._universe_repository is not None and self._entry_leverage is not None:
            try:
                await self._client.warm_entry_leverage(initial_symbols)
                log.info(
                    "live_entry_leverage_warmed",
                    symbol_count=len(initial_symbols),
                    leverage=self._entry_leverage,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning(
                    "live_entry_leverage_warmup_failed",
                    error_type=type(error).__name__,
                )

    async def _load_entry_universe_data(
        self,
        observed_at: datetime,
    ) -> LiveEntryUniverseData:
        assert self._universe_repository is not None
        assert self._positive_gainer_top_count is not None
        snapshot = await self._universe_repository.load_snapshot_at(observed_at)
        if snapshot is None:
            return LiveEntryUniverseData(symbols=frozenset(), snapshot=None)
        symbols = frozenset(
            entry.symbol
            for entry in snapshot.ranking.gainers[
                : self._positive_gainer_top_count
            ]
            if entry.utc_day_return > 0
        )
        return LiveEntryUniverseData(symbols=symbols, snapshot=snapshot)

    async def _load_entry_symbols_from_database(
        self,
        observed_at: datetime,
    ) -> frozenset[str]:
        return (await self._load_entry_universe_data(observed_at)).symbols

    async def _load_entry_symbols_from_filter_cache(
        self,
        observed_at: datetime,
    ) -> frozenset[str]:
        assert self._entry_filter_cache is not None
        return self._entry_filter_cache.symbols_for(observed_at)

    async def _load_entry_symbols_from_symbol_cache(
        self,
        observed_at: datetime,
    ) -> frozenset[str]:
        assert self._entry_symbol_cache is not None
        return self._entry_symbol_cache.symbols_for(observed_at)

    async def _load_entry_filter_context(
        self,
        state: MarketState15s,
    ) -> LiveEntryFilterContext | None:
        assert self._ema_provider is not None
        entry_price = (
            state.last_ask_price
            or state.midpoint
            or state.close_price
            or state.mark_price
        )
        if entry_price is None:
            return None
        if self._entry_filter_cache is not None:
            snapshot = self._entry_filter_cache.snapshot_for(
                symbol=state.symbol,
                observed_at=state.bucket_start,
            )
        else:
            snapshot = await asyncio.to_thread(
                self._ema_provider.load,
                symbol=state.symbol,
                observed_at=state.bucket_start,
            )
        if snapshot is None:
            return None
        return LiveEntryFilterContext(
            entry_price=entry_price,
            ema5=snapshot.ema5,
            ema10=snapshot.ema10,
            ema_observed_at=snapshot.observed_at,
            ema_snapshot_id=snapshot.snapshot_id,
            ema_config_hash=snapshot.config_hash,
        )

    def _cached_universe_data(
        self,
        observed_at: datetime,
    ) -> LiveEntryUniverseData | None:
        if self._entry_filter_cache is not None:
            return self._entry_filter_cache.universe_data_for(observed_at)
        if self._entry_symbol_cache is not None:
            return self._entry_symbol_cache.universe_data_for(observed_at)
        return None

    def _load_entry_universe_context(
        self,
        symbol: str,
        observed_at: datetime,
    ) -> dict[str, object] | None:
        assert self._positive_gainer_top_count is not None
        return universe_context_for(
            self._cached_universe_data(observed_at),
            symbol=symbol,
            entry_pool_name=(
                "positive_gainer_top" f"{self._positive_gainer_top_count}"
            ),
            entry_pool_top_count=self._positive_gainer_top_count,
        )

    def _load_entry_universe_policy_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseRankingSnapshot | None:
        assert self._positive_gainer_top_count is not None
        universe_data = self._cached_universe_data(observed_at)
        if universe_data is None:
            return None
        source_snapshot = universe_data.snapshot
        return universe_snapshot_for_symbols(
            universe_data.symbols,
            observed_at=(
                observed_at
                if source_snapshot is None
                else source_snapshot.observed_at
            ),
            snapshot_id=(
                None if source_snapshot is None else str(source_snapshot.snapshot_id)
            ),
            config_hash=(
                None if source_snapshot is None else source_snapshot.config_hash
            ),
        )


__all__ = ["LiveEntryRuntime"]
