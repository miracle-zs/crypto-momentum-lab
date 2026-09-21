import asyncio
import os
import shutil
import signal
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

import structlog
import typer
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.config.database_url import resolve_database_url
from crypto_momentum_lab.config.loader import (
    behavior_hash,
    load_runtime_config,
)
from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.domain.operational.retention_contract import (
    RetentionConsumerRequirement,
)
from crypto_momentum_lab.domain.universe.models import (
    MembershipStatus,
    UniverseSnapshot,
)
from crypto_momentum_lab.health import LocalHealthWriter, StartupPhaseTimer
from crypto_momentum_lab.health.memory import configure_tracemalloc
from crypto_momentum_lab.market_data.agg_trade_recovery import (
    AggTradeGapRecoverer,
    agg_trade_gap_quality_event,
)
from crypto_momentum_lab.market_data.backfill import PromotionHistoryBackfiller
from crypto_momentum_lab.market_data.binance.connection_pool import (
    BinanceConnectionPool,
)
from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient
from crypto_momentum_lab.market_data.binance.websocket import (
    BinanceWebSocketConnection,
)
from crypto_momentum_lab.market_data.capture.coordinator import (
    CaptureCoordinator,
)
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue
from crypto_momentum_lab.market_data.capture.service import (
    DiskSpaceGuard,
    MarketDataCaptureService,
)
from crypto_momentum_lab.market_data.capture.subscriptions import (
    SubscriptionGroup,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateHub,
    MarketStateHubConfig,
)
from crypto_momentum_lab.market_data.observability import (
    monitor_market_data_health,
)
from crypto_momentum_lab.market_data.quality.tracker import StreamQualityTracker
from crypto_momentum_lab.market_data.quote_hub import (
    MarketQuoteHub,
    MarketQuoteHubConfig,
)
from crypto_momentum_lab.market_data.quote_volume import (
    Binance24hQuoteVolumePublisher,
)
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
    ClosedMarketStatePublisherConfig,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.capture_repository import (
    PostgresCaptureRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    StrategyRuntimeCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.operational_retention import (
    PostgresOperationalRetentionRepository,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_partitions import (
    cutover_runtime_state_partition,
    prepare_runtime_state_partition,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_maintenance_database_engine,
    create_market_database_engine,
    create_observability_database_engine,
    create_partitioning_database_engine,
)
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive
from crypto_momentum_lab.persistence.raw_files.journal import PendingManifestJournal
from crypto_momentum_lab.persistence.raw_files.recovery import recover_archive_root
from crypto_momentum_lab.persistence.raw_files.retention import (
    delete_archive_files,
    retention_cutoff_date,
)
from crypto_momentum_lab.universe.daily_open_prefetch import DailyOpenPrefetcher
from crypto_momentum_lab.universe.refresh import UniverseRefreshService
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()

_UNIVERSE_REFRESH_TIMEOUT_SECONDS = 120.0
# Cap the symbol lists on the membership-churn log so a first snapshot (where
# every symbol looks "added") cannot flood the journal.
_SYMBOL_LOG_LIMIT = 20
_MARKET_DATA_STARTUP_GRACE_SECONDS = 120.0
_MARKET_DATA_STALE_AFTER_SECONDS = 120.0
_MARKET_DATA_WATCHDOG_INTERVAL_SECONDS = 15.0
_CAPTURE_STOP_TIMEOUT_SECONDS = 55.0
_PAPER_EXIT_RECONCILE_SECONDS = 15.0
_DATABASE_RETENTION_INTERVAL_SECONDS = 300.0
_DATABASE_RETENTION_MAX_RUNTIME_SECONDS = 45.0
_CONTRACT_METADATA_RETENTION_HOURS = 6.0
# Strategy warmup needs about 34 minutes of 15-second history and startup
# recovery backfills it from this table, so 12 hours is ~20x the real
# requirement.  research_collector also falls back to this table when its Hub
# cursor cannot replay; the window only decides how often that fallback is
# needed, not whether the data survives (the raw stream and the parquet
# datasets are separate, longer-lived copies).
_RUNTIME_STATE_RETENTION_HOURS = 12.0
_CONTRACT_METADATA_RETENTION_BATCH_SIZE = 250
_RUNTIME_STATE_RETENTION_BATCH_SIZE = 250
_PAPER_EXIT_RUN_IDS_ENV = "CML_PAPER_EXIT_RUN_IDS"
_LIVE_POSITION_ACCOUNT_LABEL_ENV = "CML_LIVE_POSITION_ACCOUNT_LABEL"
_LIVE_POSITION_ACCOUNT_LABELS_ENV = "CML_LIVE_POSITION_ACCOUNT_LABELS"
_MARKET_STATE_HUB_HOST_ENV = "CML_MARKET_STATE_HUB_HOST"
_MARKET_STATE_HUB_PORT_ENV = "CML_MARKET_STATE_HUB_PORT"
_MARKET_STATE_HUB_DEFAULT_HOST = "0.0.0.0"
_MARKET_STATE_HUB_DEFAULT_PORT = 8766
_MARKET_QUOTE_HUB_HOST_ENV = "CML_MARKET_QUOTE_HUB_HOST"
_MARKET_QUOTE_HUB_PORT_ENV = "CML_MARKET_QUOTE_HUB_PORT"
_MARKET_QUOTE_HUB_DEFAULT_HOST = "0.0.0.0"
_MARKET_QUOTE_HUB_DEFAULT_PORT = 8768


def _run_market_data(coroutine: Coroutine[object, object, None]) -> None:
    """Run the high-throughput capture loop on uvloop when available."""
    try:
        import uvloop
    except ImportError:
        asyncio.run(coroutine)
    else:
        uvloop.run(coroutine)


class MarketDataStaleError(RuntimeError):
    pass


def parse_observed_at(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC).replace(second=0, microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--at must include a timezone")
    return parsed.astimezone(UTC).replace(second=0, microsecond=0)


def resolve_config_path(value: Path | None) -> Path:
    if value is not None:
        return value
    return Path(
        os.environ.get(
            "CML_ENVIRONMENT_CONFIG",
            "configs/environments/research.yaml",
        )
    )


def _market_database_url(default_url: str) -> str:
    """Use the market plane when configured, otherwise the shared database."""

    return resolve_database_url(None, "CML_MARKET_DATABASE_URL") or default_url


def parse_paper_exit_run_ids(value: str | None = None) -> frozenset[str]:
    raw_value = os.environ.get(_PAPER_EXIT_RUN_IDS_ENV, "") if value is None else value
    return frozenset(
        run_id for item in raw_value.split(",") if (run_id := item.strip())
    )


def parse_live_position_account_label(value: str | None = None) -> str | None:
    raw_value = (
        os.environ.get(_LIVE_POSITION_ACCOUNT_LABEL_ENV, "") if value is None else value
    )
    normalized = raw_value.strip()
    return normalized or None


def parse_live_position_account_labels(
    value: str | None = None,
) -> frozenset[str]:
    """Parse the live accounts whose positions must remain market-protected.

    The plural variable is additive and keeps the old singular variable as a
    compatibility fallback for the one-account deployment.
    """

    if value is None:
        plural_value = os.environ.get(_LIVE_POSITION_ACCOUNT_LABELS_ENV, "")
        singular_value = parse_live_position_account_label()
    else:
        plural_value = value
        singular_value = None
    raw_labels = (
        tuple(item.strip() for item in plural_value.split(","))
        if plural_value.strip()
        else ()
    )
    if any(not item for item in raw_labels):
        raise ValueError(
            f"{_LIVE_POSITION_ACCOUNT_LABELS_ENV} must contain non-empty labels"
        )
    labels = {item for item in raw_labels if item}
    if singular_value is not None:
        labels.add(singular_value)
    return frozenset(labels)


async def _load_protected_symbols(
    *,
    paper_repository: PostgresPaperDaemonRepository,
    account_repository: PostgresAccountRepository,
    protected_run_ids: frozenset[str],
    configured_live_position_account_labels: frozenset[str],
) -> frozenset[str]:
    """Load paper symbols plus every live account with a durable open position.

    The configured labels remain an explicit startup hint for compatibility,
    while the latest ready account reconciliation runs discover labels for
    stopped or newly added live accounts automatically.
    """
    paper_symbols = await paper_repository.load_open_position_symbols(protected_run_ids)
    discovered_labels = await account_repository.load_active_position_account_labels(
        environment="live"
    )
    live_position_account_labels = (
        configured_live_position_account_labels | discovered_labels
    )
    live_symbols: set[str] = set()
    for account_label in live_position_account_labels:
        live_symbols.update(
            await account_repository.load_active_position_symbols(
                environment="live",
                account_label=account_label,
            )
        )
    return paper_symbols | live_symbols


def parse_market_state_hub_port(value: str | None = None) -> int:
    raw_value = (
        os.environ.get(_MARKET_STATE_HUB_PORT_ENV, str(_MARKET_STATE_HUB_DEFAULT_PORT))
        if value is None
        else value
    )
    try:
        port = int(raw_value)
    except ValueError as error:
        raise ValueError("market-state hub port must be an integer") from error
    if not 0 <= port <= 65535:
        raise ValueError("market-state hub port must be between 0 and 65535")
    return port


def parse_market_quote_hub_port(value: str | None = None) -> int:
    raw_value = (
        os.environ.get(_MARKET_QUOTE_HUB_PORT_ENV, str(_MARKET_QUOTE_HUB_DEFAULT_PORT))
        if value is None
        else value
    )
    try:
        port = int(raw_value)
    except ValueError as error:
        raise ValueError("market-quote hub port must be an integer") from error
    if not 0 <= port <= 65535:
        raise ValueError("market-quote hub port must be between 0 and 65535")
    return port


@asynccontextmanager
async def build_refresh_service(
    config_path: Path,
) -> AsyncIterator[tuple[UniverseRefreshService, int, int]]:
    runtime = load_runtime_config(config_path)
    engine = create_market_database_engine(_market_database_url(runtime.database_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresUniverseRepository(session_factory)
    client = BinanceUsdMRestClient(str(runtime.binance_base_url))
    try:
        yield (
            UniverseRefreshService(
                market_data=client,
                repository=repository,
                config=runtime.universe,
                config_hash=behavior_hash(runtime),
            ),
            runtime.universe.activation_minute,
            runtime.universe.refresh_interval_minutes,
        )
    finally:
        await client.aclose()
        await engine.dispose()


async def refresh_once(
    config_path: Path,
    observed_at: datetime,
) -> UniverseSnapshot:
    async with build_refresh_service(config_path) as (service, _, _):
        snapshot = await service.refresh(observed_at=observed_at)
        log_snapshot(snapshot)
        return snapshot


def format_snapshot(snapshot: UniverseSnapshot) -> str:
    eligible = len(snapshot.ranking.candidates) - len(snapshot.ranking.exclusions)
    return " ".join(
        [
            f"snapshot_id={snapshot.snapshot_id}",
            f"observed_at={snapshot.observed_at.isoformat()}",
            f"activated={str(snapshot.activated).lower()}",
            f"eligible={eligible}",
            f"target={len(snapshot.ranking.target_symbols)}",
            f"monitoring={len(snapshot.memberships)}",
            f"excluded={len(snapshot.ranking.exclusions)}",
        ]
    )


def log_snapshot(snapshot: UniverseSnapshot) -> None:
    log.info(
        "universe_refreshed",
        snapshot_id=str(snapshot.snapshot_id),
        observed_at=snapshot.observed_at.isoformat(),
        activated=snapshot.activated,
        eligible=(len(snapshot.ranking.candidates) - len(snapshot.ranking.exclusions)),
        target=len(snapshot.ranking.target_symbols),
        monitoring=len(snapshot.memberships),
        excluded=len(snapshot.ranking.exclusions),
    )


class LoggingRefreshService:
    def __init__(
        self,
        delegate: UniverseRefreshService,
        *,
        timeout_seconds: float = _UNIVERSE_REFRESH_TIMEOUT_SECONDS,
    ) -> None:
        self._delegate = delegate
        self._timeout_seconds = timeout_seconds

    async def refresh(
        self,
        *,
        observed_at: datetime,
    ) -> UniverseSnapshot:
        async with asyncio.timeout(self._timeout_seconds):
            snapshot = await self._delegate.refresh(observed_at=observed_at)
        log_snapshot(snapshot)
        return snapshot


async def monitor_market_data_freshness(
    *,
    latest_observed_at: Callable[[], datetime | None],
    startup_grace_seconds: float = _MARKET_DATA_STARTUP_GRACE_SECONDS,
    stale_after_seconds: float = _MARKET_DATA_STALE_AFTER_SECONDS,
    check_interval_seconds: float = _MARKET_DATA_WATCHDOG_INTERVAL_SECONDS,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    started_at = clock()
    while True:
        await sleeper(check_interval_seconds)
        now = clock()
        observed_at = latest_observed_at()
        if observed_at is None:
            startup_age = (now - started_at).total_seconds()
            if startup_age > startup_grace_seconds:
                raise MarketDataStaleError(
                    f"no market data after {startup_age:.1f} seconds"
                )
            continue
        age = (now - observed_at).total_seconds()
        if age > stale_after_seconds:
            raise MarketDataStaleError(f"market data stale by {age:.1f} seconds")


class CaptureSubscriptionApplier(Protocol):
    async def apply_symbols(
        self,
        symbols: frozenset[str],
        *,
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None: ...


class CaptureUniverseObserver:
    def __init__(
        self,
        capture: CaptureSubscriptionApplier,
        *,
        streams: tuple[CaptureStream, ...],
        initial_generation: int,
        prewarm_retention_minutes: int = 0,
        full_stream_max_gainer_rank: int = 0,
        must_warm_max_gainer_rank: int = 0,
        protected_symbol_loader: (
            Callable[[], Awaitable[frozenset[str]]] | None
        ) = None,
        on_symbols_changed: Callable[[frozenset[str]], None] | None = None,
        on_trade_symbols_promoted: (
            Callable[[frozenset[str]], Awaitable[None]] | None
        ) = None,
    ) -> None:
        self._capture = capture
        self._streams = streams
        self._generation = initial_generation
        if prewarm_retention_minutes < 0:
            raise ValueError("prewarm_retention_minutes must be non-negative")
        if full_stream_max_gainer_rank < 0:
            raise ValueError("full_stream_max_gainer_rank must be non-negative")
        if must_warm_max_gainer_rank < 0:
            raise ValueError("must_warm_max_gainer_rank must be non-negative")
        self._prewarm_retention = timedelta(minutes=prewarm_retention_minutes)
        self._full_stream_max_gainer_rank = full_stream_max_gainer_rank
        self._must_warm_max_gainer_rank = must_warm_max_gainer_rank
        self._protected_symbol_loader = protected_symbol_loader
        self._on_symbols_changed = on_symbols_changed
        self._on_trade_symbols_promoted = on_trade_symbols_promoted
        self._lock = asyncio.Lock()
        self._universe_symbols: frozenset[str] | None = None
        self._universe_forced_symbols: frozenset[str] = frozenset()
        self._gainer_rank_by_symbol: dict[str, int] = {}
        self._applied_symbols: frozenset[str] | None = None
        self._prewarm_until_by_symbol: dict[str, datetime] = {}
        self._previous_trade_tier: frozenset[str] | None = None
        # First wall-clock time the symbol entered the per-symbol trade tier.
        # Used to decide whether a T1→T0 promotion already has enough local
        # 15s history or still needs a REST backfill.
        self._trade_tier_joined_at: dict[str, datetime] = {}
        self._must_warm_symbols: frozenset[str] = frozenset()
        self._backfill_task: asyncio.Task[None] | None = None

    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        async with self._lock:
            universe_symbols = frozenset(item.symbol for item in snapshot.memberships)
            self._universe_forced_symbols = frozenset(
                item.symbol
                for item in snapshot.memberships
                if item.status is MembershipStatus.FORCED
            )
            self._gainer_rank_by_symbol = {
                entry.symbol: entry.rank for entry in snapshot.ranking.gainers
            }
            self._universe_symbols = universe_symbols
            await self._apply_symbols(now=snapshot.observed_at)

    @property
    def monitored_symbols(self) -> frozenset[str] | None:
        if self._universe_symbols is None:
            return None
        prewarm = frozenset(self._prewarm_until_by_symbol)
        forced = self._universe_forced_symbols
        applied = self._applied_symbols or frozenset()
        return self._universe_symbols | prewarm | forced | applied

    async def refresh_protected_symbols(self) -> None:
        async with self._lock:
            if self._universe_symbols is None:
                return
            await self._apply_symbols()

    def _active_trade_tier_symbols(
        self,
        *,
        universe_symbols: frozenset[str],
        protected_symbols: frozenset[str],
    ) -> frozenset[str]:
        """Return symbols actively qualifying for per-symbol trade streams (T0/T1)."""

        if self._full_stream_max_gainer_rank <= 0 or not self._gainer_rank_by_symbol:
            return universe_symbols | protected_symbols
        full = {
            symbol
            for symbol in universe_symbols
            if self._gainer_rank_by_symbol.get(symbol, 10**9)
            <= self._full_stream_max_gainer_rank
        }
        full |= self._universe_forced_symbols & universe_symbols
        return frozenset(full) | protected_symbols

    def _update_prewarm_symbols(
        self,
        *,
        current_trade_tier: frozenset[str],
        observed_at: datetime,
    ) -> None:
        """Keep recently exited trade-tier symbols subscribed for re-entry."""

        if (
            self._prewarm_retention > timedelta(0)
            and self._previous_trade_tier is not None
        ):
            left_symbols = self._previous_trade_tier - current_trade_tier
            expiry = observed_at + self._prewarm_retention
            for symbol in left_symbols:
                self._prewarm_until_by_symbol[symbol] = expiry

        for symbol in current_trade_tier:
            self._prewarm_until_by_symbol.pop(symbol, None)
        self._prewarm_until_by_symbol = {
            symbol: expiry
            for symbol, expiry in self._prewarm_until_by_symbol.items()
            if expiry > observed_at
        }
        self._previous_trade_tier = current_trade_tier

    def _trade_stream_symbols(
        self,
        *,
        universe_symbols: frozenset[str],
        protected_symbols: frozenset[str],
        observed_at: datetime,
    ) -> frozenset[str]:
        """Return symbols that keep per-symbol trade streams (T0/T1).

        With ``full_stream_max_gainer_rank == 0`` every monitoring symbol is
        subscribed, matching the historical single-tier behaviour. Above that
        cutoff, only active trade-tier gainers plus recently exited prewarmed
        trade symbols keep per-symbol trade streams.
        """

        active_trade_tier = self._active_trade_tier_symbols(
            universe_symbols=universe_symbols,
            protected_symbols=protected_symbols,
        )
        self._update_prewarm_symbols(
            current_trade_tier=active_trade_tier,
            observed_at=observed_at,
        )
        prewarm = frozenset(self._prewarm_until_by_symbol)
        return active_trade_tier | prewarm

    def _compute_must_warm_symbols(
        self,
        *,
        trade_symbols: frozenset[str],
    ) -> frozenset[str]:
        if self._must_warm_max_gainer_rank <= 0:
            return frozenset()
        return frozenset(
            symbol
            for symbol in trade_symbols
            if self._gainer_rank_by_symbol.get(symbol, 10**9)
            <= self._must_warm_max_gainer_rank
        )

    def _symbols_needing_history_backfill(
        self,
        *,
        trade_symbols: frozenset[str],
        added_symbols: frozenset[str],
        previous_must_warm: frozenset[str],
        now: datetime,
        lookback: timedelta,
    ) -> frozenset[str]:
        """Return trade-tier symbols that cannot yet trade on local history.

        Two cases:
        1. Newly entered the trade tier (T2 → T0/T1).
        2. Already in the trade tier but just crossed into the must-warm rank
           band (T1 → T0) before accumulating a full lookback window.
        """

        needing: set[str] = set(added_symbols & trade_symbols)
        if self._must_warm_max_gainer_rank <= 0 or lookback <= timedelta(0):
            return frozenset(needing)
        current_must_warm = self._compute_must_warm_symbols(trade_symbols=trade_symbols)
        newly_must_warm = current_must_warm - previous_must_warm
        for symbol in newly_must_warm:
            if symbol in needing:
                continue
            joined_at = self._trade_tier_joined_at.get(symbol)
            if joined_at is None or now - joined_at < lookback:
                needing.add(symbol)
        return frozenset(needing)

    def _remember_trade_tier_membership(
        self,
        *,
        symbols: frozenset[str],
        previous: frozenset[str] | None,
        now: datetime,
        lookback: timedelta,
        backfilled: frozenset[str],
    ) -> None:
        if previous is None:
            for symbol in symbols:
                self._trade_tier_joined_at[symbol] = now
            return
        for symbol in symbols - previous:
            self._trade_tier_joined_at[symbol] = now
        for symbol in previous - symbols:
            self._trade_tier_joined_at.pop(symbol, None)
        # Treat a successful backfill as if the symbol had been subscribed for
        # the full window so the next universe refresh does not re-fetch.
        warm_at = now - lookback
        for symbol in backfilled:
            joined_at = self._trade_tier_joined_at.get(symbol)
            if joined_at is None or joined_at > warm_at:
                self._trade_tier_joined_at[symbol] = warm_at

    def _schedule_history_backfill(
        self,
        symbols: frozenset[str],
    ) -> None:
        """Run REST backfill off the observer lock so refreshes stay live."""

        if not symbols or self._on_trade_symbols_promoted is None:
            return
        callback = self._on_trade_symbols_promoted
        if self._backfill_task is not None and not self._backfill_task.done():
            log.warning(
                "promotion_backfill_still_running",
                pending_symbols=sorted(symbols)[:_SYMBOL_LOG_LIMIT],
            )
            return

        async def _run() -> None:
            try:
                await callback(symbols)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning(
                    "promotion_backfill_task_failed",
                    error_type=type(error).__name__,
                    error=str(error),
                )

        self._backfill_task = asyncio.create_task(
            _run(),
            name="promotion-history-backfill",
        )

    async def _apply_symbols(self, *, now: datetime | None = None) -> None:
        if self._universe_symbols is None:
            return
        observed_at = datetime.now(tz=UTC) if now is None else now
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        protected_symbols = (
            frozenset()
            if self._protected_symbol_loader is None
            else await self._protected_symbol_loader()
        )
        symbols = self._trade_stream_symbols(
            universe_symbols=self._universe_symbols,
            protected_symbols=protected_symbols,
            observed_at=observed_at,
        )
        lookback = timedelta(minutes=35)
        previous_symbols = self._applied_symbols
        added_symbols = (
            frozenset() if previous_symbols is None else symbols - previous_symbols
        )
        removed_symbols = (
            frozenset() if previous_symbols is None else previous_symbols - symbols
        )
        needing_backfill = self._symbols_needing_history_backfill(
            trade_symbols=symbols,
            added_symbols=added_symbols,
            previous_must_warm=self._must_warm_symbols,
            now=observed_at,
            lookback=lookback,
        )
        current_must_warm = self._compute_must_warm_symbols(trade_symbols=symbols)
        # Optimistically mark membership before the async task finishes so a
        # concurrent refresh does not enqueue the same symbols twice.
        self._remember_trade_tier_membership(
            symbols=symbols,
            previous=previous_symbols,
            now=observed_at,
            lookback=lookback,
            backfilled=needing_backfill,
        )
        self._must_warm_symbols = current_must_warm
        if symbols == self._applied_symbols:
            if needing_backfill:
                self._schedule_history_backfill(needing_backfill)
            return
        # A symbol only produces market-state buckets while it is subscribed, so
        # a symbol that leaves and later re-enters the monitored set shows up
        # downstream as a gap.  Record the membership churn here so the live
        # side can tell "just entered the pool" apart from "buckets were lost".
        self._generation += 1
        await self._capture.apply_symbols(
            symbols,
            streams=self._streams,
            generation=self._generation,
        )
        self._applied_symbols = symbols
        if self._on_symbols_changed is not None:
            self._on_symbols_changed(symbols)
        watch_symbols = self._universe_symbols - symbols
        log.info(
            "capture_symbols_updated",
            universe=len(self._universe_symbols),
            prewarm=len(self._prewarm_until_by_symbol),
            protected=len(protected_symbols - self._universe_symbols),
            total=len(symbols),
            watch_only=len(watch_symbols),
            full_stream_max_gainer_rank=self._full_stream_max_gainer_rank,
            must_warm_max_gainer_rank=self._must_warm_max_gainer_rank,
            added=len(added_symbols),
            removed=len(removed_symbols),
            needing_backfill=len(needing_backfill),
        )
        if added_symbols or removed_symbols:
            log.info(
                "capture_symbols_changed",
                generation=self._generation,
                added=len(added_symbols),
                removed=len(removed_symbols),
                added_symbols=sorted(added_symbols)[:_SYMBOL_LOG_LIMIT],
                removed_symbols=sorted(removed_symbols)[:_SYMBOL_LOG_LIMIT],
            )
        # Startup applies the whole monitoring set in one shot; only later
        # universe refreshes represent a real promotion into the trade tier.
        if previous_symbols is not None and needing_backfill:
            self._schedule_history_backfill(needing_backfill)


async def reconcile_paper_exit_subscriptions(
    observer: CaptureUniverseObserver,
    *,
    interval_seconds: float = _PAPER_EXIT_RECONCILE_SECONDS,
    retry_delay_seconds: float = 5.0,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be non-negative")
    while True:
        await sleeper(interval_seconds)
        try:
            await observer.refresh_protected_symbols()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "paper_exit_subscription_reconcile_failed",
                error=str(error),
            )
            if retry_delay_seconds > 0:
                await sleeper(retry_delay_seconds)


async def prune_expired_raw_archives(
    repository: PostgresCaptureRepository,
    root: Path,
    *,
    retention_days: int,
    now: datetime | None = None,
) -> None:
    observed_at = datetime.now(UTC) if now is None else now
    cutoff_date = retention_cutoff_date(
        now=observed_at,
        retention_days=retention_days,
    )
    manifest_paths = await repository.load_manifest_paths_before(cutoff_date)
    if not manifest_paths:
        return
    result = await asyncio.to_thread(
        delete_archive_files,
        root,
        manifest_paths,
        cutoff_date=cutoff_date,
    )
    deleted_manifests = await repository.delete_manifests(result.removable_paths)
    log.info(
        "raw_archive_retention_pruned",
        cutoff_date=cutoff_date.isoformat(),
        candidate_manifests=len(manifest_paths),
        deleted_files=len(result.removable_paths),
        deleted_manifests=deleted_manifests,
        deleted_bytes=result.deleted_bytes,
        failed_files=len(result.failed_paths),
    )


async def run_raw_archive_retention_loop(
    repository: PostgresCaptureRepository,
    root: Path,
    *,
    retention_days: int,
    interval_seconds: float,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        await sleeper(interval_seconds)
        try:
            await prune_expired_raw_archives(
                repository,
                root,
                retention_days=retention_days,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "raw_archive_retention_failed",
                error=str(error),
            )


async def prune_operational_database_once(
    repository: PostgresOperationalRetentionRepository,
    *,
    contract_metadata_retention_hours: float = _CONTRACT_METADATA_RETENTION_HOURS,
    runtime_state_retention_hours: float = _RUNTIME_STATE_RETENTION_HOURS,
    contract_metadata_batch_size: int = (_CONTRACT_METADATA_RETENTION_BATCH_SIZE),
    runtime_state_batch_size: int = _RUNTIME_STATE_RETENTION_BATCH_SIZE,
    consumer_requirements: tuple[RetentionConsumerRequirement, ...] = (),
    now: datetime | None = None,
) -> None:
    if contract_metadata_retention_hours <= 0:
        raise ValueError("contract_metadata_retention_hours must be positive")
    if runtime_state_retention_hours <= 0:
        raise ValueError("runtime_state_retention_hours must be positive")
    if contract_metadata_batch_size <= 0:
        raise ValueError("contract_metadata_batch_size must be positive")
    if runtime_state_batch_size <= 0:
        raise ValueError("runtime_state_batch_size must be positive")
    observed_at = datetime.now(UTC) if now is None else now
    contract_cutoff = observed_at - timedelta(hours=contract_metadata_retention_hours)
    runtime_cutoff = observed_at - timedelta(hours=runtime_state_retention_hours)
    req_kwargs = (
        {"consumer_requirements": consumer_requirements}
        if consumer_requirements
        else {}
    )
    deleted_contracts = await repository.prune_contract_metadata(
        before=contract_cutoff,
        batch_size=contract_metadata_batch_size,
        **req_kwargs,
    )
    deleted_states = await repository.prune_runtime_market_states(
        before=runtime_cutoff,
        batch_size=runtime_state_batch_size,
        **req_kwargs,
    )
    # Keep tomorrow's event partitions present so live-strategy writers never
    # miss a day boundary.  Deletion stays on the daily archive-and-trim job.
    event_partitions_ensured = (
        await repository.ensure_strategy_runtime_event_partitions()
    )
    if deleted_contracts or deleted_states or event_partitions_ensured:
        log.info(
            "operational_database_retention_pruned",
            contract_metadata_deleted=deleted_contracts,
            runtime_market_states_deleted=deleted_states,
            event_partitions_ensured=event_partitions_ensured,
            contract_metadata_cutoff=contract_cutoff.isoformat(),
            runtime_state_cutoff=runtime_cutoff.isoformat(),
        )


async def run_operational_database_retention_loop(
    repository: PostgresOperationalRetentionRepository,
    *,
    interval_seconds: float = _DATABASE_RETENTION_INTERVAL_SECONDS,
    contract_metadata_retention_hours: float = _CONTRACT_METADATA_RETENTION_HOURS,
    runtime_state_retention_hours: float = _RUNTIME_STATE_RETENTION_HOURS,
    contract_metadata_batch_size: int = (_CONTRACT_METADATA_RETENTION_BATCH_SIZE),
    runtime_state_batch_size: int = _RUNTIME_STATE_RETENTION_BATCH_SIZE,
    consumer_requirements_provider: (
        Callable[[], Awaitable[tuple[RetentionConsumerRequirement, ...]]] | None
    ) = None,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        await sleeper(interval_seconds)
        try:
            async with asyncio.timeout(_DATABASE_RETENTION_MAX_RUNTIME_SECONDS):
                reqs: tuple[RetentionConsumerRequirement, ...] = ()
                if consumer_requirements_provider is not None:
                    try:
                        reqs = await consumer_requirements_provider()
                    except Exception as req_err:
                        log.warning(
                            "operational_retention_consumer_requirements_failed_aborting_prune",
                            error=str(req_err),
                        )
                        continue  # Fail-closed!
                await prune_operational_database_once(
                    repository,
                    contract_metadata_retention_hours=(
                        contract_metadata_retention_hours
                    ),
                    runtime_state_retention_hours=runtime_state_retention_hours,
                    contract_metadata_batch_size=contract_metadata_batch_size,
                    runtime_state_batch_size=runtime_state_batch_size,
                    consumer_requirements=reqs,
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "operational_database_retention_failed",
                error=str(error),
            )


async def _resolve_market_data_consumer_requirements(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[RetentionConsumerRequirement, ...]:
    """Resolve consumer watermarks to protect downstream strategy replay
    and positions.
    """
    requirements: list[RetentionConsumerRequirement] = []
    async with session_factory() as session:
        earliest_checkpoint = await session.scalar(
            select(func.min(StrategyRuntimeCheckpointRow.saved_at))
        )
        if earliest_checkpoint is not None:
            requirements.append(
                RetentionConsumerRequirement(
                    consumer_id="active_strategy_checkpoints",
                    min_required_watermark=earliest_checkpoint,
                    reason="protect active strategy replay and checkpoint baseline",
                )
            )
        earliest_position = await session.scalar(
            select(func.min(AccountPositionSnapshotRow.observed_at)).where(
                AccountPositionSnapshotRow.position_amt != 0
            )
        )
        if earliest_position is not None:
            requirements.append(
                RetentionConsumerRequirement(
                    consumer_id="active_position_market_states",
                    min_required_watermark=earliest_position,
                    reason="protect market states for open positions",
                )
            )
    return tuple(requirements)


@dataclass(frozen=True, slots=True)
class MarketDataRuntime:
    capture: MarketDataCaptureService
    connection_pool: BinanceConnectionPool
    capture_repository: PostgresCaptureRepository
    archive_root: Path
    archive_retention_days: int
    archive_retention_interval_seconds: float
    universe: UniverseRefreshService
    subscription_observer: CaptureUniverseObserver
    runtime_state_publisher: ClosedMarketStatePublisher
    agg_trade_recovery: AggTradeGapRecoverer
    state_hub: MarketStateHub
    quote_hub: MarketQuoteHub
    universe_activation_minute: int
    universe_refresh_interval_minutes: int
    enabled_streams: tuple[CaptureStream, ...]
    initial_symbols: frozenset[str]
    quote_volume_publisher: Binance24hQuoteVolumePublisher | None = None
    daily_open_prefetcher: DailyOpenPrefetcher | None = None
    maintenance_capture_repository: PostgresCaptureRepository | None = None
    operational_retention: PostgresOperationalRetentionRepository | None = None
    database_retention_interval_seconds: float = _DATABASE_RETENTION_INTERVAL_SECONDS
    contract_metadata_retention_hours: float = _CONTRACT_METADATA_RETENTION_HOURS
    runtime_state_retention_hours: float = _RUNTIME_STATE_RETENTION_HOURS
    maintenance_session_factory: async_sessionmaker[AsyncSession] | None = None


def _archive_retention_repository(
    runtime: MarketDataRuntime,
) -> PostgresCaptureRepository:
    return (
        getattr(runtime, "maintenance_capture_repository", None)
        or runtime.capture_repository
    )


@asynccontextmanager
async def build_market_data_runtime(
    config_path: Path,
    *,
    on_durable_state_persisted: Callable[[datetime], None] | None = None,
    startup_timer: StartupPhaseTimer | None = None,
) -> AsyncIterator[MarketDataRuntime]:
    runtime = load_runtime_config(config_path)
    if startup_timer is not None:
        startup_timer.mark(
            "runtime_config_loaded",
            environment=runtime.environment,
        )
    database_url = _market_database_url(runtime.database_url)
    market_engine = create_market_database_engine(database_url)
    observability_engine = create_observability_database_engine(database_url)
    maintenance_engine = create_maintenance_database_engine(database_url)
    market_sessions = async_sessionmaker(market_engine, expire_on_commit=False)
    observability_sessions = async_sessionmaker(
        observability_engine,
        expire_on_commit=False,
    )
    maintenance_sessions = async_sessionmaker(
        maintenance_engine,
        expire_on_commit=False,
    )
    universe_repository = PostgresUniverseRepository(market_sessions)
    capture_repository = PostgresCaptureRepository(observability_sessions)
    maintenance_capture_repository = PostgresCaptureRepository(maintenance_sessions)
    operational_retention = PostgresOperationalRetentionRepository(maintenance_sessions)
    paper_repository = PostgresPaperDaemonRepository(maintenance_sessions)
    account_repository = PostgresAccountRepository(maintenance_sessions)
    runtime_state_repository = PostgresRuntimeMarketStateRepository(market_sessions)
    if startup_timer is not None:
        startup_timer.mark("database_resources_created")
    state_hub = MarketStateHub(
        MarketStateHubConfig(
            host=os.environ.get(
                _MARKET_STATE_HUB_HOST_ENV,
                _MARKET_STATE_HUB_DEFAULT_HOST,
            ),
            port=parse_market_state_hub_port(),
        )
    )
    quote_hub = MarketQuoteHub(
        MarketQuoteHubConfig(
            host=os.environ.get(
                _MARKET_QUOTE_HUB_HOST_ENV,
                _MARKET_QUOTE_HUB_DEFAULT_HOST,
            ),
            port=parse_market_quote_hub_port(),
        )
    )
    realtime_closure_delay_seconds = (
        float(os.environ["CML_REALTIME_CLOSURE_DELAY_SECONDS"])
        if "CML_REALTIME_CLOSURE_DELAY_SECONDS" in os.environ
        else runtime.capture.realtime_closure_delay_seconds
    )
    runtime_state_publisher = ClosedMarketStatePublisher(
        repository=runtime_state_repository,
        config=ClosedMarketStatePublisherConfig(
            realtime_closure_delay_seconds=realtime_closure_delay_seconds,
            durable_closure_delay_seconds=(
                runtime.capture.durable_closure_delay_seconds
            ),
        ),
        realtime_state_sink=state_hub.publish,
        realtime_quote_sink=quote_hub.publish,
        on_durable_state_persisted=on_durable_state_persisted,
    )
    protected_run_ids = parse_paper_exit_run_ids()
    configured_live_position_account_labels = parse_live_position_account_labels()

    async def load_protected_symbols() -> frozenset[str]:
        return await _load_protected_symbols(
            paper_repository=paper_repository,
            account_repository=account_repository,
            protected_run_ids=protected_run_ids,
            configured_live_position_account_labels=(
                configured_live_position_account_labels
            ),
        )

    persisted_memberships = await universe_repository.load_active_memberships()
    initial_memberships = {
        symbol: membership
        for symbol, membership in persisted_memberships.items()
        if membership.status is not MembershipStatus.RETAINED
    }
    initial_symbols = frozenset(initial_memberships) | await load_protected_symbols()
    runtime_state_publisher.set_expected_symbols(initial_symbols)
    enabled_streams = tuple(
        CaptureStream(item) for item in runtime.capture.enabled_streams
    )
    if startup_timer is not None:
        startup_timer.mark(
            "initial_symbols_loaded",
            membership_count=len(initial_memberships),
            legacy_retained_membership_count=(
                len(persisted_memberships) - len(initial_memberships)
            ),
            initial_symbol_count=len(initial_symbols),
            stream_count=len(enabled_streams),
        )
    capture_version = behavior_hash(runtime)
    archive_config = runtime.capture.archive
    archive_config.root.mkdir(parents=True, exist_ok=True)
    manifest_journal = PendingManifestJournal(
        archive_config.root / ".pending-manifests"
    )
    quality = StreamQualityTracker(
        silence_timeout_seconds=runtime.capture.silence_timeout_seconds
    )
    archive_streams = (
        None
        if archive_config.streams is None
        else frozenset(CaptureStream(item) for item in archive_config.streams)
    )
    queue = BoundedEnvelopeQueue(
        max_events=runtime.capture.queue_max_events,
        max_bytes=runtime.capture.queue_max_bytes,
        coalescing_streams=(
            frozenset({CaptureStream.BOOK_TICKER})
            if (
                archive_streams is not None
                and CaptureStream.BOOK_TICKER not in archive_streams
            )
            else frozenset()
        ),
        coalescing_interval_seconds=(
            runtime.capture.book_ticker_coalescing_interval_seconds
        ),
        backpressure_timeout_seconds=(runtime.capture.backpressure_timeout_seconds),
    )

    async def save_manifest(manifest: ArchiveManifest) -> None:
        try:
            await maintenance_capture_repository.save_manifest(manifest)
        except SQLAlchemyError:
            await manifest_journal.append(manifest)

    recovered_manifest_count = 0
    for recovery_result in await recover_archive_root(
        archive_config.root,
        environment=runtime.environment,
        capture_version=capture_version,
    ):
        await save_manifest(recovery_result.manifest)
        recovered_manifest_count += 1
    if startup_timer is not None:
        startup_timer.mark(
            "archive_recovery_completed",
            recovered_manifest_count=recovered_manifest_count,
        )

    async def save_replayed_manifest(manifest: ArchiveManifest) -> None:
        # A replay must fail and leave its journal entry in place when the
        # database is still unavailable. Calling the normal fallback writer
        # here would append the same entry and then let replay delete it.
        await maintenance_capture_repository.save_manifest(manifest)

    replayed_manifest_count = await manifest_journal.replay(save_replayed_manifest)
    if replayed_manifest_count:
        log.info(
            "pending_manifest_journal_replayed",
            count=replayed_manifest_count,
        )
    if startup_timer is not None:
        startup_timer.mark(
            "manifest_journal_replayed",
            replayed_manifest_count=replayed_manifest_count,
        )

    await prune_expired_raw_archives(
        maintenance_capture_repository,
        archive_config.root,
        retention_days=archive_config.retention_days,
    )
    if startup_timer is not None:
        startup_timer.mark("archive_retention_checked")

    archive = ZstdJsonlArchive(
        root=archive_config.root,
        environment=runtime.environment,
        capture_version=capture_version,
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: quality.known_gap_count(
            connection_session_id=key.connection_session_id,
            stream=key.stream,
            symbol=key.symbol,
        ),
        zstd_level=archive_config.zstd_level,
        rotation_uncompressed_bytes=archive_config.rotation_uncompressed_bytes,
        max_open_writers=archive_config.max_open_writers,
        group_commit_max_events=archive_config.group_commit_max_events,
        group_commit_max_milliseconds=(archive_config.group_commit_max_milliseconds),
    )
    rest_client = BinanceUsdMRestClient(str(runtime.binance_base_url))
    agg_trade_recovery = AggTradeGapRecoverer(rest_client)
    daily_open_prefetcher = DailyOpenPrefetcher(
        rest_client,
        universe_repository,
    )
    observer: CaptureUniverseObserver | None = None
    quote_volume_publisher = Binance24hQuoteVolumePublisher(
        rest_client,
        publish=quote_hub.publish_volume,
        symbols_filter=lambda: (
            observer.monitored_symbols if observer is not None else None
        ),
        environment=runtime.environment,
    )

    async def handle_agg_trade_gap(gap: AggTradeGap) -> None:
        await runtime_state_publisher.mark_incomplete(gap)
        await capture_repository.save_quality_event(agg_trade_gap_quality_event(gap))

    coordinator = CaptureCoordinator(
        queue=queue,
        archive=archive,
        quality=quality,
        repository=capture_repository,
        acknowledgement_sink=None,
        realtime_envelope_sink=runtime_state_publisher.observe,
        envelope_recovery=agg_trade_recovery,
        gap_sink=handle_agg_trade_gap,
        archive_streams=archive_streams,
    )

    capture: MarketDataCaptureService

    async def on_capture_envelope(envelope: RawEnvelope) -> None:
        await capture.submit(envelope)

    def connection_factory(group: SubscriptionGroup) -> BinanceWebSocketConnection:
        base_url = (
            str(runtime.capture.public_websocket_url)
            if group.route is CaptureRoute.PUBLIC
            else str(runtime.capture.market_websocket_url)
        )
        return BinanceWebSocketConnection(
            base_url=base_url,
            group_id=group.group_id,
            route=group.route,
            environment=runtime.environment,
            desired_names=tuple(item.binance_name for item in group.subscriptions),
            generation=1,
            on_envelope=on_capture_envelope,
            on_lifecycle=coordinator.observe_lifecycle,
            reconnect_delays=(0.0, 1.0, 5.0),
            connection_lifetime_seconds=(runtime.capture.connection_lifetime_seconds),
            open_timeout_seconds=runtime.capture.open_timeout_seconds,
            ping_interval_seconds=runtime.capture.ping_interval_seconds,
            ping_timeout_seconds=runtime.capture.ping_timeout_seconds,
            silence_timeout_seconds=runtime.capture.silence_timeout_seconds,
            control_ack_timeout_seconds=(runtime.capture.control_ack_timeout_seconds),
            control_messages_per_second=(runtime.capture.control_messages_per_second),
            ingress_queue_max_events=(runtime.capture.ingress_queue_max_events),
            symbol_filter=coordinator.accepts_symbol,
            on_realtime_envelope=runtime_state_publisher.observe_realtime_quote,
        )

    connection_pool = BinanceConnectionPool(
        connection_factory=connection_factory,
        max_subscriptions_per_connection=(
            runtime.capture.max_subscriptions_per_connection
        ),
        control_messages_per_second=(runtime.capture.control_messages_per_second),
        max_subscriptions_per_connection_by_stream=(
            {
                CaptureStream.BOOK_TICKER: (
                    runtime.capture.book_ticker_max_subscriptions_per_connection
                )
            }
            if (
                runtime.capture.book_ticker_max_subscriptions_per_connection is not None
            )
            else None
        ),
        use_all_book_ticker_stream=(runtime.capture.book_ticker_use_all_stream),
    )
    capture = MarketDataCaptureService(
        queue=queue,
        repository=capture_repository,
        connection_pool=connection_pool,
        disk_guard=DiskSpaceGuard(
            warning_free_bytes=archive_config.warning_free_bytes,
            halt_free_bytes=archive_config.halt_free_bytes,
            recovery_free_bytes=archive_config.recovery_free_bytes,
        ),
        disk_free_bytes_provider=lambda: shutil.disk_usage(archive_config.root).free,
        coordinator=coordinator,
    )
    promotion_backfiller = PromotionHistoryBackfiller(
        client=rest_client,
        publisher=state_hub,
        environment=runtime.environment,
    )
    observer = CaptureUniverseObserver(
        capture,
        streams=enabled_streams,
        initial_generation=1,
        prewarm_retention_minutes=runtime.universe.prewarm_retention_minutes,
        full_stream_max_gainer_rank=(runtime.universe.full_stream_max_gainer_rank),
        # TARGET / entry-adjacent band: T1→T0 must already have a full local
        # 15s window, not merely be subscribed.
        must_warm_max_gainer_rank=runtime.universe.top_count,
        protected_symbol_loader=load_protected_symbols,
        on_symbols_changed=runtime_state_publisher.set_expected_symbols,
        on_trade_symbols_promoted=promotion_backfiller.backfill_symbols,
    )
    universe = UniverseRefreshService(
        market_data=rest_client,
        repository=universe_repository,
        config=runtime.universe,
        config_hash=capture_version,
        observer=observer,
        daily_open_prefetcher=daily_open_prefetcher,
    )
    if startup_timer is not None:
        startup_timer.mark("runtime_components_built")
    try:
        yield MarketDataRuntime(
            capture=capture,
            connection_pool=connection_pool,
            capture_repository=capture_repository,
            archive_root=archive_config.root,
            archive_retention_days=archive_config.retention_days,
            archive_retention_interval_seconds=(
                archive_config.retention_check_interval_seconds
            ),
            universe=universe,
            subscription_observer=observer,
            runtime_state_publisher=runtime_state_publisher,
            agg_trade_recovery=agg_trade_recovery,
            state_hub=state_hub,
            quote_hub=quote_hub,
            quote_volume_publisher=quote_volume_publisher,
            daily_open_prefetcher=daily_open_prefetcher,
            universe_activation_minute=runtime.universe.activation_minute,
            universe_refresh_interval_minutes=(
                runtime.universe.refresh_interval_minutes
            ),
            enabled_streams=enabled_streams,
            initial_symbols=initial_symbols,
            maintenance_capture_repository=maintenance_capture_repository,
            operational_retention=operational_retention,
            maintenance_session_factory=maintenance_sessions,
        )
    finally:
        await rest_client.aclose()
        await market_engine.dispose()
        await observability_engine.dispose()
        await maintenance_engine.dispose()


@app.command()
def refresh_universe(
    at: str | None = typer.Option(None, "--at"),
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        observed_at = parse_observed_at(at)
        snapshot = asyncio.run(refresh_once(resolve_config_path(config), observed_at))
    except Exception as error:
        typer.echo(f"refresh failed: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(format_snapshot(snapshot))


async def run_scheduler(config_path: Path) -> None:
    async with build_refresh_service(config_path) as (
        service,
        activation_minute,
        refresh_interval_minutes,
    ):
        log.info(
            "universe_scheduler_started",
            activation_minute=activation_minute,
            refresh_interval_minutes=refresh_interval_minutes,
        )
        await run_scheduler_loop(
            LoggingRefreshService(service),
            activation_minute=activation_minute,
            refresh_interval_minutes=refresh_interval_minutes,
        )


async def run_market_data(
    config_path: Path,
    *,
    stop_requested: asyncio.Event | None = None,
) -> None:
    startup_timer = StartupPhaseTimer(
        log,
        event="market_data_startup_phase",
        service="market-data",
    )
    log.info(
        "market_data_startup_started",
        config_path=str(config_path),
    )
    configure_tracemalloc()
    health = LocalHealthWriter.from_environment()

    first_durable_state_logged = False

    def on_durable_state_persisted(watermark: datetime) -> None:
        nonlocal first_durable_state_logged
        if not first_durable_state_logged:
            startup_timer.mark(
                "first_durable_state_persisted",
                watermark=watermark.isoformat(),
            )
            first_durable_state_logged = True
        if health is not None:
            health.heartbeat(database_ok=True)

    health_callback = on_durable_state_persisted
    async with build_market_data_runtime(
        config_path,
        on_durable_state_persisted=health_callback,
        startup_timer=startup_timer,
    ) as runtime:
        capture_task: asyncio.Task[None] | None = None
        auxiliary_tasks: tuple[asyncio.Task[None], ...] = ()
        stop_task: asyncio.Task[bool] | None = None
        quote_hub = getattr(runtime, "quote_hub", None)
        quote_volume_publisher = getattr(runtime, "quote_volume_publisher", None)
        daily_open_prefetcher = getattr(runtime, "daily_open_prefetcher", None)
        try:
            await runtime.state_hub.start()
            startup_timer.mark("state_hub_started")
            if quote_hub is not None:
                await quote_hub.start()
                startup_timer.mark("quote_hub_started")
            else:
                startup_timer.mark("quote_hub_skipped")
            await runtime.runtime_state_publisher.start()
            startup_timer.mark("runtime_state_publisher_started")
            await runtime.capture.start(
                symbols=runtime.initial_symbols,
                streams=runtime.enabled_streams,
                generation=1,
            )
            startup_timer.mark(
                "capture_started",
                initial_symbol_count=len(runtime.initial_symbols),
                stream_count=len(runtime.enabled_streams),
            )
            startup_observed_at = datetime.now(UTC).replace(
                second=0,
                microsecond=0,
            )
            if daily_open_prefetcher is not None:
                await daily_open_prefetcher.bootstrap_current_day(startup_observed_at)
                startup_timer.mark("daily_open_bootstrap_completed")
                await daily_open_prefetcher.start()
                startup_timer.mark("daily_open_prefetch_started")
            else:
                startup_timer.mark("daily_open_prefetch_skipped")
            startup_snapshot = await runtime.universe.refresh(
                observed_at=startup_observed_at
            )
            log.info(
                "universe_startup_refresh",
                observed_at=startup_snapshot.observed_at.isoformat(),
            )
            startup_timer.mark(
                "universe_startup_refresh_completed",
                membership_count=len(startup_snapshot.memberships),
                target_symbol_count=len(startup_snapshot.ranking.target_symbols),
            )
            if quote_volume_publisher is not None:
                await quote_volume_publisher.start()
                startup_timer.mark("quote_volume_publisher_started")
            else:
                startup_timer.mark("quote_volume_publisher_skipped")
            capture_task = asyncio.create_task(runtime.capture.run())
            startup_timer.mark("capture_task_scheduled")
            auxiliary_tasks = (
                asyncio.create_task(
                    run_scheduler_loop(
                        LoggingRefreshService(runtime.universe),
                        activation_minute=(runtime.universe_activation_minute),
                        refresh_interval_minutes=(
                            runtime.universe_refresh_interval_minutes
                        ),
                    )
                ),
                asyncio.create_task(
                    monitor_market_data_freshness(
                        latest_observed_at=lambda: (
                            runtime.runtime_state_publisher.metrics.latest_watermark_at
                        )
                    )
                ),
                asyncio.create_task(
                    monitor_market_data_health(
                        capture_metrics=runtime.capture.metrics_snapshot,
                        connection_metrics=(runtime.connection_pool.metrics_snapshot),
                        runtime_state_metrics=(
                            runtime.runtime_state_publisher.lateness_metrics_snapshot
                        ),
                        recovery_metrics=lambda: runtime.agg_trade_recovery.metrics,
                    )
                ),
                asyncio.create_task(
                    reconcile_paper_exit_subscriptions(runtime.subscription_observer)
                ),
                asyncio.create_task(
                    run_raw_archive_retention_loop(
                        _archive_retention_repository(runtime),
                        runtime.archive_root,
                        retention_days=runtime.archive_retention_days,
                        interval_seconds=runtime.archive_retention_interval_seconds,
                    )
                ),
            )
            operational_retention = getattr(
                runtime,
                "operational_retention",
                None,
            )
            if operational_retention is not None:
                maintenance_sessions = getattr(
                    runtime,
                    "maintenance_session_factory",
                    None,
                )
                consumer_req_provider = None
                if maintenance_sessions is not None:

                    async def consumer_req_provider() -> (
                        tuple[RetentionConsumerRequirement, ...]
                    ):
                        return await _resolve_market_data_consumer_requirements(
                            maintenance_sessions
                        )
                auxiliary_tasks += (
                    asyncio.create_task(
                        run_operational_database_retention_loop(
                            operational_retention,
                            interval_seconds=(
                                runtime.database_retention_interval_seconds
                            ),
                            contract_metadata_retention_hours=(
                                runtime.contract_metadata_retention_hours
                            ),
                            runtime_state_retention_hours=(
                                runtime.runtime_state_retention_hours
                            ),
                            consumer_requirements_provider=consumer_req_provider,
                        )
                    ),
                )
            startup_timer.mark(
                "background_tasks_started",
                auxiliary_task_count=len(auxiliary_tasks),
            )
            if health is not None:
                # Publish readiness as soon as capture is running instead of
                # waiting for the first 15s state to land. The durable-state
                # callback keeps refreshing the marker afterwards, so a stalled
                # persistence path still lets it expire.
                health.heartbeat(database_ok=True)
            monitored_tasks: tuple[asyncio.Task[object], ...] = (
                capture_task,
                *auxiliary_tasks,
            )
            if stop_requested is not None:
                stop_task = asyncio.create_task(stop_requested.wait())
                monitored_tasks = (*monitored_tasks, stop_task)
            done, _ = await asyncio.wait(
                monitored_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task is not None and stop_task in done:
                return
            completed_service_task = next(iter(done))
            await completed_service_task
            raise RuntimeError("market-data service task stopped unexpectedly")
        finally:
            if stop_task is not None:
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
            for task in auxiliary_tasks:
                task.cancel()
            if auxiliary_tasks:
                await asyncio.gather(
                    *auxiliary_tasks,
                    return_exceptions=True,
                )
            try:
                async with asyncio.timeout(_CAPTURE_STOP_TIMEOUT_SECONDS):
                    await runtime.capture.stop()
            except TimeoutError:
                log.error(
                    "market_data_capture_stop_timed_out",
                    timeout_seconds=_CAPTURE_STOP_TIMEOUT_SECONDS,
                )
            if capture_task is not None and not capture_task.done():
                try:
                    await asyncio.wait_for(capture_task, timeout=1)
                except TimeoutError:
                    capture_task.cancel()
            if capture_task is not None:
                await asyncio.gather(capture_task, return_exceptions=True)
            await runtime.runtime_state_publisher.stop()
            if daily_open_prefetcher is not None:
                await daily_open_prefetcher.stop()
            if quote_volume_publisher is not None:
                await quote_volume_publisher.stop()
            if quote_hub is not None:
                await quote_hub.stop()
            await runtime.state_hub.stop()
    if health is not None:
        health.stopped()


async def run_market_data_until_stopped(
    config_path: Path,
    stop_requested: asyncio.Event,
) -> None:
    await run_market_data(
        config_path,
        stop_requested=stop_requested,
    )


async def run_market_data_with_signal_handlers(config_path: Path) -> None:
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []

    def request_stop(signal_name: str) -> None:
        if not stop_requested.is_set():
            log.info("market_data_stop_requested", signal=signal_name)
        stop_requested.set()

    for shutdown_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                shutdown_signal,
                request_stop,
                shutdown_signal.name,
            )
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(shutdown_signal)
    try:
        await run_market_data_until_stopped(config_path, stop_requested)
    finally:
        for shutdown_signal in registered_signals:
            loop.remove_signal_handler(shutdown_signal)


async def run_market_data_for(config_path: Path, *, seconds: float) -> None:
    stop_requested = asyncio.Event()
    timer_task = asyncio.create_task(_request_stop_after(seconds, stop_requested))
    try:
        await run_market_data(
            config_path,
            stop_requested=stop_requested,
        )
    finally:
        timer_task.cancel()
        await asyncio.gather(timer_task, return_exceptions=True)


async def _request_stop_after(
    seconds: float,
    stop_requested: asyncio.Event,
) -> None:
    await asyncio.sleep(seconds)
    stop_requested.set()


@app.command()
def run_universe_scheduler(
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        asyncio.run(run_scheduler(resolve_config_path(config)))
    except KeyboardInterrupt:
        log.info("universe_scheduler_stopped")


@app.command("run-market-data")
def run_market_data_command(
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        _run_market_data(
            run_market_data_with_signal_handlers(resolve_config_path(config))
        )
    except KeyboardInterrupt:
        log.info("market_data_stopped")


@app.command("partition-runtime-states")
def partition_runtime_states_command(
    phase: str = typer.Option("prepare", "--phase"),
    lookahead_hours: float = typer.Option(168.0, "--lookahead-hours"),
    confirm_writer_paused: bool = typer.Option(
        False,
        "--confirm-writer-paused",
        help="Required for cutover; market-data must already be stopped.",
    ),
) -> None:
    """Prepare or cut over the runtime market-state partitioned table."""

    database_url = resolve_database_url(
        None,
        "CML_MARKET_DATABASE_URL",
        "CML_DATABASE_URL",
    )
    if not database_url:
        raise typer.BadParameter(
            "CML_MARKET_DATABASE_URL or CML_DATABASE_URL is required"
        )
    if phase not in {"prepare", "cutover"}:
        raise typer.BadParameter("--phase must be prepare or cutover")
    if lookahead_hours <= 0:
        raise typer.BadParameter("--lookahead-hours must be positive")
    if phase == "cutover" and not confirm_writer_paused:
        raise typer.BadParameter("--confirm-writer-paused is required for cutover")

    async def run() -> None:
        engine = create_partitioning_database_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            if phase == "prepare":
                prepare_report = await prepare_runtime_state_partition(
                    session_factory,
                    lookahead=timedelta(hours=lookahead_hours),
                )
                typer.echo(
                    " ".join(
                        (
                            "prepared",
                            f"source_rows={prepare_report.source_rows}",
                            f"shadow_rows={prepare_report.shadow_rows}",
                            f"partitions_created={prepare_report.partitions_created}",
                            f"first_partition={prepare_report.first_partition_start.isoformat()}",
                            f"last_partition_end={prepare_report.last_partition_end.isoformat()}",
                        )
                    )
                )
            else:
                cutover_report = await cutover_runtime_state_partition(session_factory)
                typer.echo(
                    " ".join(
                        (
                            "cut over",
                            f"rows_copied={cutover_report.rows_copied_during_cutover}",
                            f"source_rows={cutover_report.source_rows}",
                            f"partitioned_rows={cutover_report.shadow_rows}",
                            f"legacy_table={cutover_report.legacy_table}",
                        )
                    )
                )
        finally:
            await engine.dispose()

    _run_market_data(run())
