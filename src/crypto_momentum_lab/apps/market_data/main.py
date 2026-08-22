import asyncio
import os
import signal
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

import structlog
import typer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config.loader import (
    behavior_hash,
    load_runtime_config,
)
from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
)
from crypto_momentum_lab.domain.universe.models import UniverseSnapshot
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
from crypto_momentum_lab.persistence.postgres.operational_retention import (
    PostgresOperationalRetentionRepository,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive
from crypto_momentum_lab.persistence.raw_files.journal import PendingManifestJournal
from crypto_momentum_lab.persistence.raw_files.recovery import recover_archive_root
from crypto_momentum_lab.persistence.raw_files.retention import (
    delete_archive_files,
    retention_cutoff_date,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()

_UNIVERSE_REFRESH_TIMEOUT_SECONDS = 120.0
_MARKET_DATA_STARTUP_GRACE_SECONDS = 120.0
_MARKET_DATA_STALE_AFTER_SECONDS = 120.0
_MARKET_DATA_WATCHDOG_INTERVAL_SECONDS = 15.0
_CAPTURE_STOP_TIMEOUT_SECONDS = 55.0
_PAPER_EXIT_RECONCILE_SECONDS = 15.0
_DATABASE_RETENTION_INTERVAL_SECONDS = 60.0
_CONTRACT_METADATA_RETENTION_HOURS = 6.0
_RUNTIME_STATE_RETENTION_HOURS = 48.0
_CONTRACT_METADATA_RETENTION_BATCH_SIZE = 1_000
_RUNTIME_STATE_RETENTION_BATCH_SIZE = 1_000
_PAPER_EXIT_RUN_IDS_ENV = "CML_PAPER_EXIT_RUN_IDS"
_LIVE_POSITION_ACCOUNT_LABEL_ENV = "CML_LIVE_POSITION_ACCOUNT_LABEL"
_MARKET_STATE_HUB_HOST_ENV = "CML_MARKET_STATE_HUB_HOST"
_MARKET_STATE_HUB_PORT_ENV = "CML_MARKET_STATE_HUB_PORT"
_MARKET_STATE_HUB_DEFAULT_HOST = "0.0.0.0"
_MARKET_STATE_HUB_DEFAULT_PORT = 8766


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


def parse_paper_exit_run_ids(value: str | None = None) -> frozenset[str]:
    raw_value = (
        os.environ.get(_PAPER_EXIT_RUN_IDS_ENV, "")
        if value is None
        else value
    )
    return frozenset(
        run_id
        for item in raw_value.split(",")
        if (run_id := item.strip())
    )


def parse_live_position_account_label(value: str | None = None) -> str | None:
    raw_value = (
        os.environ.get(_LIVE_POSITION_ACCOUNT_LABEL_ENV, "")
        if value is None
        else value
    )
    normalized = raw_value.strip()
    return normalized or None


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


@asynccontextmanager
async def build_refresh_service(
    config_path: Path,
) -> AsyncIterator[tuple[UniverseRefreshService, int, int]]:
    runtime = load_runtime_config(config_path)
    engine = create_async_database_engine(runtime.database_url)
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
    eligible = (
        len(snapshot.ranking.candidates) - len(snapshot.ranking.exclusions)
    )
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
        eligible=(
            len(snapshot.ranking.candidates)
            - len(snapshot.ranking.exclusions)
        ),
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
            raise MarketDataStaleError(
                f"market data stale by {age:.1f} seconds"
            )


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
        protected_symbol_loader: (
            Callable[[], Awaitable[frozenset[str]]] | None
        ) = None,
    ) -> None:
        self._capture = capture
        self._streams = streams
        self._generation = initial_generation
        self._protected_symbol_loader = protected_symbol_loader
        self._lock = asyncio.Lock()
        self._universe_symbols: frozenset[str] | None = None
        self._applied_symbols: frozenset[str] | None = None

    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        async with self._lock:
            self._universe_symbols = frozenset(
                item.symbol for item in snapshot.memberships
            )
            await self._apply_symbols()

    async def refresh_protected_symbols(self) -> None:
        async with self._lock:
            if self._universe_symbols is None:
                return
            await self._apply_symbols()

    async def _apply_symbols(self) -> None:
        if self._universe_symbols is None:
            return
        protected_symbols = (
            frozenset()
            if self._protected_symbol_loader is None
            else await self._protected_symbol_loader()
        )
        symbols = self._universe_symbols | protected_symbols
        if symbols == self._applied_symbols:
            return
        self._generation += 1
        await self._capture.apply_symbols(
            symbols,
            streams=self._streams,
            generation=self._generation,
        )
        self._applied_symbols = symbols
        log.info(
            "capture_symbols_updated",
            universe=len(self._universe_symbols),
            protected=len(protected_symbols - self._universe_symbols),
            total=len(symbols),
        )


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
    deleted_manifests = await repository.delete_manifests(
        result.removable_paths
    )
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
    contract_metadata_batch_size: int = (
        _CONTRACT_METADATA_RETENTION_BATCH_SIZE
    ),
    runtime_state_batch_size: int = _RUNTIME_STATE_RETENTION_BATCH_SIZE,
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
    contract_cutoff = observed_at - timedelta(
        hours=contract_metadata_retention_hours
    )
    runtime_cutoff = observed_at - timedelta(hours=runtime_state_retention_hours)
    deleted_contracts = await repository.prune_contract_metadata(
        before=contract_cutoff,
        batch_size=contract_metadata_batch_size,
    )
    deleted_states = await repository.prune_runtime_market_states(
        before=runtime_cutoff,
        batch_size=runtime_state_batch_size,
    )
    if deleted_contracts or deleted_states:
        log.info(
            "operational_database_retention_pruned",
            contract_metadata_deleted=deleted_contracts,
            runtime_market_states_deleted=deleted_states,
            contract_metadata_cutoff=contract_cutoff.isoformat(),
            runtime_state_cutoff=runtime_cutoff.isoformat(),
        )


async def run_operational_database_retention_loop(
    repository: PostgresOperationalRetentionRepository,
    *,
    interval_seconds: float = _DATABASE_RETENTION_INTERVAL_SECONDS,
    contract_metadata_retention_hours: float = _CONTRACT_METADATA_RETENTION_HOURS,
    runtime_state_retention_hours: float = _RUNTIME_STATE_RETENTION_HOURS,
    contract_metadata_batch_size: int = (
        _CONTRACT_METADATA_RETENTION_BATCH_SIZE
    ),
    runtime_state_batch_size: int = _RUNTIME_STATE_RETENTION_BATCH_SIZE,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        await sleeper(interval_seconds)
        try:
            await prune_operational_database_once(
                repository,
                contract_metadata_retention_hours=contract_metadata_retention_hours,
                runtime_state_retention_hours=runtime_state_retention_hours,
                contract_metadata_batch_size=contract_metadata_batch_size,
                runtime_state_batch_size=runtime_state_batch_size,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "operational_database_retention_failed",
                error=str(error),
            )


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
    state_hub: MarketStateHub
    universe_activation_minute: int
    universe_refresh_interval_minutes: int
    enabled_streams: tuple[CaptureStream, ...]
    initial_symbols: frozenset[str]
    operational_retention: PostgresOperationalRetentionRepository | None = None
    database_retention_interval_seconds: float = (
        _DATABASE_RETENTION_INTERVAL_SECONDS
    )
    contract_metadata_retention_hours: float = _CONTRACT_METADATA_RETENTION_HOURS
    runtime_state_retention_hours: float = _RUNTIME_STATE_RETENTION_HOURS


@asynccontextmanager
async def build_market_data_runtime(
    config_path: Path,
) -> AsyncIterator[MarketDataRuntime]:
    runtime = load_runtime_config(config_path)
    engine = create_async_database_engine(runtime.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    universe_repository = PostgresUniverseRepository(sessions)
    capture_repository = PostgresCaptureRepository(sessions)
    operational_retention = PostgresOperationalRetentionRepository(sessions)
    paper_repository = PostgresPaperDaemonRepository(sessions)
    account_repository = PostgresAccountRepository(sessions)
    runtime_state_repository = PostgresRuntimeMarketStateRepository(sessions)
    state_hub = MarketStateHub(
        MarketStateHubConfig(
            host=os.environ.get(
                _MARKET_STATE_HUB_HOST_ENV,
                _MARKET_STATE_HUB_DEFAULT_HOST,
            ),
            port=parse_market_state_hub_port(),
        )
    )
    runtime_state_publisher = ClosedMarketStatePublisher(
        repository=runtime_state_repository,
        config=ClosedMarketStatePublisherConfig(
            closure_delay_seconds=runtime.capture.closure_delay_seconds
        ),
        realtime_state_sink=state_hub.publish,
    )
    protected_run_ids = parse_paper_exit_run_ids()
    live_position_account_label = parse_live_position_account_label()

    async def load_protected_symbols() -> frozenset[str]:
        paper_symbols = await paper_repository.load_open_position_symbols(
            protected_run_ids
        )
        if live_position_account_label is None:
            return paper_symbols
        live_symbols = await account_repository.load_active_position_symbols(
            environment="live",
            account_label=live_position_account_label,
        )
        return paper_symbols | live_symbols

    initial_memberships = await universe_repository.load_active_memberships()
    initial_symbols = (
        frozenset(initial_memberships) | await load_protected_symbols()
    )
    enabled_streams = tuple(
        CaptureStream(item) for item in runtime.capture.enabled_streams
    )
    capture_version = behavior_hash(runtime)
    archive_config = runtime.capture.archive
    manifest_journal = PendingManifestJournal(
        archive_config.root / ".pending-manifests"
    )
    quality = StreamQualityTracker(
        silence_timeout_seconds=runtime.capture.silence_timeout_seconds
    )
    archive_streams = (
        None
        if archive_config.streams is None
        else frozenset(
            CaptureStream(item) for item in archive_config.streams
        )
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
    )

    async def save_manifest(manifest: ArchiveManifest) -> None:
        try:
            await capture_repository.save_manifest(manifest)
        except SQLAlchemyError:
            await manifest_journal.append(manifest)

    for recovery_result in await recover_archive_root(
        archive_config.root,
        environment=runtime.environment,
        capture_version=capture_version,
    ):
        await save_manifest(recovery_result.manifest)

    await prune_expired_raw_archives(
        capture_repository,
        archive_config.root,
        retention_days=archive_config.retention_days,
    )

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
        group_commit_max_milliseconds=(
            archive_config.group_commit_max_milliseconds
        ),
    )
    coordinator = CaptureCoordinator(
        queue=queue,
        archive=archive,
        quality=quality,
        repository=capture_repository,
        acknowledgement_sink=None,
        realtime_envelope_sink=runtime_state_publisher.observe,
        archive_streams=archive_streams,
    )

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
            desired_names=tuple(
                item.binance_name for item in group.subscriptions
            ),
            generation=1,
            on_envelope=coordinator.submit,
            on_lifecycle=coordinator.observe_lifecycle,
            reconnect_delays=(0.0, 1.0, 5.0),
            connection_lifetime_seconds=(
                runtime.capture.connection_lifetime_seconds
            ),
            open_timeout_seconds=runtime.capture.open_timeout_seconds,
            ping_interval_seconds=runtime.capture.ping_interval_seconds,
            ping_timeout_seconds=runtime.capture.ping_timeout_seconds,
            silence_timeout_seconds=runtime.capture.silence_timeout_seconds,
            control_ack_timeout_seconds=(
                runtime.capture.control_ack_timeout_seconds
            ),
            control_messages_per_second=(
                runtime.capture.control_messages_per_second
            ),
            symbol_filter=coordinator.accepts_symbol,
        )

    connection_pool = BinanceConnectionPool(
        connection_factory=connection_factory,
        max_subscriptions_per_connection=(
            runtime.capture.max_subscriptions_per_connection
        ),
        control_messages_per_second=(
            runtime.capture.control_messages_per_second
        ),
        max_subscriptions_per_connection_by_stream=(
            {
                CaptureStream.BOOK_TICKER: (
                    runtime.capture.book_ticker_max_subscriptions_per_connection
                )
            }
            if (
                runtime.capture.book_ticker_max_subscriptions_per_connection
                is not None
            )
            else None
        ),
        use_all_book_ticker_stream=(
            runtime.capture.book_ticker_use_all_stream
        ),
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
        coordinator=coordinator,
    )
    observer = CaptureUniverseObserver(
        capture,
        streams=enabled_streams,
        initial_generation=1,
        protected_symbol_loader=load_protected_symbols,
    )
    rest_client = BinanceUsdMRestClient(str(runtime.binance_base_url))
    universe = UniverseRefreshService(
        market_data=rest_client,
        repository=universe_repository,
        config=runtime.universe,
        config_hash=capture_version,
        observer=observer,
    )
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
            state_hub=state_hub,
            universe_activation_minute=runtime.universe.activation_minute,
            universe_refresh_interval_minutes=(
                runtime.universe.refresh_interval_minutes
            ),
            enabled_streams=enabled_streams,
            initial_symbols=initial_symbols,
            operational_retention=operational_retention,
        )
    finally:
        await rest_client.aclose()
        await engine.dispose()


@app.command()
def refresh_universe(
    at: str | None = typer.Option(None, "--at"),
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        observed_at = parse_observed_at(at)
        snapshot = asyncio.run(
            refresh_once(resolve_config_path(config), observed_at)
        )
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
    async with build_market_data_runtime(config_path) as runtime:
        capture_task: asyncio.Task[None] | None = None
        auxiliary_tasks: tuple[asyncio.Task[None], ...] = ()
        stop_task: asyncio.Task[bool] | None = None
        try:
            await runtime.state_hub.start()
            await runtime.runtime_state_publisher.start()
            await runtime.capture.start(
                symbols=runtime.initial_symbols,
                streams=runtime.enabled_streams,
                generation=1,
            )
            startup_observed_at = datetime.now(UTC).replace(
                second=0,
                microsecond=0,
            )
            startup_snapshot = await runtime.universe.refresh(
                observed_at=startup_observed_at
            )
            log.info(
                "universe_startup_refresh",
                observed_at=startup_snapshot.observed_at.isoformat(),
            )
            capture_task = asyncio.create_task(runtime.capture.run())
            auxiliary_tasks = (
                asyncio.create_task(
                    run_scheduler_loop(
                        LoggingRefreshService(runtime.universe),
                        activation_minute=(
                            runtime.universe_activation_minute
                        ),
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
                        connection_metrics=(
                            runtime.connection_pool.metrics_snapshot
                        ),
                    )
                ),
                asyncio.create_task(
                    reconcile_paper_exit_subscriptions(
                        runtime.subscription_observer
                    )
                ),
                asyncio.create_task(
                    run_raw_archive_retention_loop(
                        runtime.capture_repository,
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
                        )
                    ),
                )
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
            await runtime.state_hub.stop()


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
    async with build_market_data_runtime(config_path) as runtime:
        capture_task: asyncio.Task[None] | None = None
        scheduler_task: asyncio.Task[None] | None = None
        subscription_task: asyncio.Task[None] | None = None
        retention_task: asyncio.Task[None] | None = None
        operational_retention_task: asyncio.Task[None] | None = None
        health_task: asyncio.Task[None] | None = None
        try:
            await runtime.state_hub.start()
            await runtime.runtime_state_publisher.start()
            await runtime.capture.start(
                symbols=runtime.initial_symbols,
                streams=runtime.enabled_streams,
                generation=1,
            )
            startup_observed_at = datetime.now(UTC).replace(
                second=0,
                microsecond=0,
            )
            await runtime.universe.refresh(observed_at=startup_observed_at)
            capture_task = asyncio.create_task(runtime.capture.run())
            scheduler_task = asyncio.create_task(
                run_scheduler_loop(
                    LoggingRefreshService(runtime.universe),
                    activation_minute=runtime.universe_activation_minute,
                    refresh_interval_minutes=(
                        runtime.universe_refresh_interval_minutes
                    ),
                )
            )
            subscription_task = asyncio.create_task(
                reconcile_paper_exit_subscriptions(
                    runtime.subscription_observer
                )
            )
            retention_task = asyncio.create_task(
                run_raw_archive_retention_loop(
                    runtime.capture_repository,
                    runtime.archive_root,
                    retention_days=runtime.archive_retention_days,
                    interval_seconds=runtime.archive_retention_interval_seconds,
                )
            )
            operational_retention = getattr(
                runtime,
                "operational_retention",
                None,
            )
            if operational_retention is not None:
                operational_retention_task = asyncio.create_task(
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
                    )
                )
            health_task = asyncio.create_task(
                monitor_market_data_health(
                    capture_metrics=runtime.capture.metrics_snapshot,
                    connection_metrics=runtime.connection_pool.metrics_snapshot,
                )
            )
            await asyncio.sleep(seconds)
        finally:
            await runtime.capture.stop()
            if capture_task is not None:
                capture_task.cancel()
            for task in (
                scheduler_task,
                subscription_task,
                retention_task,
                operational_retention_task,
                health_task,
            ):
                if task is not None:
                    task.cancel()
            tasks = tuple(
                task
                for task in (
                    capture_task,
                    scheduler_task,
                    subscription_task,
                    retention_task,
                    operational_retention_task,
                    health_task,
                )
                if task is not None
            )
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await runtime.runtime_state_publisher.stop()
            await runtime.state_hub.stop()


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
