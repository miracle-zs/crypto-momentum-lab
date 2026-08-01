import asyncio
import os
import signal
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
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
from crypto_momentum_lab.market_data.quality.tracker import StreamQualityTracker
from crypto_momentum_lab.market_data.runtime_states import (
    ClosedMarketStatePublisher,
    ClosedMarketStatePublisherConfig,
)
from crypto_momentum_lab.persistence.postgres.capture_repository import (
    PostgresCaptureRepository,
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
_CAPTURE_STOP_TIMEOUT_SECONDS = 30.0
_PAPER_EXIT_RECONCILE_SECONDS = 15.0
_PAPER_EXIT_RUN_IDS_ENV = "CML_PAPER_EXIT_RUN_IDS"


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


@dataclass(frozen=True, slots=True)
class MarketDataRuntime:
    capture: MarketDataCaptureService
    capture_repository: PostgresCaptureRepository
    archive_root: Path
    archive_retention_days: int
    archive_retention_interval_seconds: float
    universe: UniverseRefreshService
    subscription_observer: CaptureUniverseObserver
    runtime_state_publisher: ClosedMarketStatePublisher
    universe_activation_minute: int
    universe_refresh_interval_minutes: int
    enabled_streams: tuple[CaptureStream, ...]
    initial_symbols: frozenset[str]


@asynccontextmanager
async def build_market_data_runtime(
    config_path: Path,
) -> AsyncIterator[MarketDataRuntime]:
    runtime = load_runtime_config(config_path)
    engine = create_async_database_engine(runtime.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    universe_repository = PostgresUniverseRepository(sessions)
    capture_repository = PostgresCaptureRepository(sessions)
    paper_repository = PostgresPaperDaemonRepository(sessions)
    runtime_state_repository = PostgresRuntimeMarketStateRepository(sessions)
    runtime_state_publisher = ClosedMarketStatePublisher(
        repository=runtime_state_repository,
        config=ClosedMarketStatePublisherConfig(
            closure_delay_seconds=runtime.capture.closure_delay_seconds
        ),
    )
    protected_run_ids = parse_paper_exit_run_ids()

    async def load_protected_symbols() -> frozenset[str]:
        return await paper_repository.load_open_position_symbols(
            protected_run_ids
        )

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
    queue = BoundedEnvelopeQueue(
        max_events=runtime.capture.queue_max_events,
        max_bytes=runtime.capture.queue_max_bytes,
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
    )

    def connection_factory(group: SubscriptionGroup) -> BinanceWebSocketConnection:
        base_url = (
            str(runtime.capture.public_websocket_url)
            if group.route is CaptureRoute.PUBLIC
            else str(runtime.capture.market_websocket_url)
        )
        return BinanceWebSocketConnection(
            base_url=base_url,
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
            control_messages_per_second=(
                runtime.capture.control_messages_per_second
            ),
        )

    connection_pool = BinanceConnectionPool(
        connection_factory=connection_factory,
        max_subscriptions_per_connection=(
            runtime.capture.max_subscriptions_per_connection
        ),
        control_messages_per_second=(
            runtime.capture.control_messages_per_second
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
            capture_repository=capture_repository,
            archive_root=archive_config.root,
            archive_retention_days=archive_config.retention_days,
            archive_retention_interval_seconds=(
                archive_config.retention_check_interval_seconds
            ),
            universe=universe,
            subscription_observer=observer,
            runtime_state_publisher=runtime_state_publisher,
            universe_activation_minute=runtime.universe.activation_minute,
            universe_refresh_interval_minutes=(
                runtime.universe.refresh_interval_minutes
            ),
            enabled_streams=enabled_streams,
            initial_symbols=initial_symbols,
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


async def run_market_data(config_path: Path) -> None:
    async with build_market_data_runtime(config_path) as runtime:
        await runtime.capture.start(
            symbols=runtime.initial_symbols,
            streams=runtime.enabled_streams,
            generation=1,
        )
        try:
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
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(runtime.capture.run())
                tasks.create_task(
                    run_scheduler_loop(
                        LoggingRefreshService(runtime.universe),
                        activation_minute=(
                            runtime.universe_activation_minute
                        ),
                        refresh_interval_minutes=(
                            runtime.universe_refresh_interval_minutes
                        ),
                    )
                )
                tasks.create_task(
                    monitor_market_data_freshness(
                        latest_observed_at=lambda: (
                            runtime.runtime_state_publisher.metrics.latest_watermark_at
                        )
                    )
                )
                tasks.create_task(
                    reconcile_paper_exit_subscriptions(
                        runtime.subscription_observer
                    )
                )
                tasks.create_task(
                    run_raw_archive_retention_loop(
                        runtime.capture_repository,
                        runtime.archive_root,
                        retention_days=runtime.archive_retention_days,
                        interval_seconds=runtime.archive_retention_interval_seconds,
                    )
                )
        finally:
            try:
                async with asyncio.timeout(_CAPTURE_STOP_TIMEOUT_SECONDS):
                    await runtime.capture.stop()
            except TimeoutError:
                log.error(
                    "market_data_capture_stop_timed_out",
                    timeout_seconds=_CAPTURE_STOP_TIMEOUT_SECONDS,
                )


async def run_market_data_until_stopped(
    config_path: Path,
    stop_requested: asyncio.Event,
) -> None:
    service_task = asyncio.create_task(run_market_data(config_path))
    stop_task = asyncio.create_task(stop_requested.wait())
    try:
        done, _ = await asyncio.wait(
            (service_task, stop_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if service_task in done:
            await service_task
            return
        service_task.cancel()
        try:
            await service_task
        except asyncio.CancelledError:
            pass
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        if not service_task.done():
            service_task.cancel()
            await asyncio.gather(service_task, return_exceptions=True)


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
        try:
            await asyncio.sleep(seconds)
        finally:
            await runtime.capture.stop()
            for task in (
                capture_task,
                scheduler_task,
                subscription_task,
                retention_task,
            ):
                task.cancel()
            await asyncio.gather(
                capture_task,
                scheduler_task,
                subscription_task,
                retention_task,
                return_exceptions=True,
            )


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
        asyncio.run(
            run_market_data_with_signal_handlers(resolve_config_path(config))
        )
    except KeyboardInterrupt:
        log.info("market_data_stopped")
