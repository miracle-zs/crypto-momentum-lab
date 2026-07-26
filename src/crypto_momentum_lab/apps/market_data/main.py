import asyncio
import os
from collections.abc import AsyncIterator
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
)
from crypto_momentum_lab.persistence.postgres.capture_repository import (
    PostgresCaptureRepository,
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
from crypto_momentum_lab.universe.refresh import UniverseRefreshService
from crypto_momentum_lab.universe.scheduler import run_scheduler_loop

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()


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


@asynccontextmanager
async def build_refresh_service(
    config_path: Path,
) -> AsyncIterator[tuple[UniverseRefreshService, int]]:
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
        )
    finally:
        await client.aclose()
        await engine.dispose()


async def refresh_once(
    config_path: Path,
    observed_at: datetime,
) -> UniverseSnapshot:
    async with build_refresh_service(config_path) as (service, _):
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
    def __init__(self, delegate: UniverseRefreshService) -> None:
        self._delegate = delegate

    async def refresh(
        self,
        *,
        observed_at: datetime,
    ) -> UniverseSnapshot:
        snapshot = await self._delegate.refresh(observed_at=observed_at)
        log_snapshot(snapshot)
        return snapshot


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
    ) -> None:
        self._capture = capture
        self._streams = streams
        self._generation = initial_generation
        self._lock = asyncio.Lock()

    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        symbols = frozenset(item.symbol for item in snapshot.memberships)
        async with self._lock:
            self._generation += 1
            await self._capture.apply_symbols(
                symbols,
                streams=self._streams,
                generation=self._generation,
            )


@dataclass(frozen=True, slots=True)
class MarketDataRuntime:
    capture: MarketDataCaptureService
    universe: UniverseRefreshService
    universe_activation_minute: int
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
    runtime_state_repository = PostgresRuntimeMarketStateRepository(sessions)
    runtime_state_publisher = ClosedMarketStatePublisher(
        repository=runtime_state_repository
    )
    initial_memberships = await universe_repository.load_active_memberships()
    initial_symbols = frozenset(initial_memberships)
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
        archived_envelope_sink=runtime_state_publisher.observe,
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
            universe=universe,
            universe_activation_minute=runtime.universe.activation_minute,
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
    ):
        log.info(
            "universe_scheduler_started",
            activation_minute=activation_minute,
        )
        await run_scheduler_loop(
            LoggingRefreshService(service),
            activation_minute=activation_minute,
        )


async def run_market_data(config_path: Path) -> None:
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
        startup_snapshot = await runtime.universe.refresh(
            observed_at=startup_observed_at
        )
        log.info(
            "universe_startup_refresh",
            observed_at=startup_snapshot.observed_at.isoformat(),
        )
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(runtime.capture.run())
                tasks.create_task(
                    run_scheduler_loop(
                        LoggingRefreshService(runtime.universe),
                        activation_minute=(
                            runtime.universe_activation_minute
                        ),
                    )
                )
        finally:
            await runtime.capture.stop()


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
            )
        )
        try:
            await asyncio.sleep(seconds)
        finally:
            await runtime.capture.stop()
            for task in (capture_task, scheduler_task):
                task.cancel()
            await asyncio.gather(
                capture_task,
                scheduler_task,
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
        asyncio.run(run_market_data(resolve_config_path(config)))
    except KeyboardInterrupt:
        log.info("market_data_stopped")
