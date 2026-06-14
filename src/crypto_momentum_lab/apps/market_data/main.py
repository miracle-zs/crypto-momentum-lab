import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import structlog
import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config.loader import (
    behavior_hash,
    load_runtime_config,
)
from crypto_momentum_lab.domain.universe.models import UniverseSnapshot
from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
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


@app.command()
def run_universe_scheduler(
    config: Path | None = typer.Option(None, "--config"),
) -> None:
    try:
        asyncio.run(run_scheduler(resolve_config_path(config)))
    except KeyboardInterrupt:
        log.info("universe_scheduler_stopped")
