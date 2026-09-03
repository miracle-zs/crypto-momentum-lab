"""CLI for the isolated research market-state collector."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Annotated

import structlog
import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.market_data.hub import (
    MarketStateHubConfig,
    WebSocketMarketStateSource,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
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
from crypto_momentum_lab.research_collector.models import (
    CollectorConfig,
    CollectorError,
    CollectorPaused,
)
from crypto_momentum_lab.research_collector.selection import (
    AllSymbolsSelector,
    PostgresTop30Selector,
)
from crypto_momentum_lab.research_collector.service import (
    ResearchStateCollector,
)
from crypto_momentum_lab.research_collector.source import (
    PostgresMarketStateBackfillSource,
)
from crypto_momentum_lab.research_collector.storage import (
    CapacityGuard,
    CheckpointStore,
)

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()

_DEFAULT_HUB_URL = "ws://market-data:8766"
_DEFAULT_ROOT = Path("/app/research-data")
_BYTES_PER_GIB = 1024**3


@app.callback()
def research_collector_app() -> None:
    """Collect canonical 15-second market states for offline research."""


@app.command("run")
def run_command(
    hub_url: Annotated[
        str,
        typer.Option(
            "--hub-url",
            envvar="CML_MARKET_STATE_HUB_URL",
            help="Market-state Hub URL.",
        ),
    ] = _DEFAULT_HUB_URL,
    environment: Annotated[
        str,
        typer.Option("--environment", help="Market-state environment."),
    ] = "research",
    root: Annotated[
        Path,
        typer.Option(
            "--root",
            envvar="CML_RESEARCH_COLLECTOR_ROOT",
            file_okay=False,
            help="Collector data volume.",
        ),
    ] = _DEFAULT_ROOT,
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            help="Optional database URL; environment variables are used by default.",
        ),
    ] = None,
    account_label: Annotated[
        str | None,
        typer.Option(
            "--account-label",
            help="Live account label whose open positions are retained.",
        ),
    ] = None,
    position_environment: Annotated[
        str,
        typer.Option(
            "--position-environment",
            help="Account environment used for open-position protection.",
        ),
    ] = "live",
    top_count: Annotated[
        int,
        typer.Option("--top-count", min=1, help="Positive gainer rank depth."),
    ] = 30,
    all_symbols: Annotated[
        bool,
        typer.Option(
            "--all-symbols",
            help="Retain every symbol delivered by the Hub.",
        ),
    ] = False,
    soft_limit_gib: Annotated[
        float,
        typer.Option("--soft-limit-gib", min=0.1, help="Collector warning quota."),
    ] = 6.0,
    hard_limit_gib: Annotated[
        float,
        typer.Option("--hard-limit-gib", min=0.1, help="Collector pause quota."),
    ] = 8.0,
    global_warning_free_gib: Annotated[
        float,
        typer.Option(
            "--global-warning-free-gib",
            min=0.1,
            help="Host free-space warning reserve.",
        ),
    ] = 15.0,
    global_pause_free_gib: Annotated[
        float,
        typer.Option(
            "--global-pause-free-gib",
            min=0.1,
            help="Host free-space pause reserve.",
        ),
    ] = 10.0,
    window_seconds: Annotated[
        int,
        typer.Option(
            "--window-seconds",
            min=15,
            help="Parquet file window; must be a multiple of 15.",
        ),
    ] = 900,
    late_tolerance_seconds: Annotated[
        int,
        typer.Option(
            "--late-tolerance-seconds",
            min=0,
            help="Late-state tolerance before a window is sealed.",
        ),
    ] = 30,
    max_spool_gib: Annotated[
        float,
        typer.Option("--max-spool-gib", min=0.1, help="Write-ahead spool quota."),
    ] = 1.0,
) -> None:
    """Run the collector until SIGTERM or SIGINT."""

    try:
        asyncio.run(
            _run_collector(
                hub_url=hub_url,
                environment=environment,
                root=root,
                database_url=database_url,
                account_label=account_label,
                position_environment=position_environment,
                top_count=top_count,
                all_symbols=all_symbols,
                soft_limit_gib=soft_limit_gib,
                hard_limit_gib=hard_limit_gib,
                global_warning_free_gib=global_warning_free_gib,
                global_pause_free_gib=global_pause_free_gib,
                window_seconds=window_seconds,
                late_tolerance_seconds=late_tolerance_seconds,
                max_spool_gib=max_spool_gib,
            )
        )
    except CollectorPaused as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=2) from error
    except CollectorError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1) from error


@app.command("health")
def health_command(
    root: Annotated[
        Path,
        typer.Option(
            "--root",
            envvar="CML_RESEARCH_COLLECTOR_ROOT",
            file_okay=False,
        ),
    ] = _DEFAULT_ROOT,
    environment: Annotated[
        str,
        typer.Option("--environment"),
    ] = "research",
    max_age_seconds: Annotated[
        int,
        typer.Option("--max-age-seconds", min=0),
    ] = 120,
) -> None:
    """Check the local checkpoint and capacity guard for container health."""

    checkpoint = CheckpointStore(root / "checkpoints" / f"{environment}.json").load(
        environment=environment
    )
    if checkpoint is None:
        typer.echo("collector checkpoint is missing", err=True)
        raise typer.Exit(code=1)
    config = CollectorConfig(environment=environment, root=root)
    snapshot = CapacityGuard(
        root,
        soft_limit_bytes=config.soft_limit_bytes,
        hard_limit_bytes=config.hard_limit_bytes,
        global_warning_free_bytes=config.global_warning_free_bytes,
        global_pause_free_bytes=config.global_pause_free_bytes,
    ).snapshot()
    now = checkpoint.updated_at
    stale = now is None or (max_age_seconds > 0 and _age_seconds(now) > max_age_seconds)
    payload = {
        "environment": environment,
        "last_sequence": checkpoint.last_sequence,
        "updated_at": None if now is None else now.isoformat(),
        "collector_bytes": snapshot.collector_bytes,
        "disk_free_bytes": snapshot.disk_free_bytes,
        "capacity_state": snapshot.state.value,
        "stale": stale,
    }
    typer.echo(json.dumps(payload, sort_keys=True))
    if stale or snapshot.state.value == "paused":
        raise typer.Exit(code=1)


async def _run_collector(
    *,
    hub_url: str,
    environment: str,
    root: Path,
    database_url: str | None,
    account_label: str | None,
    position_environment: str,
    top_count: int,
    all_symbols: bool,
    soft_limit_gib: float,
    hard_limit_gib: float,
    global_warning_free_gib: float,
    global_pause_free_gib: float,
    window_seconds: int,
    late_tolerance_seconds: int,
    max_spool_gib: float,
) -> None:
    resolved_database_url = (
        database_url
        or os.environ.get("CML_MARKET_DATABASE_URL")
        or os.environ.get("CML_DATABASE_URL")
    )
    if not resolved_database_url:
        raise CollectorError(
            "database URL is required for replay recovery; set "
            "CML_MARKET_DATABASE_URL or CML_DATABASE_URL"
        )
    if account_label is None:
        account_label = os.environ.get("CML_LIVE_ACCOUNT_LABEL")

    engine = create_async_database_engine(
        resolved_database_url,
        pool_size=1,
        max_overflow=0,
        pool_timeout_seconds=2,
        command_timeout_seconds=5,
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    universe_repository = PostgresUniverseRepository(sessions)
    account_repository = (
        None if account_label is None else PostgresAccountRepository(sessions)
    )
    runtime_repository = PostgresRuntimeMarketStateRepository(sessions)
    selector = (
        AllSymbolsSelector()
        if all_symbols
        else PostgresTop30Selector(
            universe_repository=universe_repository,
            account_repository=account_repository,
            environment=environment,
            top_count=top_count,
            account_label=account_label,
            position_environment=position_environment,
        )
    )
    source = WebSocketMarketStateSource(
        url=hub_url,
        environment=environment,
        consumer_id=f"research-collector:{environment}",
        config=MarketStateHubConfig(),
        fail_on_replay_unavailable=True,
        preserve_sequence_on_overflow=True,
    )
    collector_config = CollectorConfig(
        environment=environment,
        root=root,
        soft_limit_bytes=_gib_bytes(soft_limit_gib),
        hard_limit_bytes=_gib_bytes(hard_limit_gib),
        global_warning_free_bytes=_gib_bytes(global_warning_free_gib),
        global_pause_free_bytes=_gib_bytes(global_pause_free_gib),
        window_seconds=window_seconds,
        late_tolerance_seconds=late_tolerance_seconds,
        max_spool_bytes=_gib_bytes(max_spool_gib),
    )
    collector = ResearchStateCollector(
        config=collector_config,
        source=source,
        selector=selector,
        backfill_source=PostgresMarketStateBackfillSource(
            runtime_repository,
            environment=environment,
        ),
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []

    def request_stop(signal_name: str) -> None:
        log.info(
            "research_collector_stop_requested",
            signal=signal_name,
        )
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

    collector_task = asyncio.create_task(collector.run())
    stop_task = asyncio.create_task(stop_requested.wait())
    try:
        done, _pending = await asyncio.wait(
            (collector_task, stop_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done and not collector_task.done():
            collector_task.cancel()
        await asyncio.gather(collector_task, return_exceptions=False)
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        await collector.stop()
        for shutdown_signal in registered_signals:
            loop.remove_signal_handler(shutdown_signal)
        await engine.dispose()


def _gib_bytes(value: float) -> int:
    return int(value * _BYTES_PER_GIB)


def _age_seconds(value: object) -> float:
    from datetime import UTC, datetime

    if not isinstance(value, datetime):
        return float("inf")
    return max(0.0, (datetime.now(UTC) - value).total_seconds())
