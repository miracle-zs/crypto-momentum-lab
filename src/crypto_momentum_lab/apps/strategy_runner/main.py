import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.strategy import (
    RunMode,
    StrategyRunIdentity,
    deterministic_config_hash,
)
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.persistence.postgres import (
    PostgresPaperDaemonRepository,
    PostgresRuntimeMarketStateRepository,
    PostgresStrategyRunRepository,
    create_async_database_engine,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)
from crypto_momentum_lab.strategy_runner import (
    AsyncPostgresRuntimeStateLoader,
    InMemoryPaperMarketStateSource,
    PaperLiveDaemonConfig,
    PaperLiveSourceConfig,
    PaperRunnerConfig,
    PaperTradingRunReport,
    PostgresPaperMarketStateSource,
    ReplayConfig,
    ReplayExecutionConfig,
    SimulatedFillStatus,
    build_strategy_replay_report,
    run_paper_live_daemon,
    run_paper_trading,
    write_paper_trading_report,
    write_strategy_replay_report,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def strategy_runner_app() -> None:
    """Strategy runner utilities."""


@app.command("replay")
def replay_command(
    strategy_name: Annotated[
        str,
        typer.Option(
            "--strategy",
            help="Strategy name. V0 supports compression_breakout.",
        ),
    ],
    states_root: Annotated[
        Path,
        typer.Option(
            "--states-root",
            exists=True,
            file_okay=False,
            readable=True,
            help="Root directory containing market_states_15s Parquet files.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            "--output",
            dir_okay=False,
            help="JSON replay report output path.",
        ),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    generated_at: Annotated[
        str | None,
        typer.Option("--generated-at", help="Optional ISO timestamp for tests."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 8,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    simulate_fills: Annotated[
        bool,
        typer.Option(
            "--simulate-fills/--no-simulate-fills",
            help="Enable deterministic cost-aware simulated fills.",
        ),
    ] = True,
    execution_latency_buckets: Annotated[
        int,
        typer.Option("--execution-latency-buckets", min=0),
    ] = 1,
    taker_fee_rate: Annotated[
        str,
        typer.Option("--taker-fee-rate"),
    ] = "0.0004",
    slippage_bps: Annotated[
        str,
        typer.Option("--slippage-bps"),
    ] = "0",
) -> None:
    created_at = _parse_generated_at(generated_at)
    execution = (
        ReplayExecutionConfig(
            latency_buckets=execution_latency_buckets,
            taker_fee_rate=Decimal(taker_fee_rate),
            slippage_bps=Decimal(slippage_bps),
        )
        if simulate_fills
        else None
    )
    config = ReplayConfig(
        strategy_name=strategy_name,
        run_id=run_id or f"replay-{uuid4()}",
        code_commit="unknown",
        generated_at=created_at,
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
        execution=execution,
    )
    report = build_strategy_replay_report(
        state_paths=(states_root,),
        config=config,
    )
    write_strategy_replay_report(report, output_path)
    typer.echo(
        "Replay completed: "
        f"states={report.input_state_count} "
        f"signals={len(report.signals)} "
        f"candidates={len(report.candidates)}"
    )
    if execution is not None:
        simulated_fills = tuple(getattr(report, "simulated_fills", ()))
        filled_count = sum(
            1 for fill in simulated_fills if fill.status is SimulatedFillStatus.FILLED
        )
        total_cost = sum(
            (fill.total_cost for fill in simulated_fills),
            Decimal("0"),
        )
        typer.echo(
            "Simulated fills: "
            f"filled={filled_count} "
            f"unfilled={len(simulated_fills) - filled_count} "
            f"total_cost={total_cost}"
        )
    typer.echo(output_path.as_posix())


@app.command("paper")
def paper_command(
    strategy_name: Annotated[
        str,
        typer.Option(
            "--strategy",
            help="Strategy name. V0 supports compression_breakout.",
        ),
    ],
    states_root: Annotated[
        Path,
        typer.Option(
            "--states-root",
            exists=True,
            file_okay=False,
            readable=True,
            help="Root directory containing market_states_15s Parquet files.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            "--output",
            dir_okay=False,
            help="JSON paper run report output path.",
        ),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    generated_at: Annotated[
        str | None,
        typer.Option("--generated-at", help="Optional ISO timestamp for tests."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 8,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    execution_latency_buckets: Annotated[
        int,
        typer.Option("--execution-latency-buckets", min=0),
    ] = 1,
    taker_fee_rate: Annotated[
        str,
        typer.Option("--taker-fee-rate"),
    ] = "0.0004",
    slippage_bps: Annotated[
        str,
        typer.Option("--slippage-bps"),
    ] = "0",
    max_states: Annotated[
        int | None,
        typer.Option("--max-states", min=1),
    ] = None,
    persist: Annotated[
        bool,
        typer.Option(
            "--persist",
            help="Persist the paper report to PostgreSQL.",
        ),
    ] = False,
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            help="Async PostgreSQL URL for --persist.",
        ),
    ] = None,
) -> None:
    created_at = _parse_generated_at(generated_at)
    source = build_paper_state_source((states_root,))
    config = PaperRunnerConfig(
        strategy_name=strategy_name,
        run_id=run_id or f"paper-{uuid4()}",
        code_commit="unknown",
        generated_at=created_at,
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
        execution=ReplayExecutionConfig(
            latency_buckets=execution_latency_buckets,
            taker_fee_rate=Decimal(taker_fee_rate),
            slippage_bps=Decimal(slippage_bps),
        ),
        max_states=max_states,
    )
    report = run_paper_trading(source=source, config=config)
    write_paper_trading_report(report, output_path)
    persisted = False
    if persist:
        resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
        if not resolved_database_url:
            raise typer.BadParameter(
                "--persist requires --database-url or CML_DATABASE_URL"
            )
        asyncio.run(persist_paper_report(report, resolved_database_url))
        persisted = True
    typer.echo(
        "Paper run completed: "
        f"states={report.input_state_count} "
        f"signals={len(report.signals)} "
        f"candidates={len(report.candidates)} "
        f"fills={len(report.paper_fills)} "
        f"persisted={str(persisted).lower()}"
    )
    typer.echo(output_path.as_posix())


@app.command("paper-live-source")
def paper_live_source_command(
    strategy_name: Annotated[
        str,
        typer.Option(
            "--strategy",
            help="Strategy name. V0 supports compression_breakout.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            "--output",
            dir_okay=False,
            help="JSON paper run report output path.",
        ),
    ],
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            help="Async PostgreSQL URL for runtime state polling.",
        ),
    ] = None,
    environment: Annotated[
        str,
        typer.Option("--environment", help="Runtime state environment."),
    ] = "research",
    start_at: Annotated[
        str | None,
        typer.Option(
            "--start-at",
            help="Optional inclusive ISO timestamp for the first bucket.",
        ),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    generated_at: Annotated[
        str | None,
        typer.Option("--generated-at", help="Optional ISO timestamp for tests."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 8,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    execution_latency_buckets: Annotated[
        int,
        typer.Option("--execution-latency-buckets", min=0),
    ] = 1,
    taker_fee_rate: Annotated[
        str,
        typer.Option("--taker-fee-rate"),
    ] = "0.0004",
    slippage_bps: Annotated[
        str,
        typer.Option("--slippage-bps"),
    ] = "0",
    max_states: Annotated[
        int,
        typer.Option("--max-states", min=1),
    ] = 1000,
    poll_interval_seconds: Annotated[
        float,
        typer.Option("--poll-interval-seconds", min=0),
    ] = 1.0,
    idle_timeout_seconds: Annotated[
        float,
        typer.Option("--idle-timeout-seconds", min=0),
    ] = 60.0,
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", min=1),
    ] = 500,
    persist: Annotated[
        bool,
        typer.Option(
            "--persist",
            help="Persist the paper report to PostgreSQL.",
        ),
    ] = False,
) -> None:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter(
            "--database-url or CML_DATABASE_URL is required"
        )
    created_at = _parse_generated_at(generated_at)
    source = build_postgres_paper_source(
        database_url=resolved_database_url,
        environment=environment,
        start_at=_parse_optional_start_at(start_at),
        poll_interval_seconds=poll_interval_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        max_states=max_states,
        batch_size=batch_size,
    )
    config = PaperRunnerConfig(
        strategy_name=strategy_name,
        run_id=run_id or f"paper-live-{uuid4()}",
        code_commit="unknown",
        generated_at=created_at,
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
        execution=ReplayExecutionConfig(
            latency_buckets=execution_latency_buckets,
            taker_fee_rate=Decimal(taker_fee_rate),
            slippage_bps=Decimal(slippage_bps),
        ),
        max_states=max_states,
    )
    report = run_paper_trading(source=source, config=config)
    write_paper_trading_report(report, output_path)
    persisted = False
    if persist:
        asyncio.run(persist_paper_report(report, resolved_database_url))
        persisted = True
    typer.echo(
        "Paper live-source run completed: "
        f"states={report.input_state_count} "
        f"signals={len(report.signals)} "
        f"candidates={len(report.candidates)} "
        f"fills={len(report.paper_fills)} "
        f"persisted={str(persisted).lower()}"
    )
    typer.echo(output_path.as_posix())


@app.command("paper-live-daemon")
def paper_live_daemon_command(
    strategy_name: Annotated[
        str,
        typer.Option(
            "--strategy",
            help="Strategy name. V0 supports compression_breakout.",
        ),
    ],
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            help="Async PostgreSQL URL for runtime state polling.",
        ),
    ] = None,
    environment: Annotated[
        str,
        typer.Option("--environment", help="Runtime state environment."),
    ] = "research",
    start_at: Annotated[
        str | None,
        typer.Option(
            "--start-at",
            help="Optional inclusive ISO timestamp for the first bucket.",
        ),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    generated_at: Annotated[
        str | None,
        typer.Option("--generated-at", help="Optional ISO timestamp for tests."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 8,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    max_states: Annotated[
        int,
        typer.Option("--max-states", min=1),
    ] = 1000,
    poll_interval_seconds: Annotated[
        float,
        typer.Option("--poll-interval-seconds", min=0),
    ] = 1.0,
    idle_timeout_seconds: Annotated[
        float,
        typer.Option("--idle-timeout-seconds", min=0),
    ] = 60.0,
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", min=1),
    ] = 500,
    checkpoint_every_states: Annotated[
        int,
        typer.Option("--checkpoint-every-states", min=1),
    ] = 100,
    checkpoint_every_seconds: Annotated[
        float,
        typer.Option("--checkpoint-every-seconds", min=1),
    ] = 60.0,
    max_market_state_age_seconds: Annotated[
        float,
        typer.Option("--max-market-state-age-seconds", min=1),
    ] = 120.0,
    continue_while_halted: Annotated[
        bool,
        typer.Option("--continue-while-halted"),
    ] = False,
) -> None:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter(
            "--database-url or CML_DATABASE_URL is required"
        )
    resolved_run_id = run_id or f"paper-live-daemon-{uuid4()}"
    created_at = _parse_generated_at(generated_at)
    source = build_postgres_paper_source(
        database_url=resolved_database_url,
        environment=environment,
        start_at=_parse_optional_start_at(start_at),
        poll_interval_seconds=poll_interval_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        max_states=max_states,
        batch_size=batch_size,
    )
    strategy = build_runtime_strategy_for_cli(
        strategy_name=strategy_name,
        run_id=resolved_run_id,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
    )
    repository = build_paper_daemon_repository(resolved_database_url)
    result = run_paper_live_daemon(
        source=source,
        strategy=strategy,
        repository=repository,
        config=PaperLiveDaemonConfig(
            run_id=resolved_run_id,
            strategy_name=strategy_name,
            environment=environment,
            checkpoint_every_states=checkpoint_every_states,
            checkpoint_every_seconds=checkpoint_every_seconds,
            max_market_state_age_seconds=max_market_state_age_seconds,
            continue_while_halted=continue_while_halted,
        ),
        clock=_SystemClock(),
    )
    typer.echo(
        "Paper live daemon completed: "
        f"states={result.processed_state_count} "
        f"halt={result.halt_reason or 'none'}"
    )


def build_paper_state_source(
    state_paths: tuple[Path, ...],
) -> InMemoryPaperMarketStateSource:
    states = tuple(
        sorted(
            read_market_states_15s_dataset(state_paths),
            key=lambda item: (item.bucket_start, item.symbol),
        )
    )
    return InMemoryPaperMarketStateSource(
        states=states,
        description=",".join(path.as_posix() for path in state_paths),
    )


def build_postgres_paper_source(
    *,
    database_url: str,
    environment: str,
    start_at: datetime | None,
    poll_interval_seconds: float,
    idle_timeout_seconds: float,
    max_states: int,
    batch_size: int,
) -> PostgresPaperMarketStateSource:
    engine = create_async_database_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresRuntimeMarketStateRepository(factory)
    loader = AsyncPostgresRuntimeStateLoader(
        repository=repository,
        environment=environment,
    )
    return PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment=environment,
            start_at=start_at,
            poll_interval_seconds=poll_interval_seconds,
            idle_timeout_seconds=idle_timeout_seconds,
            max_states=max_states,
            batch_size=batch_size,
        ),
    )


async def persist_paper_report(
    report: PaperTradingRunReport,
    database_url: str,
) -> None:
    engine = create_async_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        repository = PostgresStrategyRunRepository(factory)
        await repository.save_paper_report(report)
    finally:
        await engine.dispose()


def build_paper_daemon_repository(database_url: str) -> PostgresPaperDaemonRepository:
    engine = create_async_database_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return PostgresPaperDaemonRepository(factory)


def build_runtime_strategy_for_cli(
    *,
    strategy_name: str,
    run_id: str,
    generated_at: datetime,
    source_description: str,
    compression_breakout: CompressionBreakoutConfig,
    candidate_notional: Decimal | None,
    candidate_ttl_buckets: int,
) -> CompressionBreakoutRuntimeStrategy:
    if strategy_name != "compression_breakout":
        raise typer.BadParameter(f"unsupported strategy: {strategy_name}")
    runtime_config = CompressionBreakoutRuntimeConfig(
        event_config=compression_breakout,
        candidate_notional=candidate_notional,
        candidate_ttl_buckets=candidate_ttl_buckets,
    )
    identity = StrategyRunIdentity(
        run_id=run_id,
        strategy_name=strategy_name,
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=generated_at,
        source_paths=(source_description,),
    )
    return CompressionBreakoutRuntimeStrategy(
        config=runtime_config,
        identity=identity,
    )


def _parse_generated_at(value: str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_optional_start_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise typer.BadParameter("--start-at must include a timezone")
    return parsed.astimezone(UTC)


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)
