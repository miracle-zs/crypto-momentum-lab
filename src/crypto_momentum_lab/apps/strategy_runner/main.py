import asyncio
import os
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.build_info import resolve_code_commit
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
    PostgresUniverseRepository,
    create_async_database_engine,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    AsyncPostgresRuntimeStateLoader,
    BinanceRestClosedCandle15mSource,
    InMemoryPaperMarketStateSource,
    PairedPaperLiveAccount,
    PaperEntryFilterConfig,
    PaperExitConfig,
    PaperExitMode,
    PaperLiveDaemonConfig,
    PaperLiveSourceConfig,
    PaperRunnerConfig,
    PaperTradingRunReport,
    PostgresPaperMarketStateSource,
    ReplayConfig,
    ReplayExecutionConfig,
    SimulatedFillStatus,
    build_strategy_replay_report,
    run_paired_paper_live_daemon,
    run_paper_live_daemon,
    run_paper_trading,
    write_paper_trading_report,
    write_strategy_replay_report,
)
from crypto_momentum_lab.strategy_runner.registry import (
    RuntimeStrategyProtocol,
    build_runtime_config,
    build_runtime_strategy,
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
    ] = "0.025",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.003",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 12,
    signal_interval_seconds: Annotated[
        int,
        typer.Option("--signal-interval-seconds", min=15),
    ] = 300,
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
        signal_interval_seconds=signal_interval_seconds,
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
    ] = "0.025",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.003",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 12,
    signal_interval_seconds: Annotated[
        int,
        typer.Option("--signal-interval-seconds", min=15),
    ] = 300,
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
        signal_interval_seconds=signal_interval_seconds,
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
    ] = "0.025",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.003",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 12,
    signal_interval_seconds: Annotated[
        int,
        typer.Option("--signal-interval-seconds", min=15),
    ] = 300,
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
    ] = 0.25,
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
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
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
        signal_interval_seconds=signal_interval_seconds,
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
    binance_base_url: Annotated[
        str,
        typer.Option(
            "--binance-base-url",
            help="Binance USD-M REST base URL for official 15m exits.",
        ),
    ] = "https://fapi.binance.com",
    environment: Annotated[
        str,
        typer.Option("--environment", help="Runtime state environment."),
    ] = "research",
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="Optional deterministic run ID."),
    ] = None,
    compression_window_buckets: Annotated[
        int,
        typer.Option("--compression-window-buckets", min=1),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option("--max-range-width-pct"),
    ] = "0.025",
    min_breakout_pct: Annotated[
        str,
        typer.Option("--min-breakout-pct"),
    ] = "0.003",
    acceptance_buckets: Annotated[
        int,
        typer.Option("--acceptance-buckets", min=1),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option("--cooldown-buckets", min=0),
    ] = 12,
    signal_interval_seconds: Annotated[
        int,
        typer.Option("--signal-interval-seconds", min=15),
    ] = 300,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "100",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    paper_initial_balance: Annotated[
        str,
        typer.Option("--paper-initial-balance"),
    ] = "1000",
    exit_mode: Annotated[
        str,
        typer.Option(
            "--exit-mode",
            help="Exit mode: fixed or candle_15m.",
        ),
    ] = PaperExitMode.FIXED.value,
    take_profit_pct: Annotated[
        str,
        typer.Option("--take-profit-pct"),
    ] = "0.02",
    stop_loss_pct: Annotated[
        str,
        typer.Option("--stop-loss-pct"),
    ] = "0.01",
    max_holding_buckets: Annotated[
        int,
        typer.Option("--max-holding-buckets", min=1),
    ] = 80,
    candle_grace_bars: Annotated[
        int,
        typer.Option(
            "--candle-grace-bars",
            min=0,
            help="After the first adverse 15m candle, wait this many bars.",
        ),
    ] = 0,
    candle_grace_profit_pct: Annotated[
        str,
        typer.Option(
            "--candle-grace-profit-pct",
            help="Recovery limit as a price move from entry, e.g. 0.0058.",
        ),
    ] = "0",
    entry_long_only: Annotated[
        bool,
        typer.Option(
            "--entry-long-only/--entry-all-sides",
            help="Accept only long entry signals for this paper account.",
        ),
    ] = False,
    entry_max_abs_aggressive_imbalance: Annotated[
        str | None,
        typer.Option(
            "--entry-max-abs-aggressive-imbalance",
            help="Optional inclusive absolute aggressive-imbalance ceiling.",
        ),
    ] = None,
    entry_max_cluster_trade_count: Annotated[
        int | None,
        typer.Option(
            "--entry-max-cluster-trade-count",
            min=1,
            help="Optional inclusive liquidation-cluster trade-count ceiling.",
        ),
    ] = None,
    max_states: Annotated[
        int,
        typer.Option("--max-states", min=1),
    ] = 1000,
    poll_interval_seconds: Annotated[
        float,
        typer.Option("--poll-interval-seconds", min=0),
    ] = 0.25,
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
    require_market_quote: Annotated[
        bool,
        typer.Option("--require-market-quote/--allow-close-fallback"),
    ] = False,
) -> None:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
    resolved_run_id = run_id or f"paper-live-daemon-{uuid4()}"
    clock = _SystemClock()
    created_at = clock.now()
    source = build_postgres_paper_source(
        database_url=resolved_database_url,
        environment=environment,
        start_at=created_at,
        poll_interval_seconds=poll_interval_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        max_states=max_states,
        batch_size=batch_size,
    )
    compression_breakout = CompressionBreakoutConfig(
        compression_window_buckets=compression_window_buckets,
        max_range_width_pct=Decimal(max_range_width_pct),
        min_breakout_pct=Decimal(min_breakout_pct),
        acceptance_buckets=acceptance_buckets,
        cooldown_buckets=cooldown_buckets,
        forward_horizon_buckets=(1,),
    )
    identity = build_runtime_identity_for_cli(
        strategy_name=strategy_name,
        run_id=resolved_run_id,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=compression_breakout,
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
    )
    strategy = build_runtime_strategy_for_cli(
        strategy_name=strategy_name,
        run_id=resolved_run_id,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=compression_breakout,
        candidate_notional=Decimal(candidate_notional),
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
        identity=identity,
    )
    repository = build_paper_daemon_repository(resolved_database_url)
    resolved_exit_mode = PaperExitMode(exit_mode)
    candle_source_context = (
        BinanceRestClosedCandle15mSource(binance_base_url)
        if resolved_exit_mode is PaperExitMode.CANDLE_15M
        else nullcontext(None)
    )
    with candle_source_context as candle_source:
        result = run_paper_live_daemon(
            source=source,
            strategy=strategy,
            repository=repository,
            artifact_repository=repository,
            config=PaperLiveDaemonConfig(
                run_id=resolved_run_id,
                strategy_name=strategy_name,
                environment=environment,
                checkpoint_every_states=checkpoint_every_states,
                checkpoint_every_seconds=checkpoint_every_seconds,
                max_market_state_age_seconds=max_market_state_age_seconds,
                run_identity=identity,
                source_description=source.description,
                execution=ReplayExecutionConfig(
                    latency_buckets=0,
                    require_market_quote=require_market_quote,
                ),
                portfolio=PaperExitConfig(
                    exit_mode=resolved_exit_mode,
                    initial_balance=Decimal(paper_initial_balance),
                    take_profit_pct=Decimal(take_profit_pct),
                    stop_loss_pct=Decimal(stop_loss_pct),
                    max_holding_buckets=max_holding_buckets,
                    require_executable_quote=require_market_quote,
                    candle_grace_bars=candle_grace_bars,
                    candle_grace_profit_pct=Decimal(candle_grace_profit_pct),
                ),
                entry_filter=_entry_filter_config(
                    long_only=entry_long_only,
                    max_abs_aggressive_imbalance=(
                        entry_max_abs_aggressive_imbalance
                    ),
                    max_cluster_trade_count=entry_max_cluster_trade_count,
                ),
            ),
            clock=clock,
            entry_symbol_loader=source.load_active_symbols_at,
            candle_source=candle_source,
        )
    typer.echo(
        "Paper live daemon completed: "
        f"states={result.processed_state_count} "
        f"halt={result.halt_reason or 'none'}"
    )


@app.command("paper-live-pair")
def paper_live_pair_command(
    strategy_name: Annotated[
        str,
        typer.Option("--strategy", help="Strategy name."),
    ],
    fixed_run_id: Annotated[
        str,
        typer.Option("--fixed-run-id", help="Run ID for the fixed-exit account."),
    ],
    candle_run_id: Annotated[
        str,
        typer.Option(
            "--candle-run-id",
            help="Run ID for the 15-minute candle-exit account.",
        ),
    ],
    third_run_id: Annotated[
        str | None,
        typer.Option(
            "--third-run-id",
            help="Optional third candle-exit account sharing the same entries.",
        ),
    ] = None,
    fourth_run_id: Annotated[
        str | None,
        typer.Option(
            "--fourth-run-id",
            help="Optional fourth 15-minute candle-exit account.",
        ),
    ] = None,
    fifth_run_id: Annotated[
        str | None,
        typer.Option(
            "--fifth-run-id",
            help="Optional fifth 15-minute candle-exit account.",
        ),
    ] = None,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="Async PostgreSQL URL."),
    ] = None,
    binance_base_url: Annotated[
        str,
        typer.Option(
            "--binance-base-url",
            help="Binance USD-M REST base URL for official 15m exits.",
        ),
    ] = "https://fapi.binance.com",
    environment: Annotated[
        str,
        typer.Option("--environment", help="Runtime state environment."),
    ] = "research",
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
    signal_interval_seconds: Annotated[
        int,
        typer.Option("--signal-interval-seconds", min=15),
    ] = 15,
    candidate_notional: Annotated[
        str,
        typer.Option("--candidate-notional"),
    ] = "25",
    candidate_ttl_buckets: Annotated[
        int,
        typer.Option("--candidate-ttl-buckets", min=1),
    ] = 4,
    paper_initial_balance: Annotated[
        str,
        typer.Option("--paper-initial-balance"),
    ] = "1000",
    fixed_take_profit_pct: Annotated[
        str,
        typer.Option("--fixed-take-profit-pct"),
    ] = "0.02",
    fixed_stop_loss_pct: Annotated[
        str,
        typer.Option("--fixed-stop-loss-pct"),
    ] = "0.01",
    fixed_max_holding_buckets: Annotated[
        int,
        typer.Option("--fixed-max-holding-buckets", min=1),
    ] = 80,
    candle_max_holding_buckets: Annotated[
        int,
        typer.Option("--candle-max-holding-buckets", min=1),
    ] = 5760,
    third_candle_minimum_holding_buckets: Annotated[
        int,
        typer.Option(
            "--third-candle-minimum-holding-buckets",
            min=0,
            help="Minimum 15-second buckets before the third candle exit.",
        ),
    ] = 0,
    third_candle_confirmation_count: Annotated[
        int,
        typer.Option(
            "--third-candle-confirmation-count",
            min=1,
            help="Opposite 15-minute candles required for the third exit.",
        ),
    ] = 1,
    fourth_entry_long_only: Annotated[
        bool,
        typer.Option(
            "--fourth-entry-long-only/--fourth-entry-all-sides",
            help="Accept only long signals in the fourth account.",
        ),
    ] = False,
    fourth_entry_max_abs_aggressive_imbalance: Annotated[
        str | None,
        typer.Option(
            "--fourth-entry-max-abs-aggressive-imbalance",
            help="Inclusive aggressive-imbalance ceiling for account four.",
        ),
    ] = None,
    fifth_entry_long_only: Annotated[
        bool,
        typer.Option(
            "--fifth-entry-long-only/--fifth-entry-all-sides",
            help="Accept only long signals in the fifth account.",
        ),
    ] = False,
    fifth_entry_max_abs_aggressive_imbalance: Annotated[
        str | None,
        typer.Option(
            "--fifth-entry-max-abs-aggressive-imbalance",
            help="Inclusive aggressive-imbalance ceiling for account five.",
        ),
    ] = None,
    sixth_run_id: Annotated[
        str | None,
        typer.Option(
            "--sixth-run-id",
            help="Optional sixth 15-minute candle-exit account.",
        ),
    ] = None,
    sixth_entry_long_only: Annotated[
        bool,
        typer.Option(
            "--sixth-entry-long-only/--sixth-entry-all-sides",
            help="Accept only long signals in the sixth account.",
        ),
    ] = False,
    sixth_candle_grace_bars: Annotated[
        int,
        typer.Option(
            "--sixth-candle-grace-bars",
            min=0,
            help="Grace bars for the sixth account.",
        ),
    ] = 0,
    sixth_candle_grace_profit_pct: Annotated[
        str,
        typer.Option(
            "--sixth-candle-grace-profit-pct",
            help="Recovery limit for the sixth account, e.g. 0.0058.",
        ),
    ] = "0",
    seventh_run_id: Annotated[
        str | None,
        typer.Option(
            "--seventh-run-id",
            help="Optional seventh 15-minute candle-exit account.",
        ),
    ] = None,
    seventh_entry_long_only: Annotated[
        bool,
        typer.Option(
            "--seventh-entry-long-only/--seventh-entry-all-sides",
            help="Accept only long signals in the seventh account.",
        ),
    ] = False,
    seventh_candle_grace_bars: Annotated[
        int,
        typer.Option(
            "--seventh-candle-grace-bars",
            min=0,
            help="Grace bars for the seventh account.",
        ),
    ] = 0,
    seventh_candle_grace_profit_pct: Annotated[
        str,
        typer.Option(
            "--seventh-candle-grace-profit-pct",
            help="Recovery limit for the seventh account, e.g. 0.0058.",
        ),
    ] = "0",
    max_states: Annotated[
        int,
        typer.Option("--max-states", min=1),
    ] = 1000,
    poll_interval_seconds: Annotated[
        float,
        typer.Option("--poll-interval-seconds", min=0),
    ] = 0.25,
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
    require_market_quote: Annotated[
        bool,
        typer.Option("--require-market-quote/--allow-close-fallback"),
    ] = False,
) -> None:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
    clock = _SystemClock()
    created_at = clock.now()
    source = build_postgres_paper_source(
        database_url=resolved_database_url,
        environment=environment,
        start_at=created_at,
        poll_interval_seconds=poll_interval_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        max_states=max_states,
        batch_size=batch_size,
    )
    compression_breakout = CompressionBreakoutConfig(
        compression_window_buckets=compression_window_buckets,
        max_range_width_pct=Decimal(max_range_width_pct),
        min_breakout_pct=Decimal(min_breakout_pct),
        acceptance_buckets=acceptance_buckets,
        cooldown_buckets=cooldown_buckets,
        forward_horizon_buckets=(1,),
    )
    candidate_notional_decimal = Decimal(candidate_notional)
    fixed_identity = build_runtime_identity_for_cli(
        run_id=fixed_run_id,
        strategy_name=strategy_name,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=compression_breakout,
        candidate_notional=candidate_notional_decimal,
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
    )
    candle_identity = build_runtime_identity_for_cli(
        run_id=candle_run_id,
        strategy_name=strategy_name,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=compression_breakout,
        candidate_notional=candidate_notional_decimal,
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
    )
    third_identity = (
        None
        if third_run_id is None
        else build_runtime_identity_for_cli(
            run_id=third_run_id,
            strategy_name=strategy_name,
            generated_at=created_at,
            source_description=source.description,
            compression_breakout=compression_breakout,
            candidate_notional=candidate_notional_decimal,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    )
    fourth_identity = (
        None
        if fourth_run_id is None
        else build_runtime_identity_for_cli(
            run_id=fourth_run_id,
            strategy_name=strategy_name,
            generated_at=created_at,
            source_description=source.description,
            compression_breakout=compression_breakout,
            candidate_notional=candidate_notional_decimal,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    )
    fifth_identity = (
        None
        if fifth_run_id is None
        else build_runtime_identity_for_cli(
            run_id=fifth_run_id,
            strategy_name=strategy_name,
            generated_at=created_at,
            source_description=source.description,
            compression_breakout=compression_breakout,
            candidate_notional=candidate_notional_decimal,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    )
    sixth_identity = (
        None
        if sixth_run_id is None
        else build_runtime_identity_for_cli(
            run_id=sixth_run_id,
            strategy_name=strategy_name,
            generated_at=created_at,
            source_description=source.description,
            compression_breakout=compression_breakout,
            candidate_notional=candidate_notional_decimal,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    )
    seventh_identity = (
        None
        if seventh_run_id is None
        else build_runtime_identity_for_cli(
            run_id=seventh_run_id,
            strategy_name=strategy_name,
            generated_at=created_at,
            source_description=source.description,
            compression_breakout=compression_breakout,
            candidate_notional=candidate_notional_decimal,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    )
    strategy = build_runtime_strategy_for_cli(
        strategy_name=strategy_name,
        run_id=fixed_run_id,
        generated_at=created_at,
        source_description=source.description,
        compression_breakout=compression_breakout,
        candidate_notional=candidate_notional_decimal,
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
        identity=fixed_identity,
    )
    repository = build_paper_daemon_repository(resolved_database_url)
    fixed_config = PaperLiveDaemonConfig(
        run_id=fixed_run_id,
        strategy_name=strategy_name,
        environment=environment,
        checkpoint_every_states=checkpoint_every_states,
        checkpoint_every_seconds=checkpoint_every_seconds,
        max_market_state_age_seconds=max_market_state_age_seconds,
        run_identity=fixed_identity,
        source_description=source.description,
        execution=ReplayExecutionConfig(
            latency_buckets=0,
            require_market_quote=require_market_quote,
        ),
        portfolio=PaperExitConfig(
            exit_mode=PaperExitMode.FIXED,
            initial_balance=Decimal(paper_initial_balance),
            take_profit_pct=Decimal(fixed_take_profit_pct),
            stop_loss_pct=Decimal(fixed_stop_loss_pct),
            max_holding_buckets=fixed_max_holding_buckets,
            require_executable_quote=require_market_quote,
        ),
    )
    candle_config = PaperLiveDaemonConfig(
        run_id=candle_run_id,
        strategy_name=strategy_name,
        environment=environment,
        checkpoint_every_states=checkpoint_every_states,
        checkpoint_every_seconds=checkpoint_every_seconds,
        max_market_state_age_seconds=max_market_state_age_seconds,
        run_identity=candle_identity,
        source_description=source.description,
        execution=ReplayExecutionConfig(
            latency_buckets=0,
            require_market_quote=require_market_quote,
        ),
        portfolio=PaperExitConfig(
            exit_mode=PaperExitMode.CANDLE_15M,
            initial_balance=Decimal(paper_initial_balance),
            max_holding_buckets=candle_max_holding_buckets,
            require_executable_quote=require_market_quote,
        ),
    )
    accounts = [
        PairedPaperLiveAccount(repository, repository, fixed_config),
        PairedPaperLiveAccount(repository, repository, candle_config),
    ]
    if third_run_id is not None:
        if third_identity is None:
            raise AssertionError("third identity must be present")
        accounts.append(
            PairedPaperLiveAccount(
                repository,
                repository,
                PaperLiveDaemonConfig(
                    run_id=third_run_id,
                    strategy_name=strategy_name,
                    environment=environment,
                    checkpoint_every_states=checkpoint_every_states,
                    checkpoint_every_seconds=checkpoint_every_seconds,
                    max_market_state_age_seconds=max_market_state_age_seconds,
                    run_identity=third_identity,
                    source_description=source.description,
                    execution=ReplayExecutionConfig(
                        latency_buckets=0,
                        require_market_quote=require_market_quote,
                    ),
                    portfolio=PaperExitConfig(
                        exit_mode=PaperExitMode.CANDLE_15M,
                        initial_balance=Decimal(paper_initial_balance),
                        max_holding_buckets=candle_max_holding_buckets,
                        candle_minimum_holding_buckets=(
                            third_candle_minimum_holding_buckets
                        ),
                        candle_confirmation_count=third_candle_confirmation_count,
                        require_executable_quote=require_market_quote,
                    ),
                ),
            )
        )
    filtered_accounts = (
        (
            "fourth",
            fourth_run_id,
            fourth_identity,
            fourth_entry_long_only,
            fourth_entry_max_abs_aggressive_imbalance,
            0,
            "0",
        ),
        (
            "fifth",
            fifth_run_id,
            fifth_identity,
            fifth_entry_long_only,
            fifth_entry_max_abs_aggressive_imbalance,
            0,
            "0",
        ),
        (
            "sixth",
            sixth_run_id,
            sixth_identity,
            sixth_entry_long_only,
            None,
            sixth_candle_grace_bars,
            sixth_candle_grace_profit_pct,
        ),
        (
            "seventh",
            seventh_run_id,
            seventh_identity,
            seventh_entry_long_only,
            None,
            seventh_candle_grace_bars,
            seventh_candle_grace_profit_pct,
        ),
    )
    for (
        ordinal,
        filtered_run_id,
        filtered_identity,
        long_only,
        max_abs_imbalance,
        grace_bars,
        grace_profit_pct,
    ) in filtered_accounts:
        if filtered_run_id is None:
            if long_only or max_abs_imbalance is not None or grace_bars:
                raise typer.BadParameter(
                    f"--{ordinal}-run-id is required for its entry filters"
                )
            continue
        if filtered_identity is None:
            raise AssertionError(f"{ordinal} identity must be present")
        accounts.append(
            PairedPaperLiveAccount(
                repository,
                repository,
                PaperLiveDaemonConfig(
                    run_id=filtered_run_id,
                    strategy_name=strategy_name,
                    environment=environment,
                    checkpoint_every_states=checkpoint_every_states,
                    checkpoint_every_seconds=checkpoint_every_seconds,
                    max_market_state_age_seconds=max_market_state_age_seconds,
                    run_identity=filtered_identity,
                    source_description=source.description,
                    execution=ReplayExecutionConfig(
                        latency_buckets=0,
                        require_market_quote=require_market_quote,
                    ),
                    portfolio=PaperExitConfig(
                        exit_mode=PaperExitMode.CANDLE_15M,
                        initial_balance=Decimal(paper_initial_balance),
                        max_holding_buckets=candle_max_holding_buckets,
                        require_executable_quote=require_market_quote,
                        candle_grace_bars=grace_bars,
                        candle_grace_profit_pct=Decimal(grace_profit_pct),
                    ),
                    entry_filter=_entry_filter_config(
                        long_only=long_only,
                        max_abs_aggressive_imbalance=max_abs_imbalance,
                    ),
                ),
            )
        )
    with BinanceRestClosedCandle15mSource(binance_base_url) as candle_source:
        result = run_paired_paper_live_daemon(
            source=source,
            strategy=strategy,
            accounts=tuple(accounts),
            clock=clock,
            entry_symbol_loader=source.load_active_symbols_at,
            candle_source=candle_source,
        )
    states_processed = result.account_results[0].processed_state_count
    halt_reason = result.account_results[0].halt_reason
    typer.echo(
        "Paper live pair completed: "
        f"strategy={strategy_name} accounts={len(accounts)} "
        f"states={states_processed} "
        f"halt={halt_reason or 'none'}"
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
        universe_repository=PostgresUniverseRepository(factory),
        shutdown=engine.dispose,
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
    engine = create_async_database_engine(database_url, pooled=False)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return PostgresPaperDaemonRepository(factory)


def _entry_filter_config(
    *,
    long_only: bool,
    max_abs_aggressive_imbalance: str | None = None,
    max_cluster_trade_count: int | None = None,
) -> PaperEntryFilterConfig:
    try:
        max_imbalance = (
            None
            if max_abs_aggressive_imbalance is None
            else Decimal(max_abs_aggressive_imbalance)
        )
        return PaperEntryFilterConfig(
            allow_long=True,
            allow_short=not long_only,
            max_abs_aggressive_imbalance=max_imbalance,
            max_cluster_trade_count=max_cluster_trade_count,
        )
    except (InvalidOperation, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


def build_runtime_strategy_for_cli(
    *,
    strategy_name: str,
    run_id: str,
    generated_at: datetime,
    source_description: str,
    compression_breakout: CompressionBreakoutConfig,
    candidate_notional: Decimal | None,
    candidate_ttl_buckets: int,
    signal_interval_seconds: int = 300,
    identity: StrategyRunIdentity | None = None,
) -> RuntimeStrategyProtocol:
    resolved_identity = identity or build_runtime_identity_for_cli(
        strategy_name=strategy_name,
        run_id=run_id,
        generated_at=generated_at,
        source_description=source_description,
        compression_breakout=compression_breakout,
        candidate_notional=candidate_notional,
        candidate_ttl_buckets=candidate_ttl_buckets,
        signal_interval_seconds=signal_interval_seconds,
    )
    config_payload: dict[str, object] = {
        "candidate_notional": candidate_notional,
        "candidate_ttl_buckets": candidate_ttl_buckets,
        "signal_interval_seconds": signal_interval_seconds,
        "compression_breakout": compression_breakout,
    }
    return build_runtime_strategy(
        strategy_name,
        config=config_payload,
        identity=resolved_identity,
    )


def build_runtime_identity_for_cli(
    *,
    strategy_name: str,
    run_id: str,
    generated_at: datetime,
    source_description: str,
    compression_breakout: CompressionBreakoutConfig,
    candidate_notional: Decimal | None,
    candidate_ttl_buckets: int,
    signal_interval_seconds: int = 300,
    code_commit: str | None = None,
) -> StrategyRunIdentity:
    config_payload: dict[str, object] = {
        "candidate_notional": candidate_notional,
        "candidate_ttl_buckets": candidate_ttl_buckets,
        "signal_interval_seconds": signal_interval_seconds,
        "compression_breakout": compression_breakout,
    }
    runtime_config = build_runtime_config(
        strategy_name,
        config=config_payload,
    )
    return StrategyRunIdentity(
        run_id=run_id,
        strategy_name=strategy_name,
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.PAPER,
        code_commit=(
            resolve_code_commit()
            if code_commit is None
            else code_commit
        ),
        created_at=generated_at,
        source_paths=(source_description,),
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
