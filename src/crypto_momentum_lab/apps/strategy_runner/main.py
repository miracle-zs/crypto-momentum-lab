from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer

from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    ReplayConfig,
    build_strategy_replay_report,
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
) -> None:
    created_at = _parse_generated_at(generated_at)
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
    typer.echo(output_path.as_posix())


def _parse_generated_at(value: str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
