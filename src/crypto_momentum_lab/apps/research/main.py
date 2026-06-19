from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer

from crypto_momentum_lab.research.compression_breakout import (
    build_compression_breakout_report,
)
from crypto_momentum_lab.research.datasets import derive_market_datasets
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def research_app() -> None:
    """Offline research utilities."""


@app.command("derive-datasets")
def derive_datasets_command(
    raw_paths: Annotated[
        list[Path],
        typer.Argument(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Finalized raw .jsonl.zst archive files.",
        ),
    ],
    output_root: Annotated[
        Path,
        typer.Option(
            "--output-root",
            file_okay=False,
            help="Root directory for derived Parquet datasets.",
        ),
    ],
) -> None:
    result = derive_market_datasets(
        raw_paths=tuple(raw_paths),
        output_root=output_root,
    )
    typer.echo(f"market_events={len(result.market_event_manifests)}")
    for manifest in result.market_event_manifests:
        typer.echo(manifest.relative_path.as_posix())
    typer.echo(f"market_states_15s={len(result.market_state_manifests)}")
    for manifest in result.market_state_manifests:
        typer.echo(manifest.relative_path.as_posix())


@app.command("compression-breakout-study")
def compression_breakout_study_command(
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
            help="JSON report output path.",
        ),
    ],
    compression_window_buckets: Annotated[
        int,
        typer.Option(
            "--compression-window-buckets",
            min=1,
            help="Completed 15-second states used for the frozen range.",
        ),
    ] = 20,
    max_range_width_pct: Annotated[
        str,
        typer.Option(
            "--max-range-width-pct",
            help="Maximum lookback range width as a decimal percentage.",
        ),
    ] = "0.005",
    min_breakout_pct: Annotated[
        str,
        typer.Option(
            "--min-breakout-pct",
            help="Minimum breakout distance beyond the frozen range boundary.",
        ),
    ] = "0.001",
    acceptance_buckets: Annotated[
        int,
        typer.Option(
            "--acceptance-buckets",
            min=1,
            help="Consecutive buckets that must remain outside the range.",
        ),
    ] = 1,
    cooldown_buckets: Annotated[
        int,
        typer.Option(
            "--cooldown-buckets",
            min=0,
            help="Buckets skipped after a detected event.",
        ),
    ] = 8,
    forward_horizon_buckets: Annotated[
        list[int] | None,
        typer.Option(
            "--forward-horizon-buckets",
            min=1,
            help="Forward-label horizon in 15-second buckets. Repeatable.",
        ),
    ] = None,
) -> None:
    horizons = tuple(forward_horizon_buckets or [2, 4, 8, 12, 20, 40, 60, 120, 240])
    report = build_compression_breakout_report(
        state_paths=(states_root,),
        output_path=output_path,
        config=CompressionBreakoutConfig(
            compression_window_buckets=compression_window_buckets,
            max_range_width_pct=Decimal(max_range_width_pct),
            min_breakout_pct=Decimal(min_breakout_pct),
            acceptance_buckets=acceptance_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=horizons,
        ),
    )
    typer.echo(f"compression_breakout_events={report.summary.total_count}")
    typer.echo(output_path.as_posix())
