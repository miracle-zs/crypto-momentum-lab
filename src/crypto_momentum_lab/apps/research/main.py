from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer

from crypto_momentum_lab.research.compression_breakout import (
    build_compression_breakout_report,
)
from crypto_momentum_lab.research.datasets import derive_market_datasets
from crypto_momentum_lab.research.liquidation_cascade import (
    build_liquidation_cascade_report,
)
from crypto_momentum_lab.research.order_flow_impulse import (
    build_order_flow_impulse_report,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
)
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowImpulseConfig,
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
            help="Completed signal buckets used for the frozen range.",
        ),
    ] = 20,
    signal_interval_seconds: Annotated[
        int,
        typer.Option(
            "--signal-interval-seconds",
            min=15,
            help="Signal bucket duration; raw 15-second states are aggregated.",
        ),
    ] = 300,
    max_range_width_pct: Annotated[
        str,
        typer.Option(
            "--max-range-width-pct",
            help="Maximum lookback range width as a decimal percentage.",
        ),
    ] = "0.025",
    min_breakout_pct: Annotated[
        str,
        typer.Option(
            "--min-breakout-pct",
            help="Minimum breakout distance beyond the frozen range boundary.",
        ),
    ] = "0.003",
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
    ] = 12,
    forward_horizon_buckets: Annotated[
        list[int] | None,
        typer.Option(
            "--forward-horizon-buckets",
            min=1,
            help="Forward-label horizon in signal buckets. Repeatable.",
        ),
    ] = None,
) -> None:
    horizons = tuple(forward_horizon_buckets or [1, 3, 6, 12])
    report = build_compression_breakout_report(
        state_paths=(states_root,),
        output_path=output_path,
        signal_interval_seconds=signal_interval_seconds,
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


@app.command("order-flow-impulse-study")
def order_flow_impulse_study_command(
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
    impulse_window_buckets: Annotated[
        int,
        typer.Option(
            "--impulse-window-buckets",
            min=2,
            help="15-second states used for the impulse window.",
        ),
    ] = 4,
    baseline_window_buckets: Annotated[
        int,
        typer.Option(
            "--baseline-window-buckets",
            min=1,
            help="Historical states before the impulse window for notional baseline.",
        ),
    ] = 40,
    breakout_window_buckets: Annotated[
        int,
        typer.Option(
            "--breakout-window-buckets",
            min=1,
            help="Completed states before candidate used for frozen high/low.",
        ),
    ] = 20,
    min_return_pct: Annotated[
        str,
        typer.Option(
            "--min-return-pct",
            help="Minimum directional impulse return as decimal percentage.",
        ),
    ] = "0.003",
    min_aggressive_imbalance: Annotated[
        str,
        typer.Option(
            "--min-aggressive-imbalance",
            help="Minimum absolute aggressive notional imbalance.",
        ),
    ] = "0.35",
    min_notional_intensity: Annotated[
        str,
        typer.Option(
            "--min-notional-intensity",
            help="Minimum impulse notional versus historical baseline.",
        ),
    ] = "1.5",
    confirmation_buckets: Annotated[
        int,
        typer.Option(
            "--confirmation-buckets",
            min=1,
            help="Consecutive buckets that must stay beyond the breakout level.",
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
    horizons = tuple(forward_horizon_buckets or [2, 4, 8, 12, 20, 40, 60])
    report = build_order_flow_impulse_report(
        state_paths=(states_root,),
        output_path=output_path,
        config=OrderFlowImpulseConfig(
            impulse_window_buckets=impulse_window_buckets,
            baseline_window_buckets=baseline_window_buckets,
            breakout_window_buckets=breakout_window_buckets,
            min_return_pct=Decimal(min_return_pct),
            min_aggressive_imbalance=Decimal(min_aggressive_imbalance),
            min_notional_intensity=Decimal(min_notional_intensity),
            confirmation_buckets=confirmation_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=horizons,
        ),
    )
    typer.echo(f"order_flow_impulse_events={report.summary.total_count}")
    typer.echo(output_path.as_posix())


@app.command("liquidation-cascade-study")
def liquidation_cascade_study_command(
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
    liquidation_window_buckets: Annotated[
        int,
        typer.Option(
            "--liquidation-window-buckets",
            min=1,
            help="15-second states used for liquidation cluster detection.",
        ),
    ] = 2,
    breakout_window_buckets: Annotated[
        int,
        typer.Option(
            "--breakout-window-buckets",
            min=1,
            help="Completed states before candidate used for frozen high/low.",
        ),
    ] = 8,
    min_liquidation_count: Annotated[
        int,
        typer.Option(
            "--min-liquidation-count",
            min=1,
            help="Minimum liquidation records in the cluster window.",
        ),
    ] = 1,
    min_liquidation_notional: Annotated[
        str,
        typer.Option(
            "--min-liquidation-notional",
            help="Minimum liquidation notional in the cluster window.",
        ),
    ] = "10000",
    min_price_move_pct: Annotated[
        str,
        typer.Option(
            "--min-price-move-pct",
            help="Minimum directional price move during the cluster.",
        ),
    ] = "0.003",
    min_aggressive_imbalance: Annotated[
        str,
        typer.Option(
            "--min-aggressive-imbalance",
            help="Minimum absolute aggressive notional imbalance.",
        ),
    ] = "0.35",
    confirmation_buckets: Annotated[
        int,
        typer.Option(
            "--confirmation-buckets",
            min=1,
            help="Consecutive buckets that must stay beyond the breakout level.",
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
    horizons = tuple(forward_horizon_buckets or [2, 4, 6, 8, 20, 40])
    report = build_liquidation_cascade_report(
        state_paths=(states_root,),
        output_path=output_path,
        config=LiquidationCascadeConfig(
            liquidation_window_buckets=liquidation_window_buckets,
            breakout_window_buckets=breakout_window_buckets,
            min_liquidation_count=min_liquidation_count,
            min_liquidation_notional=Decimal(min_liquidation_notional),
            min_price_move_pct=Decimal(min_price_move_pct),
            min_aggressive_imbalance=Decimal(min_aggressive_imbalance),
            confirmation_buckets=confirmation_buckets,
            cooldown_buckets=cooldown_buckets,
            forward_horizon_buckets=horizons,
        ),
    )
    typer.echo(f"liquidation_cascade_events={report.summary.total_count}")
    typer.echo(output_path.as_posix())
