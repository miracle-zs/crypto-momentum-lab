from pathlib import Path
from typing import Annotated

import typer

from crypto_momentum_lab.research.datasets import derive_market_datasets

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
