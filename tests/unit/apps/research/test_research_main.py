from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from typer.testing import CliRunner

from crypto_momentum_lab.apps.research import main
from crypto_momentum_lab.persistence.parquet import (
    DatasetName,
    DerivedDatasetManifest,
)
from crypto_momentum_lab.research.datasets import DerivedMarketDatasets

runner = CliRunner()


def test_derive_datasets_command_prints_manifest_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    raw_path = tmp_path / "raw.jsonl.zst"
    raw_path.write_bytes(b"raw")
    output_root = tmp_path / "derived"
    calls: list[tuple[tuple[Path, ...], Path]] = []

    def fake_derive_market_datasets(
        *,
        raw_paths: tuple[Path, ...],
        output_root: Path,
    ) -> DerivedMarketDatasets:
        calls.append((raw_paths, output_root))
        return DerivedMarketDatasets(
            market_event_manifests=(
                _manifest(DatasetName.MARKET_EVENTS, "market_events/out.parquet"),
            ),
            market_state_manifests=(
                _manifest(
                    DatasetName.MARKET_STATES_15S,
                    "market_states_15s/out.parquet",
                ),
            ),
        )

    monkeypatch.setattr(
        main,
        "derive_market_datasets",
        fake_derive_market_datasets,
    )

    result = runner.invoke(
        main.app,
        [
            "derive-datasets",
            "--output-root",
            str(output_root),
            str(raw_path),
        ],
    )

    assert result.exit_code == 0
    assert calls == [((raw_path,), output_root)]
    assert "market_events=1" in result.stdout
    assert "market_states_15s=1" in result.stdout
    assert "market_events/out.parquet" in result.stdout


def _manifest(dataset: DatasetName, relative_path: str) -> DerivedDatasetManifest:
    return DerivedDatasetManifest(
        manifest_id=UUID(int=1),
        dataset_name=dataset,
        schema_version=1,
        relative_path=Path(relative_path),
        row_count=1,
        input_paths=("raw.jsonl.zst",),
        input_sha256="input",
        output_sha256="output",
        first_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        last_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        created_at=datetime(2026, 6, 15, 2, 1, tzinfo=UTC),
    )
