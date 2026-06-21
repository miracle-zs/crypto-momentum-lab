from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from typer.testing import CliRunner

from crypto_momentum_lab.apps.research import main
from crypto_momentum_lab.persistence.parquet import (
    DatasetName,
    DerivedDatasetManifest,
)
from crypto_momentum_lab.research.datasets import DerivedMarketDatasets
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
)
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowImpulseConfig,
)

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


def test_compression_breakout_study_command_writes_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "report.json"
    calls: list[tuple[tuple[Path, ...], Path, CompressionBreakoutConfig]] = []

    def fake_build_compression_breakout_report(
        *,
        state_paths: tuple[Path, ...],
        output_path: Path,
        config: CompressionBreakoutConfig,
    ) -> object:
        calls.append((state_paths, output_path, config))
        return SimpleNamespace(summary=SimpleNamespace(total_count=2))

    monkeypatch.setattr(
        main,
        "build_compression_breakout_report",
        fake_build_compression_breakout_report,
    )

    result = runner.invoke(
        main.app,
        [
            "compression-breakout-study",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--compression-window-buckets",
            "4",
            "--max-range-width-pct",
            "0.01",
            "--min-breakout-pct",
            "0.001",
            "--acceptance-buckets",
            "2",
            "--cooldown-buckets",
            "3",
            "--forward-horizon-buckets",
            "1",
            "--forward-horizon-buckets",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            (states_root,),
            output_path,
            CompressionBreakoutConfig(
                compression_window_buckets=4,
                max_range_width_pct=Decimal("0.01"),
                min_breakout_pct=Decimal("0.001"),
                acceptance_buckets=2,
                cooldown_buckets=3,
                forward_horizon_buckets=(1, 2),
            ),
        )
    ]
    assert "compression_breakout_events=2" in result.stdout


def test_order_flow_impulse_study_command_writes_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "order-flow-report.json"
    calls: list[tuple[tuple[Path, ...], Path, OrderFlowImpulseConfig]] = []

    def fake_build_order_flow_impulse_report(
        *,
        state_paths: tuple[Path, ...],
        output_path: Path,
        config: OrderFlowImpulseConfig,
    ) -> object:
        calls.append((state_paths, output_path, config))
        return SimpleNamespace(summary=SimpleNamespace(total_count=3))

    monkeypatch.setattr(
        main,
        "build_order_flow_impulse_report",
        fake_build_order_flow_impulse_report,
    )

    result = runner.invoke(
        main.app,
        [
            "order-flow-impulse-study",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--impulse-window-buckets",
            "3",
            "--baseline-window-buckets",
            "4",
            "--breakout-window-buckets",
            "5",
            "--min-return-pct",
            "0.01",
            "--min-aggressive-imbalance",
            "0.50",
            "--min-notional-intensity",
            "2",
            "--confirmation-buckets",
            "2",
            "--cooldown-buckets",
            "6",
            "--forward-horizon-buckets",
            "1",
            "--forward-horizon-buckets",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            (states_root,),
            output_path,
            OrderFlowImpulseConfig(
                impulse_window_buckets=3,
                baseline_window_buckets=4,
                breakout_window_buckets=5,
                min_return_pct=Decimal("0.01"),
                min_aggressive_imbalance=Decimal("0.50"),
                min_notional_intensity=Decimal("2"),
                confirmation_buckets=2,
                cooldown_buckets=6,
                forward_horizon_buckets=(1, 2),
            ),
        )
    ]
    assert "order_flow_impulse_events=3" in result.stdout


def test_liquidation_cascade_study_command_writes_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "liquidation-report.json"
    calls: list[tuple[tuple[Path, ...], Path, LiquidationCascadeConfig]] = []

    def fake_build_liquidation_cascade_report(
        *,
        state_paths: tuple[Path, ...],
        output_path: Path,
        config: LiquidationCascadeConfig,
    ) -> object:
        calls.append((state_paths, output_path, config))
        return SimpleNamespace(summary=SimpleNamespace(total_count=4))

    monkeypatch.setattr(
        main,
        "build_liquidation_cascade_report",
        fake_build_liquidation_cascade_report,
    )

    result = runner.invoke(
        main.app,
        [
            "liquidation-cascade-study",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--liquidation-window-buckets",
            "2",
            "--breakout-window-buckets",
            "5",
            "--min-liquidation-count",
            "2",
            "--min-liquidation-notional",
            "500",
            "--min-price-move-pct",
            "0.01",
            "--min-aggressive-imbalance",
            "0.50",
            "--confirmation-buckets",
            "2",
            "--cooldown-buckets",
            "6",
            "--forward-horizon-buckets",
            "1",
            "--forward-horizon-buckets",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            (states_root,),
            output_path,
            LiquidationCascadeConfig(
                liquidation_window_buckets=2,
                breakout_window_buckets=5,
                min_liquidation_count=2,
                min_liquidation_notional=Decimal("500"),
                min_price_move_pct=Decimal("0.01"),
                min_aggressive_imbalance=Decimal("0.50"),
                confirmation_buckets=2,
                cooldown_buckets=6,
                forward_horizon_buckets=(1, 2),
            ),
        )
    ]
    assert "liquidation_cascade_events=4" in result.stdout


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
