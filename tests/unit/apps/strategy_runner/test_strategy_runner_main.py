from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from crypto_momentum_lab.apps.strategy_runner import main
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import ReplayConfig

runner = CliRunner()


def test_replay_command_writes_report(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "report.json"
    calls: list[tuple[tuple[Path, ...], ReplayConfig]] = []
    writes: list[tuple[object, Path]] = []

    def fake_build_strategy_replay_report(
        *,
        state_paths: tuple[Path, ...],
        config: ReplayConfig,
    ) -> object:
        calls.append((state_paths, config))
        return SimpleNamespace(
            input_state_count=5,
            signals=(object(), object()),
            candidates=(object(),),
        )

    def fake_write_strategy_replay_report(report: object, path: Path) -> None:
        writes.append((report, path))

    monkeypatch.setattr(
        main,
        "build_strategy_replay_report",
        fake_build_strategy_replay_report,
    )
    monkeypatch.setattr(
        main,
        "write_strategy_replay_report",
        fake_write_strategy_replay_report,
    )

    result = runner.invoke(
        main.app,
        [
            "replay",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--run-id",
            "run-cli",
            "--generated-at",
            "2026-06-22T00:00:00+00:00",
            "--compression-window-buckets",
            "3",
            "--max-range-width-pct",
            "0.01",
            "--min-breakout-pct",
            "0.001",
            "--acceptance-buckets",
            "2",
            "--cooldown-buckets",
            "4",
            "--candidate-notional",
            "250",
            "--candidate-ttl-buckets",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            (states_root,),
            ReplayConfig(
                strategy_name="compression_breakout",
                run_id="run-cli",
                code_commit="unknown",
                generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
                compression_breakout=CompressionBreakoutConfig(
                    compression_window_buckets=3,
                    max_range_width_pct=Decimal("0.01"),
                    min_breakout_pct=Decimal("0.001"),
                    acceptance_buckets=2,
                    cooldown_buckets=4,
                    forward_horizon_buckets=(1,),
                ),
                candidate_notional=Decimal("250"),
                candidate_ttl_buckets=3,
            ),
        )
    ]
    assert writes[0][1] == output_path
    assert "Replay completed: states=5 signals=2 candidates=1" in result.stdout
    assert output_path.as_posix() in result.stdout


def test_replay_command_generates_default_run_id(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "report.json"
    configs: list[ReplayConfig] = []

    def fake_build_strategy_replay_report(
        *,
        state_paths: tuple[Path, ...],
        config: ReplayConfig,
    ) -> object:
        configs.append(config)
        return SimpleNamespace(input_state_count=0, signals=(), candidates=())

    monkeypatch.setattr(
        main,
        "build_strategy_replay_report",
        fake_build_strategy_replay_report,
    )
    monkeypatch.setattr(main, "write_strategy_replay_report", lambda report, path: None)

    result = runner.invoke(
        main.app,
        [
            "replay",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert configs[0].run_id.startswith("replay-")
    assert configs[0].generated_at.tzinfo is not None
