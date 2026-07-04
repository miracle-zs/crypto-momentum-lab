from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from crypto_momentum_lab.apps.strategy_runner import main
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    PaperRunnerConfig,
    ReplayConfig,
    ReplayExecutionConfig,
)

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
            "--execution-latency-buckets",
            "2",
            "--taker-fee-rate",
            "0.0005",
            "--slippage-bps",
            "1.5",
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
                execution=ReplayExecutionConfig(
                    latency_buckets=2,
                    taker_fee_rate=Decimal("0.0005"),
                    slippage_bps=Decimal("1.5"),
                ),
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


def test_paper_command_writes_report(tmp_path: Path, monkeypatch) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "paper.json"
    source = object()
    calls: list[tuple[object, PaperRunnerConfig]] = []
    writes: list[tuple[object, Path]] = []

    def fake_build_source(state_paths: tuple[Path, ...]) -> object:
        assert state_paths == (states_root,)
        return source

    def fake_run_paper_trading(
        *,
        source: object,
        config: PaperRunnerConfig,
    ) -> object:
        calls.append((source, config))
        return SimpleNamespace(
            input_state_count=6,
            signals=(object(),),
            candidates=(object(),),
            paper_fills=(object(),),
        )

    def fake_write_paper_trading_report(report: object, path: Path) -> None:
        writes.append((report, path))

    monkeypatch.setattr(main, "build_paper_state_source", fake_build_source)
    monkeypatch.setattr(main, "run_paper_trading", fake_run_paper_trading)
    monkeypatch.setattr(
        main,
        "write_paper_trading_report",
        fake_write_paper_trading_report,
    )

    result = runner.invoke(
        main.app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--run-id",
            "paper-cli",
            "--generated-at",
            "2026-06-30T00:00:00+00:00",
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
            "--execution-latency-buckets",
            "2",
            "--taker-fee-rate",
            "0.0005",
            "--slippage-bps",
            "1.5",
            "--max-states",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            source,
            PaperRunnerConfig(
                strategy_name="compression_breakout",
                run_id="paper-cli",
                code_commit="unknown",
                generated_at=datetime(2026, 6, 30, 0, 0, tzinfo=UTC),
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
                execution=ReplayExecutionConfig(
                    latency_buckets=2,
                    taker_fee_rate=Decimal("0.0005"),
                    slippage_bps=Decimal("1.5"),
                ),
                max_states=10,
            ),
        )
    ]
    assert writes[0][1] == output_path
    assert "Paper run completed: states=6 signals=1 candidates=1 fills=1" in (
        result.stdout
    )
    assert "persisted=false" in result.stdout
    assert output_path.as_posix() in result.stdout


def test_paper_command_rejects_persist_without_database_url(
    tmp_path: Path,
    monkeypatch,
) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "paper.json"

    monkeypatch.setattr(main, "build_paper_state_source", lambda paths: object())
    monkeypatch.setattr(
        main,
        "run_paper_trading",
        lambda **kwargs: SimpleNamespace(
            input_state_count=1,
            signals=(),
            candidates=(),
            paper_fills=(),
        ),
    )
    monkeypatch.setattr(main, "write_paper_trading_report", lambda report, path: None)
    monkeypatch.delenv("CML_DATABASE_URL", raising=False)

    result = runner.invoke(
        main.app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--persist",
        ],
    )

    assert result.exit_code != 0
    assert "--persist requires --database-url or CML_DATABASE_URL" in result.output


def test_paper_command_persists_with_database_url(
    tmp_path: Path,
    monkeypatch,
) -> None:
    states_root = tmp_path / "states"
    states_root.mkdir()
    output_path = tmp_path / "paper.json"
    report = SimpleNamespace(
        input_state_count=1,
        signals=(),
        candidates=(),
        paper_fills=(),
    )
    persisted: list[tuple[object, str]] = []

    async def fake_persist_paper_report(
        paper_report: object,
        database_url: str,
    ) -> None:
        persisted.append((paper_report, database_url))

    monkeypatch.setattr(main, "build_paper_state_source", lambda paths: object())
    monkeypatch.setattr(main, "run_paper_trading", lambda **kwargs: report)
    monkeypatch.setattr(main, "write_paper_trading_report", lambda report, path: None)
    monkeypatch.setattr(
        main,
        "persist_paper_report",
        fake_persist_paper_report,
        raising=False,
    )

    result = runner.invoke(
        main.app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(output_path),
            "--persist",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
        ],
    )

    assert result.exit_code == 0
    assert persisted == [
        (report, "postgresql+asyncpg://cml:cml@localhost:54329/cml")
    ]
    assert "persisted=true" in result.stdout


def test_paper_live_source_requires_database_url(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("CML_DATABASE_URL", raising=False)

    result = runner.invoke(
        main.app,
        [
            "paper-live-source",
            "--strategy",
            "compression_breakout",
            "--environment",
            "research",
            "--output",
            str(tmp_path / "paper-live.json"),
        ],
    )

    assert result.exit_code != 0
    assert "--database-url or CML_DATABASE_URL is required" in result.output


def test_paper_live_source_runs_bounded_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_path = tmp_path / "paper-live.json"
    calls: list[tuple[str, str, int, int]] = []
    report = SimpleNamespace(
        input_state_count=2,
        signals=(),
        candidates=(),
        paper_fills=(),
    )

    def fake_build_source(
        *,
        database_url: str,
        environment: str,
        start_at,
        poll_interval_seconds: float,
        idle_timeout_seconds: float,
        max_states: int,
        batch_size: int,
    ) -> object:
        calls.append((database_url, environment, max_states, batch_size))
        return object()

    monkeypatch.setattr(main, "build_postgres_paper_source", fake_build_source)
    monkeypatch.setattr(main, "run_paper_trading", lambda **kwargs: report)
    monkeypatch.setattr(main, "write_paper_trading_report", lambda report, path: None)

    result = runner.invoke(
        main.app,
        [
            "paper-live-source",
            "--strategy",
            "compression_breakout",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
            "--environment",
            "research",
            "--output",
            str(output_path),
            "--max-states",
            "2",
            "--batch-size",
            "5",
            "--idle-timeout-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
            "research",
            2,
            5,
        )
    ]
    assert (
        "Paper live-source run completed: states=2 signals=0 candidates=0 "
        "fills=0 persisted=false"
    ) in result.stdout


def test_paper_live_daemon_requires_database_url(monkeypatch) -> None:
    monkeypatch.delenv("CML_DATABASE_URL", raising=False)

    result = runner.invoke(
        main.app,
        [
            "paper-live-daemon",
            "--strategy",
            "compression_breakout",
            "--environment",
            "research",
        ],
    )

    assert result.exit_code != 0
    assert "--database-url or CML_DATABASE_URL is required" in result.output


def test_paper_live_daemon_builds_daemon_config(monkeypatch) -> None:
    calls: list[object] = []

    def fake_run_daemon(**kwargs) -> object:
        calls.append(kwargs["config"])
        return SimpleNamespace(
            processed_state_count=3,
            halt_reason=None,
            final_cursor=datetime(2026, 7, 4, 0, 0, 30, tzinfo=UTC),
            final_checkpoint_saved_at=datetime(2026, 7, 4, 0, 0, 45, tzinfo=UTC),
        )

    monkeypatch.setattr(
        main,
        "build_postgres_paper_source",
        lambda **kwargs: SimpleNamespace(description="fake-source"),
    )
    monkeypatch.setattr(
        main,
        "build_runtime_strategy_for_cli",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        main,
        "build_paper_daemon_repository",
        lambda database_url: object(),
    )
    monkeypatch.setattr(main, "run_paper_live_daemon", fake_run_daemon)

    result = runner.invoke(
        main.app,
        [
            "paper-live-daemon",
            "--strategy",
            "compression_breakout",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
            "--environment",
            "research",
            "--run-id",
            "daemon-run",
            "--checkpoint-every-states",
            "7",
            "--checkpoint-every-seconds",
            "30",
            "--max-market-state-age-seconds",
            "90",
            "--max-states",
            "3",
            "--idle-timeout-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0
    assert calls[0].run_id == "daemon-run"
    assert calls[0].checkpoint_every_states == 7
    assert calls[0].checkpoint_every_seconds == 30
    assert calls[0].max_market_state_age_seconds == 90
    assert "Paper live daemon completed: states=3 halt=none" in result.stdout


def test_runtime_strategy_builder_supports_orderflow() -> None:
    strategy = main.build_runtime_strategy_for_cli(
        strategy_name="orderflow_impulse",
        run_id="run-orderflow",
        generated_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        source_description="memory",
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=3,
            max_range_width_pct=Decimal("0.01"),
            min_breakout_pct=Decimal("0.001"),
            acceptance_buckets=1,
            cooldown_buckets=2,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
    )

    assert strategy.metadata().name == "orderflow_impulse"
