from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import signature
from types import SimpleNamespace

from typer.testing import CliRunner

from crypto_momentum_lab.apps.live_rollout import main

app = main.app

runner = CliRunner()


def test_cli_requires_confirmation_flag_for_live_run() -> None:
    result = runner.invoke(
        app,
        ["run", "--database-url", "postgresql+asyncpg://unused"],
    )

    assert result.exit_code != 0
    assert "i-understand-this-places-real-orders" in result.output


def test_live_cli_exposes_required_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in (
        "approve",
        "prepare",
        "preflight",
        "run",
        "submit-plan",
        "status",
        "disable-new-entries",
        "report",
        "strategy-config-hash",
    ):
        assert command in result.stdout


def test_strategy_config_hash_is_stable_for_selected_strategy() -> None:
    first = runner.invoke(
        app,
        ["strategy-config-hash", "--strategy", "liquidation_cascade"],
    )
    second = runner.invoke(
        app,
        ["strategy-config-hash", "--strategy", "liquidation_cascade"],
    )

    assert first.exit_code == 0
    assert first.stdout == second.stdout
    assert len(first.stdout.strip()) == 64


def test_unlimited_cli_values_map_to_absent_capacity_limits() -> None:
    assert main._parse_optional_decimal_limit("unlimited", "--cap") is None
    assert main._parse_optional_integer_limit("unlimited", "--count") is None
    assert main._parse_optional_decimal_limit("100", "--cap") == Decimal("100")
    assert main._parse_optional_integer_limit("3", "--count") == 3
    assert main._parse_optional_decimal_limit("unlimited", "--daily-loss") is None


def test_approval_expiration_defaults_to_permanent() -> None:
    now = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)

    assert main._parse_approval_expiration(now, "never") is None
    assert main._parse_approval_expiration(now, "60") == now + timedelta(hours=1)


def test_live_run_default_has_no_symbol_cooldown() -> None:
    assert signature(main.run_command).parameters["cooldown_seconds"].default == 0


async def test_live_warmup_ignores_outputs_and_stops_before_fresh_states() -> None:
    now = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    stale = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=now - timedelta(seconds=46),
        bucket_end=now - timedelta(seconds=31),
    )
    fresh = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=now - timedelta(seconds=45),
        bucket_end=now - timedelta(seconds=30),
    )

    class Strategy:
        def __init__(self) -> None:
            self.seen = []

        def on_market_state(self, state):
            self.seen.append(state)
            return SimpleNamespace(candidates=())

    class Repository:
        async def load_after(self, **kwargs):
            assert kwargs["environment"] == "research"
            return (stale, fresh)

    strategy = Strategy()
    cursor = await main._warm_live_strategy(
        strategy=strategy,
        repository=Repository(),
        environment="research",
        now=now,
        stale_after_seconds=30,
    )

    assert strategy.seen == [stale]
    assert cursor.bucket_start == stale.bucket_start
    assert cursor.symbol == "BTCUSDT"
