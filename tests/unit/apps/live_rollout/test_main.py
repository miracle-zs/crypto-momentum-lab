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


def test_live_run_does_not_expose_removed_safety_limits() -> None:
    parameters = signature(main.run_command).parameters
    for removed in (
        "cooldown_seconds",
        "max_spread",
        "state_stale_after_seconds",
        "max_holding_seconds",
    ):
        assert removed not in parameters


async def test_live_warmup_applies_all_states_and_continues_from_boundary() -> None:
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
    )

    assert strategy.seen == [stale, fresh]
    assert cursor.bucket_start == fresh.bucket_start
    assert cursor.symbol == "BTCUSDT"


async def test_shadow_preflight_accepts_an_old_matching_session() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.statement = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, statement):
            self.statement = statement
            return "shadow-old"

    class FakeFactory:
        def __init__(self, session: FakeSession) -> None:
            self.session = session

        def __call__(self):
            return self.session

    session = FakeSession()
    factory = FakeFactory(session)

    assert await main._has_matching_shadow_session(
        factory,
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
    )
    assert session.statement is not None
    assert "ended_at >=" not in str(session.statement)


async def test_missing_shadow_preflight_only_logs_a_warning(
    monkeypatch,
) -> None:
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return None

    class FakeFactory:
        def __call__(self):
            return FakeSession()

    warnings = []

    class FakeLogger:
        def warning(self, event, **kwargs):
            warnings.append((event, kwargs))

    monkeypatch.setattr(main, "log", FakeLogger())

    await main._warn_if_shadow_preflight_missing(
        FakeFactory(),
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        account_label="primary",
        session_id="live-1",
    )

    assert warnings == [
        (
            "live_shadow_preflight_missing",
            {
                "account_label": "primary",
                "session_id": "live-1",
                "strategy_name": "orderflow_impulse",
                "strategy_config_hash": "a" * 64,
            },
        )
    ]
