import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import signature
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from crypto_momentum_lab.apps.live_rollout import main
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.market_data.hub import MarketStateHubError

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


def test_strategy_config_hash_includes_live_entry_filters() -> None:
    filtered = main._live_strategy_config_hash(
        "orderflow_impulse",
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )
    unfiltered = main._live_strategy_config_hash(
        "orderflow_impulse",
        entry_positive_gainer_top_count=None,
        require_price_above_ema5=False,
        require_price_above_ema10=False,
    )

    assert filtered != unfiltered


def test_live_defaults_disable_ema_and_use_lower_orderflow_imbalance() -> None:
    assert main._LIVE_ENTRY_PRICE_ABOVE_EMA5 is False
    assert main._LIVE_ENTRY_PRICE_ABOVE_EMA10 is False
    assert main._live_strategy_config()[
        "order_flow_impulse_min_aggressive_imbalance"
    ] == Decimal("0.40")


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


def test_live_startup_retry_delay_uses_exchange_retry_after() -> None:
    assert main._live_startup_retry_delay(1, retry_after_seconds=17) == 17
    assert main._live_startup_retry_delay(2, retry_after_seconds=None) == 30
    assert main._live_startup_retry_delay(10, retry_after_seconds=None) == 300


def test_only_transient_live_startup_errors_are_retryable() -> None:
    assert main._is_retryable_live_startup_error(
        RuntimeError("live gate blocked: missing_active_lease")
    )
    assert main._is_retryable_live_startup_error(TimeoutError("recovery timed out"))
    assert not main._is_retryable_live_startup_error(
        RuntimeError("position mode mismatch: expected hedge, got one-way")
    )


@pytest.mark.asyncio
async def test_compact_checkpoint_recovery_warms_without_evaluating_signals() -> None:
    now = datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
    warmed: list[object] = []
    seen: dict[str, object] = {}

    class Strategy:
        def required_data(self):
            return SimpleNamespace(warmup_buckets=1)

        def warm_market_state(self, state) -> None:
            warmed.append(state)

        def checkpoint(self, *, include_market_state_buffers=True):
            assert include_market_state_buffers is False
            return StrategyCheckpoint(
                last_processed_at_by_symbol={"BTCUSDT": now},
                warmup_buckets_by_symbol={"BTCUSDT": len(warmed)},
                cooldown_buckets_remaining_by_symbol={"BTCUSDT": 2},
                payload={"signal_sequence": 4},
            )

    class Repository:
        async def load_recovery_window(self, **kwargs):
            seen.update(kwargs)
            return (SimpleNamespace(symbol="BTCUSDT"),)

    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": now},
        warmup_buckets_by_symbol={"BTCUSDT": 7},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 2},
        payload={"signal_sequence": 4},
    )

    await main._restore_live_strategy_from_checkpoint(
        strategy=Strategy(),
        checkpoint=checkpoint,
        repository=Repository(),
        environment="research",
    )

    assert len(warmed) == 1
    assert seen["environment"] == "research"
    assert seen["last_processed_at_by_symbol"] == {"BTCUSDT": now}


@pytest.mark.asyncio
async def test_periodic_reconcile_runs_outside_market_state_loop(monkeypatch) -> None:
    calls = 0
    delays: list[float] = []

    async def fake_reconcile(**_: object) -> None:
        nonlocal calls
        calls += 1

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(main, "_reconcile_run_orders", fake_reconcile)

    with pytest.raises(asyncio.CancelledError):
        await main._periodic_reconcile_run_orders(
            order_repository=object(),  # type: ignore[arg-type]
            state_machine=object(),  # type: ignore[arg-type]
            run_id="live-manual",
            interval_seconds=60,
            sleep=controlled_sleep,
        )

    assert calls == 1
    assert delays == [60, 60]


def test_live_lease_auto_reacquire_requires_prior_live_session() -> None:
    assert main._should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=False,
        gate_reasons=("missing_active_lease",),
    )
    assert not main._should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=False,
        draining=False,
        gate_reasons=("missing_active_lease",),
    )
    assert not main._should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=True,
        gate_reasons=("missing_active_lease",),
    )
    assert not main._should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=False,
        gate_reasons=("missing_active_lease", "active_risk_halt"),
    )


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


async def test_resilient_market_state_stream_retries_after_hub_failure() -> None:
    state = SimpleNamespace(symbol="BTCUSDT")

    class Source:
        def __init__(self) -> None:
            self.attempts = 0

        def __aiter__(self):
            self.attempts += 1
            attempt = self.attempts

            async def stream():
                if attempt == 1:
                    raise MarketStateHubError("market-state hub unavailable")
                yield state

            return stream()

    source = Source()
    observed = []

    async for item in main._resilient_market_state_stream(
        source,
        retry_delay_seconds=0,
    ):
        observed.append(item)

    assert observed == [state]
    assert source.attempts == 2


@pytest.mark.asyncio
async def test_resilient_account_event_stream_retries_after_hub_failure() -> None:
    event = SimpleNamespace(event_type="ORDER_TRADE_UPDATE")

    class Source:
        def __init__(self) -> None:
            self.attempts = 0

        def __aiter__(self):
            self.attempts += 1
            attempt = self.attempts

            async def stream():
                if attempt == 1:
                    raise main.AccountEventHubError("account-event hub unavailable")
                yield event

            return stream()

    source = Source()
    observed = []

    async for item in main._resilient_account_event_stream(
        source,
        retry_delay_seconds=0,
    ):
        observed.append(item)

    assert observed == [event]
    assert source.attempts == 2


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
