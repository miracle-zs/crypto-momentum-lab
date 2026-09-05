import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import signature
from types import SimpleNamespace

import pytest
from typer import BadParameter
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
        "resolve-missing-order",
        "run",
        "submit-plan",
        "status",
        "disable-new-entries",
        "report",
        "strategy-config-hash",
    ):
        assert command in result.stdout


def test_live_run_exposes_operation_aware_telemetry_option() -> None:
    result = runner.invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "--persist-exchan" in result.stdout


def test_live_run_rejects_conflicting_entry_policy_modes() -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--entry-policy-compare-only",
            "--entry-policy-enforce",
            "--i-understand-this-places-real-orders",
        ],
    )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("", None),
        ("  ", None),
        ("submit, cancel,submit", frozenset({"submit", "cancel"})),
    ],
)
def test_live_exchange_operation_option_is_parsed_explicitly(
    raw_value: str,
    expected: frozenset[str] | None,
) -> None:
    assert main._parse_exchange_operations(raw_value) == expected


def test_live_exchange_operation_option_rejects_empty_tokens() -> None:
    with pytest.raises(BadParameter, match="comma-separated list"):
        main._parse_exchange_operations("submit,,cancel")


def test_live_run_passes_exchange_operation_allowlist_to_daemon(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_run_live_daemon(**kwargs: object):
        captured.update(kwargs)
        return main.LiveDaemonResult(
            processed_state_count=0,
            approved_intent_count=0,
            submitted_order_count=0,
            halt_reason=None,
            final_state_at=None,
        )

    async def fake_startup_backoff(run_once):
        return await run_once()

    monkeypatch.setattr(main, "_run_live_daemon", fake_run_live_daemon)
    monkeypatch.setattr(
        main,
        "_run_with_live_startup_backoff",
        fake_startup_backoff,
    )
    monkeypatch.setenv("BINANCE_TRADE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_TRADE_API_SECRET", "test-secret")

    result = runner.invoke(
        app,
        [
            "run",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--persist-exchange-operations",
            "submit,cancel",
            "--entry-policy-compare-only",
            "--i-understand-this-places-real-orders",
        ],
    )

    assert result.exit_code == 0
    assert captured["persist_exchange_operations"] == frozenset(
        {"submit", "cancel"}
    )
    assert captured["entry_policy_compare_only"] is True


def test_live_run_passes_entry_policy_enforce_to_daemon(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_run_live_daemon(**kwargs: object):
        captured.update(kwargs)
        return main.LiveDaemonResult(
            processed_state_count=0,
            approved_intent_count=0,
            submitted_order_count=0,
            halt_reason=None,
            final_state_at=None,
        )

    async def fake_startup_backoff(run_once):
        return await run_once()

    monkeypatch.setattr(main, "_run_live_daemon", fake_run_live_daemon)
    monkeypatch.setattr(
        main,
        "_run_with_live_startup_backoff",
        fake_startup_backoff,
    )
    monkeypatch.setenv("BINANCE_TRADE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_TRADE_API_SECRET", "test-secret")

    result = runner.invoke(
        app,
        [
            "run",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--entry-policy-enforce",
            "--i-understand-this-places-real-orders",
        ],
    )

    assert result.exit_code == 0
    assert captured["entry_policy_enforce"] is True


def test_live_cli_legacy_credentials_require_explicit_fallback(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_TRADE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TRADE_API_SECRET", raising=False)
    monkeypatch.setenv("BINANCE_API_KEY", "legacy-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "legacy-secret")

    with pytest.raises(BadParameter, match="BINANCE_TRADE_API_KEY"):
        main._resolve_live_cli_credentials(
            api_key_env=None,
            api_secret_env=None,
            allow_legacy_fallback=False,
        )

    resolved = main._resolve_live_cli_credentials(
        api_key_env=None,
        api_secret_env=None,
        allow_legacy_fallback=True,
    )
    assert resolved.api_key == "legacy-key"
    assert resolved.api_key_env == "BINANCE_API_KEY"


def test_resolve_missing_order_requires_exact_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "resolve-missing-order",
            "--client-order-id",
            "cml_missing",
            "--operator",
            "operator",
        ],
    )

    assert result.exit_code != 0
    assert "RESOLVE MISSING LIVE ORDER" in result.output


def test_missing_order_resolution_guard_accepts_confirmed_absent_reduce_only_order(
) -> None:
    main._validate_missing_order_resolution(
        state="unknown_pending_reconciliation",
        reduce_only=True,
        exchange_order_id=None,
        created_at=datetime(2026, 8, 31, 4, 0, tzinfo=UTC),
        now=datetime(2026, 8, 31, 4, 20, tzinfo=UTC),
        order_quantity=Decimal("2159.3"),
        executed_quantity=Decimal("0"),
        position_quantity=Decimal("2159.3"),
        exchange_order_found=False,
        matching_open_order_found=False,
        min_missing_age_seconds=600,
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"reduce_only": False}, "reduce-only"),
        ({"exchange_order_found": True}, "still exists"),
        ({"matching_open_order_found": True}, "open order"),
        ({"position_quantity": Decimal("2000")}, "position quantity changed"),
        (
            {"now": datetime(2026, 8, 31, 4, 5, tzinfo=UTC)},
            "younger than",
        ),
    ],
)
def test_missing_order_resolution_guard_fails_closed(
    overrides: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "state": "unknown_pending_reconciliation",
        "reduce_only": True,
        "exchange_order_id": None,
        "created_at": datetime(2026, 8, 31, 4, 0, tzinfo=UTC),
        "now": datetime(2026, 8, 31, 4, 20, tzinfo=UTC),
        "order_quantity": Decimal("2159.3"),
        "executed_quantity": Decimal("0"),
        "position_quantity": Decimal("2159.3"),
        "exchange_order_found": False,
        "matching_open_order_found": False,
        "min_missing_age_seconds": 600,
    }
    values.update(overrides)

    with pytest.raises(RuntimeError, match=message):
        main._validate_missing_order_resolution(**values)  # type: ignore[arg-type]


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
    enforced = main._live_strategy_config_hash(
        "orderflow_impulse",
        entry_positive_gainer_top_count=None,
        entry_policy_enforce=True,
        require_price_above_ema5=False,
        require_price_above_ema10=False,
    )

    assert filtered != unfiltered
    assert enforced != unfiltered


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
