import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import signature
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer import BadParameter
from typer.testing import CliRunner

from crypto_momentum_lab.apps.live_rollout import main
from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.execution_account.hub import AccountEventHubError
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    restore_live_strategy_from_checkpoint,
    validate_live_warmup_coverage,
    warm_live_strategy,
)
from crypto_momentum_lab.live_rollout.startup_resilience import (
    is_retryable_live_startup_error,
    live_startup_retry_delay,
    should_auto_reacquire_live_lease,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_account_event_stream,
    resilient_market_state_stream,
)
from crypto_momentum_lab.market_data.hub import MarketStateHubError

app = main.app

runner = CliRunner()


def test_live_entry_positive_gainer_top_count_resolves_environment(monkeypatch) -> None:
    monkeypatch.setenv("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "25")

    assert main._resolve_live_entry_positive_gainer_top_count(None) == 25
    assert main._resolve_live_entry_positive_gainer_top_count(12) == 12


def test_live_entry_positive_gainer_top_count_rejects_invalid_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "0")

    with pytest.raises(BadParameter, match="must be positive"):
        main._resolve_live_entry_positive_gainer_top_count(None)


def test_strategy_hash_can_be_derived_from_runtime_manifest(monkeypatch) -> None:
    monkeypatch.setenv("CML_CODE_COMMIT", "a" * 40)

    result = runner.invoke(
        app,
        [
            "strategy-config-hash",
            "--account-label",
            "account-3",
            "--runtime-manifest",
            "deploy/live-runtime.yaml",
        ],
    )

    assert result.exit_code == 0
    assert len(result.stdout.strip()) == 64


def test_live_run_uses_runtime_manifest_identity_and_strategy_inputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "runtime.yaml"
    manifest_path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  migration_revision: "20260911_0040"
accounts:
  - label: account-2
    strategy: orderflow_impulse
    session_id: live-account-2-v1
    lease_owner: live-worker-account-2
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy-account-2]
    strategy_config:
      impulse_window_buckets: 2
      confirmation_buckets: 1
      min_return_pct: 0.007
      min_imbalance: 0.35
      min_intensity: 2.5
      min_notional_5m_vs_30m: 1.75
      cooldown_buckets: 2
      entry_positive_gainer_top_count: 17
      require_price_above_ema5: true
      require_price_above_ema10: false
      entry_policy_mode: compare_only
      entry_order_type: limit
      entry_limit_ttl_seconds: 1200
""",
        encoding="utf-8",
    )
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
    monkeypatch.setattr(
        main,
        "_resolve_live_cli_credentials",
        lambda **_: SimpleNamespace(
            api_key="test-key",
            api_secret="test-secret",
            metadata=lambda: {
                "credential_role": "trade",
                "api_key_env": "BINANCE_TRADE_API_KEY",
                "api_secret_env": "BINANCE_TRADE_API_SECRET",
                "api_key_fingerprint": "test-fingerprint",
            },
        ),
    )

    result = runner.invoke(
        app,
        [
            "run",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--account-label",
            "account-2",
            "--runtime-manifest",
            str(manifest_path),
            "--git-commit-hash",
            "a" * 40,
            "--migration-revision",
            "20260911_0040",
            "--i-understand-this-places-real-orders",
        ],
    )

    account = main._runtime_manifest_account_for_cli(
        manifest_path,
        account_label="account-2",
        strategy="orderflow_impulse",
    )
    expected_hash = main._runtime_manifest_strategy_config_hash(account)

    assert result.exit_code == 0
    assert captured["session_id"] == "live-account-2-v1"
    assert captured["lease_owner"] == "live-worker-account-2"
    assert captured["git_commit_hash"] == "a" * 40
    assert captured["migration_revision"] == "20260911_0040"
    assert captured["strategy_config_hash"] == expected_hash
    profile = captured["profile"]
    assert isinstance(profile, main.LiveOrderFlowImpulseProfile)
    assert profile.impulse_window_buckets == 2
    assert profile.min_notional_5m_vs_30m == Decimal("1.75")
    assert captured["entry_positive_gainer_top_count"] == 17
    assert captured["require_price_above_ema5"] is True
    assert captured["require_price_above_ema10"] is False
    assert captured["entry_policy_compare_only"] is True
    assert captured["entry_policy_enforce"] is False
    assert captured["entry_order_type"] is main.EntryType.LIMIT
    assert captured["entry_limit_ttl_seconds"] == 1200


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
        "approve-runtime",
        "refresh-approval-runtime",
        "prepare",
        "renew-lease",
        "preflight",
        "resolve-missing-order",
        "run",
        "submit-plan",
        "status",
        "disable-new-entries",
        "cancel-all-open-entries",
        "request-flatten",
        "report",
        "strategy-config-hash",
    ):
        assert command in result.stdout


@pytest.mark.parametrize(
    ("command", "confirmation"),
    [
        (
            "cancel-all-open-entries",
            "CANCEL ALL OPEN LIVE ENTRIES",
        ),
        ("request-flatten", "EMERGENCY FLATTEN LIVE ACCOUNT"),
    ],
)
def test_one_shot_risk_control_commands_require_explicit_confirmation(
    command: str,
    confirmation: str,
) -> None:
    result = runner.invoke(
        app,
        [
            command,
            "--session-id",
            "live-primary-v1",
            "--operator",
            "operator",
            "--idempotency-key",
            "command-1",
        ],
    )

    assert result.exit_code != 0
    assert confirmation in result.output


def test_renew_lease_requires_explicit_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "renew-lease",
            "--database-url",
            "postgresql+asyncpg://unused",
        ],
    )

    assert result.exit_code != 0
    assert "RENEW LIVE RISK LEASE" in result.output


def test_renew_live_lease_checks_owner_and_extends_expiration(monkeypatch) -> None:
    now = datetime.now(tz=UTC)
    lease = TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="account-2",
        strategy_name="orderflow_impulse",
        owner="live-worker-account-2",
        code_generation="test-generation",
        state=TradingLeaseState.ACTIVE,
        acquired_at=now - timedelta(minutes=5),
        expires_at=now + timedelta(minutes=5),
    )
    renewed_calls: list[tuple[str, str, datetime, str | None]] = []

    class FakeEngine:
        async def dispose(self) -> None:
            return None

    class FakeRepository:
        def __init__(self, factory) -> None:
            del factory

        async def load_active_lease(self, environment, account_label, current):
            assert environment == "live"
            assert account_label == "account-2"
            assert current.tzinfo is not None
            return lease

        async def renew_lease(
            self,
            *,
            lease_id,
            owner,
            expires_at,
            code_generation=None,
        ):
            renewed_calls.append((lease_id, owner, expires_at, code_generation))
            return TradingLease(
                lease_id=lease.lease_id,
                environment=lease.environment,
                account_label=lease.account_label,
                strategy_name=lease.strategy_name,
                owner=lease.owner,
                code_generation=code_generation or lease.code_generation,
                state=lease.state,
                acquired_at=lease.acquired_at,
                expires_at=expires_at,
            )

    monkeypatch.setattr(
        main,
        "create_execution_database_engine",
        lambda _: FakeEngine(),
    )
    monkeypatch.setattr(main, "PostgresRiskRepository", FakeRepository)

    with pytest.raises(RuntimeError, match="owner mismatch"):
        asyncio.run(
            main._renew_live_lease(
                database_url="postgresql+asyncpg://unused",
                account_label="account-2",
                strategy_name="orderflow_impulse",
                lease_owner="wrong-owner",
                lease_ttl_seconds=3600,
            )
        )
    with pytest.raises(RuntimeError, match="strategy mismatch"):
        asyncio.run(
            main._renew_live_lease(
                database_url="postgresql+asyncpg://unused",
                account_label="account-2",
                strategy_name="other_strategy",
                lease_owner="live-worker-account-2",
                lease_ttl_seconds=3600,
            )
        )

    payload = asyncio.run(
        main._renew_live_lease(
            database_url="postgresql+asyncpg://unused",
            account_label="account-2",
            strategy_name="orderflow_impulse",
            lease_owner="live-worker-account-2",
            lease_ttl_seconds=3600,
            code_generation="new-generation",
        )
    )

    assert payload["account_label"] == "account-2"
    assert payload["lease_id"] == "lease-1"
    assert renewed_calls[0][0:2] == ("lease-1", "live-worker-account-2")
    assert renewed_calls[0][2] > lease.expires_at
    assert renewed_calls[0][3] == "new-generation"


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
        ("", frozenset({"submit", "cancel"})),
        ("  ", frozenset({"submit", "cancel"})),
        ("all", None),
        (" ALL ", None),
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


def test_live_exchange_operation_option_rejects_mixed_all_mode() -> None:
    with pytest.raises(BadParameter, match="'all' only by itself"):
        main._parse_exchange_operations("submit,all")


@pytest.mark.parametrize(
    ("option_value", "expected"),
    [
        ("submit,cancel", frozenset({"submit", "cancel"})),
        ("all", None),
        (None, frozenset({"submit", "cancel"})),
    ],
)
def test_live_run_passes_exchange_operation_allowlist_to_daemon(
    monkeypatch,
    option_value: str | None,
    expected: frozenset[str] | None,
) -> None:
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
    monkeypatch.setenv("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "25")

    arguments = [
        "run",
        "--database-url",
        "postgresql+asyncpg://unused",
    ]
    if option_value is not None:
        arguments.extend(
            ["--persist-exchange-operations", option_value]
        )
    arguments.extend(
        [
            "--entry-policy-compare-only",
            "--i-understand-this-places-real-orders",
        ]
    )
    result = runner.invoke(app, arguments)

    assert result.exit_code == 0
    assert captured["persist_exchange_operations"] == expected
    assert captured["entry_policy_compare_only"] is True
    assert captured["entry_positive_gainer_top_count"] == 25


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


def test_live_run_passes_account_scoped_profile_to_daemon(monkeypatch) -> None:
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
            "--impulse-window-buckets",
            "4",
            "--confirmation-buckets",
            "1",
            "--min-return-pct",
            "0.01",
            "--min-imbalance",
            "0.40",
            "--min-intensity",
            "2",
            "--min-notional-5m-vs-30m",
            "1.50",
            "--cooldown-buckets",
            "0",
            "--i-understand-this-places-real-orders",
        ],
    )

    assert result.exit_code == 0
    profile = captured["profile"]
    assert isinstance(profile, main.LiveOrderFlowImpulseProfile)
    assert profile.impulse_window_buckets == 4
    assert profile.min_notional_5m_vs_30m == Decimal("1.50")


def test_live_run_passes_shadow_preflight_acknowledgment_to_daemon(monkeypatch) -> None:
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
            "--acknowledge-missing-shadow-preflight",
            "--i-understand-this-places-real-orders",
        ],
    )

    assert result.exit_code == 0
    assert captured["acknowledge_missing_shadow_preflight"] is True


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


@pytest.mark.parametrize(
    ("value", "length"),
    [
        ("a" * 39, 40),
        ("a" * 41, 40),
        ("a" * 63, 64),
        ("a" * 64 + "g", 64),
    ],
)
def test_operator_hash_values_require_exact_hex_lengths(
    value: str,
    length: int,
) -> None:
    with pytest.raises(BadParameter):
        main._validate_hex_hash(value, "--hash", length)


def test_runtime_strategy_config_hash_uses_live_environment(monkeypatch) -> None:
    monkeypatch.setenv("CML_LIVE_ENTRY_POLICY_MODE", "enforce")
    monkeypatch.setenv("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "10")
    monkeypatch.setenv("CML_LIVE_IMPULSE_WINDOW_BUCKETS", "2")
    monkeypatch.setenv("CML_LIVE_CONFIRMATION_BUCKETS", "1")
    monkeypatch.setenv("CML_LIVE_MIN_RETURN_PCT", "0.005")
    monkeypatch.setenv("CML_LIVE_MIN_IMBALANCE", "0.60")
    monkeypatch.setenv("CML_LIVE_MIN_INTENSITY", "2.0")
    monkeypatch.setenv("CML_LIVE_COOLDOWN_BUCKETS", "0")

    runtime_hash = main._runtime_strategy_config_hash("orderflow_impulse")

    assert runtime_hash == (
        "81d514396e45a861abcc120dc0c1ed01927690c8fe3c5debcc6e545ed8d4234f"
    )


def test_refresh_approval_runtime_preserves_existing_limits(monkeypatch) -> None:
    now = datetime.now(tz=UTC)
    current = main.LiveOperatorApproval(
        approval_id="approval-old",
        account_label="account-2",
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        risk_config_hash="b" * 64,
        git_commit_hash="c" * 40,
        database_migration_revision="20260906_0030",
        approved_notional_cap=Decimal("10000"),
        approved_max_open_positions=500,
        approved_max_daily_loss=Decimal("10000"),
        approver_name="operator",
        approval_text="ENABLE SMALL LIVE TRADING",
        expires_at=None,
        created_at=now - timedelta(minutes=1),
    )
    saved: list[main.LiveOperatorApproval] = []

    async def fake_load(*args):
        del args
        return current

    async def fake_risk_hash(*args):
        del args
        return "d" * 64

    async def fake_save(_database_url, approval):
        saved.append(approval)

    monkeypatch.setattr(main, "_load_active_approval", fake_load)
    monkeypatch.setattr(main, "_latest_risk_config_hash", fake_risk_hash)
    monkeypatch.setattr(main, "_runtime_strategy_config_hash", lambda _: "e" * 64)
    monkeypatch.setattr(main, "_save_approval", fake_save)

    result = runner.invoke(
        app,
        [
            "refresh-approval-runtime",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--account-label",
            "account-2",
            "--git-commit-hash",
            "f" * 40,
            "--migration-revision",
            "20260906_0030",
        ],
    )

    assert result.exit_code == 0
    assert len(saved) == 1
    refreshed = saved[0]
    assert refreshed.approval_id != current.approval_id
    assert refreshed.strategy_config_hash == "e" * 64
    assert refreshed.risk_config_hash == "d" * 64
    assert refreshed.git_commit_hash == "f" * 40
    assert refreshed.approved_notional_cap == current.approved_notional_cap
    assert (
        refreshed.approved_max_open_positions
        == current.approved_max_open_positions
    )
    assert refreshed.approved_max_daily_loss == current.approved_max_daily_loss
    assert refreshed.approver_name == current.approver_name
    assert refreshed.approval_text == current.approval_text


def test_strict_preflight_returns_failure_exit_code(monkeypatch) -> None:
    async def fake_summary(*args, **kwargs):
        del args, kwargs
        return {"preflight_ok": False, "preflight_errors": ["approval_present"]}

    monkeypatch.setattr(main, "_preflight_summary", fake_summary)

    result = runner.invoke(
        app,
        [
            "preflight",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--strict",
        ],
    )

    assert result.exit_code == 1
    assert "approval_present" in result.stdout


def test_preflight_passes_runtime_manifest_expectations_to_summary(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "runtime.yaml"
    manifest_path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  migration_revision: "20260911_0040"
accounts:
  - label: primary
    strategy: orderflow_impulse
    session_id: live-primary-v1
    lease_owner: live-worker
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
""",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    async def fake_summary(*args, **kwargs):
        captured.update(kwargs)
        return {"preflight_ok": True, "preflight_errors": []}

    monkeypatch.setattr(main, "_preflight_summary", fake_summary)

    result = runner.invoke(
        app,
        [
            "preflight",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--runtime-manifest",
            str(manifest_path),
        ],
    )

    assert result.exit_code == 0
    assert captured["expected_git_commit"] == "a" * 40
    assert captured["expected_migration_revision"] == "20260911_0040"
    assert captured["expected_lease_owner"] == "live-worker"
    assert isinstance(captured["expected_strategy_config_hash"], str)
    assert len(captured["expected_strategy_config_hash"]) == 64


def test_prepare_uses_runtime_manifest_identity_before_writing_risk_gates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "runtime.yaml"
    manifest_path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  migration_revision: "20260911_0040"
accounts:
  - label: primary
    strategy: orderflow_impulse
    session_id: live-primary-v1
    lease_owner: live-worker
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
""",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    async def fake_prepare(**kwargs):
        captured.update(kwargs)
        return {
            "risk_config_hash": "b" * 64,
            "strategy_config_hash": "c" * 64,
        }

    monkeypatch.setattr(main, "_prepare_live_risk_gates", fake_prepare)

    result = runner.invoke(
        app,
        [
            "prepare",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--runtime-manifest",
            str(manifest_path),
            "--confirmation",
            "PREPARE LIVE RISK GATES",
        ],
    )

    assert result.exit_code == 0
    assert captured["code_generation"] == "a" * 40
    assert captured["lease_owner"] == "live-worker"


def test_prepare_rejects_manifest_lease_owner_mismatch(tmp_path: Path) -> None:
    manifest_path = tmp_path / "runtime.yaml"
    manifest_path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  migration_revision: "20260911_0040"
accounts:
  - label: primary
    strategy: orderflow_impulse
    session_id: live-primary-v1
    lease_owner: manifest-worker
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
""",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "prepare",
            "--database-url",
            "postgresql+asyncpg://unused",
            "--runtime-manifest",
            str(manifest_path),
            "--confirmation",
            "PREPARE LIVE RISK GATES",
        ],
    )

    assert result.exit_code != 0
    assert "lease owner does not match" in result.output


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


def test_live_defaults_disable_ema_and_use_primary_orderflow_imbalance() -> None:
    assert main._LIVE_ENTRY_PRICE_ABOVE_EMA5 is False
    assert main._LIVE_ENTRY_PRICE_ABOVE_EMA10 is False
    assert main._live_strategy_config()[
        "order_flow_impulse_min_aggressive_imbalance"
    ] == Decimal("0.30")
    assert main._live_strategy_config()[
        "order_flow_impulse_min_notional_5m_vs_30m"
    ] == Decimal("1.50")


def test_strategy_config_hash_includes_account_scoped_profile() -> None:
    primary = main._live_strategy_config_hash("orderflow_impulse")
    account_two = main._live_strategy_config_hash(
        "orderflow_impulse",
        profile=main.LiveOrderFlowImpulseProfile(impulse_window_buckets=3),
    )

    assert primary != account_two


def test_preflight_runtime_strategy_config_reads_live_lane_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "10")
    monkeypatch.setenv("CML_LIVE_ENTRY_POLICY_MODE", "enforce")
    monkeypatch.setenv("CML_LIVE_IMPULSE_WINDOW_BUCKETS", "4")

    config = main._preflight_runtime_strategy_config()

    assert config.profile.impulse_window_buckets == 4
    assert config.entry_positive_gainer_top_count == 10
    assert config.entry_policy_mode == "enforce"
    assert config.entry_policy_enforce is True


def test_preflight_runtime_strategy_config_rejects_unknown_policy_mode(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CML_LIVE_ENTRY_POLICY_MODE", "unexpected")

    with pytest.raises(ValueError, match="CML_LIVE_ENTRY_POLICY_MODE"):
        main._preflight_runtime_strategy_config()


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
    assert live_startup_retry_delay(1, retry_after_seconds=17) == 17
    assert live_startup_retry_delay(2, retry_after_seconds=None) == 30
    assert live_startup_retry_delay(10, retry_after_seconds=None) == 300


def test_only_transient_live_startup_errors_are_retryable() -> None:
    assert is_retryable_live_startup_error(
        RuntimeError("live gate blocked: missing_active_lease")
    )
    assert is_retryable_live_startup_error(TimeoutError("recovery timed out"))
    assert not is_retryable_live_startup_error(
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

    await restore_live_strategy_from_checkpoint(
        strategy=Strategy(),
        checkpoint=checkpoint,
        repository=Repository(),
        environment="research",
    )

    assert len(warmed) == 1
    assert seen["environment"] == "research"
    assert seen["last_processed_at_by_symbol"] == {"BTCUSDT": now}


@pytest.mark.asyncio
async def test_periodic_reconcile_runs_outside_market_state_loop() -> None:
    calls = 0
    delays: list[float] = []

    class Repository:
        async def load_unresolved_orders(self, run_id: str):
            nonlocal calls
            calls += 1
            return ()

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 2:
            raise asyncio.CancelledError

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=object(),  # type: ignore[arg-type]
        run_id="live-manual",
        interval_seconds=60,
    )

    with pytest.raises(asyncio.CancelledError):
        await reconciliation.run_periodically(sleep=controlled_sleep)

    assert calls == 1
    assert delays == [60, 60]


def test_live_lease_auto_reacquire_requires_prior_live_session() -> None:
    assert should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=False,
        gate_reasons=("missing_active_lease",),
    )
    assert not should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=False,
        draining=False,
        gate_reasons=("missing_active_lease",),
    )
    assert not should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=True,
        gate_reasons=("missing_active_lease",),
    )
    assert not should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=False,
        gate_reasons=("missing_active_lease", "active_risk_halt"),
    )


async def test_live_warmup_applies_all_states_and_continues_from_boundary() -> None:
    now = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    stale = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=now - timedelta(seconds=30),
        bucket_end=now - timedelta(seconds=15),
    )
    fresh = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=now - timedelta(seconds=15),
        bucket_end=now,
    )

    class Strategy:
        def __init__(self) -> None:
            self.seen = []

        def required_data(self):
            return SimpleNamespace(
                warmup_buckets=1,
                base_state_interval_seconds=15,
                required_fields=(),
            )

        def warm_market_state(self, state):
            self.seen.append(state)

    class Repository:
        async def load_after(self, **kwargs):
            assert kwargs["environment"] == "research"
            return (stale, fresh)

    strategy = Strategy()
    cursor = await warm_live_strategy(
        strategy=strategy,
        repository=Repository(),
        environment="research",
        now=now,
    )

    assert strategy.seen == [stale, fresh]
    assert cursor.bucket_start == fresh.bucket_start
    assert cursor.symbol == "BTCUSDT"


@pytest.mark.asyncio
async def test_live_warmup_defers_symbols_without_a_complete_window() -> None:
    now = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    states = tuple(
        SimpleNamespace(
            symbol=symbol,
            bucket_start=now - timedelta(seconds=offset),
            bucket_end=now - timedelta(seconds=offset - 15),
        )
        for symbol, offset in (
            ("BTCUSDT", 45),
            ("BTCUSDT", 30),
            ("BTCUSDT", 15),
            ("NEWUSDT", 15),
        )
    )

    class Strategy:
        def required_data(self):
            return SimpleNamespace(
                warmup_buckets=3,
                base_state_interval_seconds=15,
                required_fields=(),
            )

        def warm_market_state(self, state):
            return None

    class Repository:
        async def load_symbols_at(self, **kwargs):
            assert kwargs["environment"] == "research"
            return frozenset({"BTCUSDT", "NEWUSDT"})

        async def load_after(self, **kwargs):
            return states

    cursor = await warm_live_strategy(
        strategy=Strategy(),
        repository=Repository(),
        environment="research",
        now=now,
        cutover_at=now,
    )

    assert cursor.bucket_start == states[-1].bucket_start
    assert cursor.symbol == states[-1].symbol


@pytest.mark.asyncio
async def test_live_checkpoint_recovery_scales_limit_to_symbol_universe() -> None:
    now = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    symbols = {f"S{index:05d}USDT" for index in range(718)}
    seen: dict[str, object] = {}

    class Strategy:
        def required_data(self):
            return SimpleNamespace(
                warmup_buckets=140,
                base_state_interval_seconds=15,
                required_fields=(),
            )

        def warm_market_state(self, state):
            return None

        def checkpoint(self, *, include_market_state_buffers=True):
            return StrategyCheckpoint(
                last_processed_at_by_symbol={"S00000USDT": now},
                warmup_buckets_by_symbol={"S00000USDT": 140},
                cooldown_buckets_remaining_by_symbol={"S00000USDT": 0},
                payload={},
            )

    class Repository:
        async def load_symbols_at(self, **kwargs):
            return frozenset(symbols)

        async def load_recovery_window(self, **kwargs):
            seen.update(kwargs)
            return ()

    with pytest.raises(RuntimeError, match="no symbol has a complete window"):
        await restore_live_strategy_from_checkpoint(
            strategy=Strategy(),
            checkpoint=StrategyCheckpoint(
                last_processed_at_by_symbol={"S00000USDT": now},
                warmup_buckets_by_symbol={"S00000USDT": 140},
                cooldown_buckets_remaining_by_symbol={"S00000USDT": 0},
                payload={"signal_sequence": 4},
            ),
            repository=Repository(),
            environment="research",
        )

    assert seen["limit"] == 718 * 158


def test_live_warmup_rejects_a_symbol_with_a_window_gap() -> None:
    start = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)
    states = tuple(
        SimpleNamespace(
            symbol="BTCUSDT",
            bucket_start=start + timedelta(seconds=offset),
            close_price=Decimal("1"),
        )
        for offset in (0, 15, 45)
    )

    class Strategy:
        def required_data(self):
            return SimpleNamespace(
                warmup_buckets=3,
                base_state_interval_seconds=15,
                required_fields=("close_price",),
            )

    with pytest.raises(RuntimeError, match="gaps=BTCUSDT"):
        validate_live_warmup_coverage(
            strategy=Strategy(),
            states=states,
            expected_symbols=("BTCUSDT",),
            cutover_at=states[-1].bucket_start,
        )


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

    async for item in resilient_market_state_stream(
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
                    raise AccountEventHubError("account-event hub unavailable")
                yield event

            return stream()

    source = Source()
    observed = []

    async for item in resilient_account_event_stream(
        source,
        retry_delay_seconds=0,
    ):
        observed.append(item)

    assert observed == [event]
    assert source.attempts == 2


@pytest.mark.asyncio
async def test_account_event_reconciles_order_before_publishing_snapshot() -> None:
    event = SimpleNamespace(
        event_type="ORDER_TRADE_UPDATE",
        client_order_id="entry-1",
        has_fill=False,
        symbols=("BTCUSDT",),
    )
    ordering: list[str] = []

    class Reconciliation:
        run_id = "run-1"

        async def reconcile_account_event(self, _event) -> None:
            ordering.append("reconcile")

    def publish_snapshot(_event) -> None:
        ordering.append("snapshot")

    class Source:
        def __aiter__(self):
            async def stream():
                yield event

            return stream()

    latest_market_states = SimpleNamespace(for_symbols=lambda _symbols: ())
    await main._run_account_event_channel(
        source=Source(),
        daemon=None,
        latest_market_states=latest_market_states,
        latest_market_quotes=None,
        order_reconciliation=Reconciliation(),
        on_account_snapshot=publish_snapshot,
    )

    assert ordering == ["reconcile", "snapshot"]


@pytest.mark.asyncio
async def test_account_event_retries_pending_position_sync(
    monkeypatch,
) -> None:
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=False,
        symbols=("BTCUSDT",),
    )
    state = SimpleNamespace(symbol="BTCUSDT")
    delays: list[float] = []
    failures: list[tuple[str, str | None]] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(main.asyncio, "sleep", sleep)

    class Daemon:
        def __init__(self) -> None:
            self.calls = 0

        async def process_account_event(self, _state, *, quote):
            del quote
            self.calls += 1
            if self.calls == 1:
                return "pending_live_positions:BTCUSDT"
            return None

    class Source:
        def __aiter__(self):
            async def stream():
                yield event

            return stream()

    daemon = Daemon()
    await main._run_account_event_channel(
        source=Source(),
        daemon=daemon,
        latest_market_states=SimpleNamespace(
            for_symbols=lambda _symbols: (state,)
        ),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda _symbols: ()),
        order_repository=None,
        state_machine=None,
        run_id="run-1",
        on_exit_failure=lambda symbol, failure: failures.append(
            (symbol, failure)
        ),
    )

    assert daemon.calls == 2
    assert delays == [0.25]
    assert failures == [("BTCUSDT", None)]


@pytest.mark.asyncio
async def test_account_event_does_not_retry_confirmed_unmanaged_position(
    monkeypatch,
) -> None:
    event = SimpleNamespace(
        event_type="ACCOUNT_UPDATE",
        client_order_id=None,
        has_fill=False,
        symbols=("BTCUSDT",),
    )
    state = SimpleNamespace(symbol="BTCUSDT")
    sleep_calls: list[float] = []
    failures: list[tuple[str, str | None]] = []

    async def sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(main.asyncio, "sleep", sleep)

    class Daemon:
        managed_position_symbols = frozenset()

        async def process_account_event(self, _state, *, quote):
            del quote
            return "unmanaged_live_positions:BTCUSDT"

    class Source:
        def __aiter__(self):
            async def stream():
                yield event

            return stream()

    await main._run_account_event_channel(
        source=Source(),
        daemon=Daemon(),
        latest_market_states=SimpleNamespace(
            for_symbols=lambda _symbols: (state,)
        ),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda _symbols: ()),
        order_repository=None,
        state_machine=None,
        run_id="run-1",
        on_exit_failure=lambda symbol, failure: failures.append(
            (symbol, failure)
        ),
    )

    assert sleep_calls == []
    assert failures == [("BTCUSDT", "unmanaged_live_positions:BTCUSDT")]


@pytest.mark.asyncio
async def test_grace_timeout_channel_degrades_on_order_identity_conflict(
    monkeypatch,
) -> None:
    state = SimpleNamespace(symbol="BTCUSDT")
    failures: list[tuple[str, str | None]] = []

    async def stop_after_first_cycle(_delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "sleep", stop_after_first_cycle)

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_grace_timeout(self, _state, *, now, latest_quote):
            del now, latest_quote
            raise ValueError(
                "client order ID is already bound to a different order"
            )

    with pytest.raises(asyncio.CancelledError):
        await main._run_grace_timeout_channel(
            daemon=Daemon(),
            latest_market_states=SimpleNamespace(
                for_symbols=lambda _symbols: (state,),
            ),
            latest_market_quotes=SimpleNamespace(
                for_symbols=lambda _symbols: (),
            ),
            on_exit_failure=lambda symbol, failure: failures.append(
                (symbol, failure)
            ),
        )

    assert failures == [("BTCUSDT", "order_identity_conflict")]


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


@pytest.mark.asyncio
async def test_acknowledged_missing_shadow_preflight_logs_info(
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

    events = []

    class FakeLogger:
        def info(self, event, **kwargs):
            events.append(("info", event, kwargs))

        def warning(self, event, **kwargs):
            events.append(("warning", event, kwargs))

    monkeypatch.setattr(main, "log", FakeLogger())

    await main._warn_if_shadow_preflight_missing(
        FakeFactory(),
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        account_label="primary",
        session_id="live-1",
        acknowledged=True,
    )

    assert events == [
        (
            "info",
            "live_shadow_preflight_missing_acknowledged",
            {
                "account_label": "primary",
                "session_id": "live-1",
                "strategy_name": "orderflow_impulse",
                "strategy_config_hash": "a" * 64,
            },
        )
    ]
