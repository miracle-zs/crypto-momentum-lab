"""Resolve live CLI options into one immutable runtime configuration.

The Typer command is the process boundary, not the owner of live-runtime
configuration semantics.  This module keeps precedence, manifest conflict
checks, and environment fallbacks together so they can be tested without
constructing the CLI or a live daemon.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from crypto_momentum_lab.config import ResolvedBinanceCredentials
from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.live_rollout.runtime_config import (
    _DEFAULT_PERSIST_EXCHANGE_OPERATIONS,
    _LIVE_ENTRY_LIMIT_TTL_SECONDS,
    _LIVE_ENTRY_ORDER_TYPE,
    _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    LiveRuntimeConfig,
    LiveRuntimeCredentials,
    LiveRuntimeDatabases,
    LiveRuntimeExecution,
    LiveRuntimeIdentity,
    LiveRuntimeLifecycle,
    LiveRuntimeMarket,
    LiveRuntimeStrategy,
    _live_strategy_config_hash,
)
from crypto_momentum_lab.live_rollout.runtime_manifest import (
    LiveRuntimeAccount,
    RuntimeManifestError,
    load_live_runtime_manifest,
)
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode


class LiveRuntimeOptionsError(ValueError):
    """Raised when CLI/environment values cannot form a safe live runtime."""


_GIT_COMMIT_HASH_LENGTH = 40
_CONFIG_HASH_LENGTH = 64
_HEX_HASH_PATTERN = re.compile(r"^[0-9a-f]+$")


@dataclass(frozen=True, slots=True)
class LiveRunOptions:
    """Raw options accepted by the long-running ``live run`` command."""

    database_url: str | None
    account_label: str
    strategy: str
    runtime_manifest: Path | None
    impulse_window_buckets: int | None
    confirmation_buckets: int | None
    min_return_pct: str | None
    min_imbalance: str | None
    min_intensity: str | None
    min_notional_5m_vs_30m: str | None
    cooldown_buckets: int | None
    market_environment: str
    market_state_source: str
    market_state_hub_url: str
    market_quote_hub_url: str
    market_quote_volume_hub_url: str
    market_websocket_url: str
    account_event_hub_url: str
    risk_control_hub_url: str | None
    session_id: str | None
    operator: str
    lease_owner: str | None
    strategy_config_hash: str
    git_commit_hash: str
    migration_revision: str
    max_runtime_seconds: int
    poll_interval_seconds: float
    checkpoint_every_states: int
    hedge_mode: bool | None
    exit_mode: PositionExitMode | None
    take_profit_pct: str | None
    stop_loss_pct: str | None
    entry_long_only: bool | None
    entry_leverage: int | None
    margin_type: str | None
    entry_positive_gainer_top_count: int | None
    entry_price_above_ema5: bool | None
    entry_price_above_ema10: bool | None
    entry_order_type: EntryType | None
    entry_limit_ttl_seconds: int | None
    candle_grace_bars: int | None
    candle_grace_decision_profit_pct: str | None
    candle_grace_profit_pct: str | None
    base_url: str
    entry_policy_compare_only: bool | None
    entry_policy_enforce: bool | None
    acknowledge_missing_shadow_preflight: bool
    persist_exchange_operations: str | None


def runtime_manifest_account_for_cli(
    path: Path,
    *,
    account_label: str,
    strategy: str,
    environment: Mapping[str, str] | None = None,
) -> LiveRuntimeAccount:
    """Load one manifest account and enforce the CLI strategy identity."""

    try:
        manifest = load_live_runtime_manifest(path, environment=environment)
        account = manifest.account(account_label)
    except RuntimeManifestError as error:
        raise LiveRuntimeOptionsError(str(error)) from error
    if strategy != account.strategy:
        raise LiveRuntimeOptionsError(
            "--strategy does not match the runtime manifest account"
        )
    return account


def runtime_manifest_strategy_config_hash(account: LiveRuntimeAccount) -> str:
    """Validate and return the hash derived from a manifest account."""

    inputs = account.strategy_inputs
    try:
        computed = _live_strategy_config_hash(
            account.strategy,
            profile=inputs.profile,
            entry_positive_gainer_top_count=(
                inputs.entry_positive_gainer_top_count
            ),
            require_price_above_ema5=inputs.require_price_above_ema5,
            require_price_above_ema10=inputs.require_price_above_ema10,
            entry_policy_enforce=inputs.entry_policy_enforce,
            entry_order_type=inputs.entry_order_type,
            entry_limit_ttl_seconds=inputs.entry_limit_ttl_seconds,
        )
    except (TypeError, ValueError) as error:
        raise LiveRuntimeOptionsError(str(error)) from error
    if account.strategy_config_hash != "unset":
        configured = _validate_hex_hash(
            account.strategy_config_hash,
            "runtime manifest strategy_config_hash",
            _CONFIG_HASH_LENGTH,
        )
        if configured != computed:
            raise LiveRuntimeOptionsError(
                "runtime manifest strategy_config_hash does not match "
                "its strategy_config inputs"
            )
    return computed


def resolve_manifest_option[T](
    value: T | None,
    expected: T,
    option_name: str,
) -> T:
    """Use a manifest value while rejecting an explicit conflict."""

    if value is not None and value != expected:
        raise LiveRuntimeOptionsError(
            f"{option_name} does not match the runtime manifest"
        )
    return expected


def resolve_manifest_decimal_option(
    value: str | None,
    expected: Decimal,
    option_name: str,
) -> str:
    if value is None:
        return str(expected)
    try:
        configured = Decimal(value)
    except InvalidOperation as error:
        raise LiveRuntimeOptionsError(f"{option_name} must be a decimal") from error
    if configured != expected:
        raise LiveRuntimeOptionsError(
            f"{option_name} does not match the runtime manifest"
        )
    return str(expected)


def resolve_manifest_operations(
    value: str | None,
    expected: str,
    option_name: str,
) -> str:
    if value is None:
        return expected
    configured = parse_exchange_operations(value)
    expected_operations = (
        None
        if expected.strip().lower() == "all"
        else frozenset(item.strip() for item in expected.split(",") if item.strip())
    )
    if configured != expected_operations:
        raise LiveRuntimeOptionsError(
            f"{option_name} does not match the runtime manifest"
        )
    return expected


def resolve_live_entry_positive_gainer_top_count(
    value: int | None,
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    if value is not None:
        return value
    values = os.environ if environment is None else environment
    raw = values.get("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "").strip()
    if not raw:
        return _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT
    try:
        resolved = int(raw)
    except ValueError as error:
        raise LiveRuntimeOptionsError(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be an integer"
        ) from error
    if resolved <= 0:
        raise LiveRuntimeOptionsError(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be positive"
        )
    return resolved


def resolve_live_profile_options(
    *,
    impulse_window_buckets: int | None,
    confirmation_buckets: int | None,
    min_return_pct: str | None,
    min_imbalance: str | None,
    min_intensity: str | None,
    min_notional_5m_vs_30m: str | None,
    cooldown_buckets: int | None,
    environment: Mapping[str, str] | None = None,
) -> LiveOrderFlowImpulseProfile:
    """Resolve a complete profile or the configured environment profile."""

    values = (
        impulse_window_buckets,
        confirmation_buckets,
        min_return_pct,
        min_imbalance,
        min_intensity,
        min_notional_5m_vs_30m,
        cooldown_buckets,
    )
    if not any(value is not None for value in values):
        try:
            return LiveOrderFlowImpulseProfile.from_environment(environment)
        except ValueError as error:
            raise LiveRuntimeOptionsError(str(error)) from error
    legacy_values = values[:5] + values[6:]
    if min_notional_5m_vs_30m is None and all(
        value is not None for value in legacy_values
    ):
        # Preserve the six-option CLI form; the seventh dimension is opt-in.
        min_notional_5m_vs_30m = "0"
    if not all(value is not None for value in values):
        raise LiveRuntimeOptionsError(
            "all seven order-flow profile options must be provided together"
        )
    assert impulse_window_buckets is not None
    assert confirmation_buckets is not None
    assert min_return_pct is not None
    assert min_imbalance is not None
    assert min_intensity is not None
    assert min_notional_5m_vs_30m is not None
    assert cooldown_buckets is not None
    try:
        return LiveOrderFlowImpulseProfile(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=Decimal(min_return_pct),
            min_aggressive_imbalance=Decimal(min_imbalance),
            min_notional_intensity=Decimal(min_intensity),
            min_notional_5m_vs_30m=Decimal(min_notional_5m_vs_30m),
            cooldown_buckets=cooldown_buckets,
        )
    except (InvalidOperation, ValueError) as error:
        raise LiveRuntimeOptionsError(
            f"invalid live order-flow profile: {error}"
        ) from error


def parse_exchange_operations(raw_value: str) -> frozenset[str] | None:
    """Parse the durable exchange telemetry allow-list."""

    normalized_value = raw_value.strip()
    if not normalized_value:
        return _DEFAULT_PERSIST_EXCHANGE_OPERATIONS
    if normalized_value.lower() == "all":
        return None
    operations = tuple(operation.strip() for operation in raw_value.split(","))
    if any(not operation for operation in operations):
        raise LiveRuntimeOptionsError(
            "--persist-exchange-operations must be a comma-separated list "
            "of non-empty operation names"
        )
    if any(operation.lower() == "all" for operation in operations):
        raise LiveRuntimeOptionsError(
            "--persist-exchange-operations accepts 'all' only by itself"
        )
    return frozenset(operations)


def resolve_live_runtime_config(
    options: LiveRunOptions,
    *,
    credentials: ResolvedBinanceCredentials,
    environment: Mapping[str, str] | None = None,
) -> LiveRuntimeConfig:
    """Resolve CLI, manifest, and environment values into daemon inputs."""

    values = os.environ if environment is None else environment
    manifest_account = (
        None
        if options.runtime_manifest is None
        else runtime_manifest_account_for_cli(
            options.runtime_manifest,
            account_label=options.account_label,
            strategy=options.strategy,
            environment=values,
        )
    )

    session_id = options.session_id
    lease_owner = options.lease_owner
    strategy = options.strategy
    profile: LiveOrderFlowImpulseProfile
    entry_positive_gainer_top_count: int | None
    entry_price_above_ema5: bool
    entry_price_above_ema10: bool
    entry_order_type = options.entry_order_type
    entry_limit_ttl_seconds = options.entry_limit_ttl_seconds
    entry_policy_compare_only = options.entry_policy_compare_only
    entry_policy_enforce = options.entry_policy_enforce
    hedge_mode = options.hedge_mode
    exit_mode = options.exit_mode
    take_profit_pct = options.take_profit_pct
    stop_loss_pct = options.stop_loss_pct
    entry_long_only = options.entry_long_only
    entry_leverage: int | None = None
    margin_type: str | None = None
    candle_grace_bars = options.candle_grace_bars
    candle_grace_decision_profit_pct = options.candle_grace_decision_profit_pct
    candle_grace_profit_pct = options.candle_grace_profit_pct
    persist_exchange_operations = options.persist_exchange_operations
    strategy_config_hash = options.strategy_config_hash
    git_commit_hash = options.git_commit_hash
    migration_revision = options.migration_revision

    if manifest_account is None:
        session_id = session_id or "live-manual"
        lease_owner = lease_owner or "live-worker"
        hedge_mode = True if hedge_mode is None else hedge_mode
        entry_long_only = True if entry_long_only is None else entry_long_only
        entry_price_above_ema5 = (
            _LIVE_ENTRY_PRICE_ABOVE_EMA5
            if options.entry_price_above_ema5 is None
            else options.entry_price_above_ema5
        )
        entry_price_above_ema10 = (
            _LIVE_ENTRY_PRICE_ABOVE_EMA10
            if options.entry_price_above_ema10 is None
            else options.entry_price_above_ema10
        )
        entry_order_type = (
            _LIVE_ENTRY_ORDER_TYPE
            if entry_order_type is None
            else entry_order_type
        )
        entry_limit_ttl_seconds = (
            _LIVE_ENTRY_LIMIT_TTL_SECONDS
            if entry_limit_ttl_seconds is None
            else entry_limit_ttl_seconds
        )
        entry_policy_compare_only = (
            False
            if entry_policy_compare_only is None
            else entry_policy_compare_only
        )
        entry_policy_enforce = (
            False if entry_policy_enforce is None else entry_policy_enforce
        )
        entry_leverage = 1 if options.entry_leverage is None else options.entry_leverage
        margin_type = "CROSSED" if options.margin_type is None else options.margin_type
        exit_mode = PositionExitMode.CANDLE_15M if exit_mode is None else exit_mode
        take_profit_pct = "0.02" if take_profit_pct is None else take_profit_pct
        stop_loss_pct = "0.01" if stop_loss_pct is None else stop_loss_pct
        candle_grace_bars = 1 if candle_grace_bars is None else candle_grace_bars
        candle_grace_decision_profit_pct = (
            "0.001"
            if candle_grace_decision_profit_pct is None
            else candle_grace_decision_profit_pct
        )
        candle_grace_profit_pct = (
            "0.0088" if candle_grace_profit_pct is None else candle_grace_profit_pct
        )
        persist_exchange_operations = (
            "submit,cancel"
            if persist_exchange_operations is None
            else persist_exchange_operations
        )
        profile = resolve_live_profile_options(
            impulse_window_buckets=options.impulse_window_buckets,
            confirmation_buckets=options.confirmation_buckets,
            min_return_pct=options.min_return_pct,
            min_imbalance=options.min_imbalance,
            min_intensity=options.min_intensity,
            min_notional_5m_vs_30m=options.min_notional_5m_vs_30m,
            cooldown_buckets=options.cooldown_buckets,
            environment=values,
        )
        entry_positive_gainer_top_count = resolve_live_entry_positive_gainer_top_count(
            options.entry_positive_gainer_top_count,
            environment=values,
        )
    else:
        if session_id is not None and session_id != manifest_account.session_id:
            raise LiveRuntimeOptionsError(
                "session id does not match the runtime manifest"
            )
        if lease_owner is not None and lease_owner != manifest_account.lease_owner:
            raise LiveRuntimeOptionsError(
                "lease owner does not match the runtime manifest"
            )
        session_id = manifest_account.session_id
        lease_owner = manifest_account.lease_owner
        strategy_inputs = manifest_account.strategy_inputs
        profile = strategy_inputs.profile
        entry_positive_gainer_top_count = (
            strategy_inputs.entry_positive_gainer_top_count
        )
        entry_price_above_ema5 = strategy_inputs.require_price_above_ema5
        entry_price_above_ema10 = strategy_inputs.require_price_above_ema10
        entry_policy_compare_only = strategy_inputs.entry_policy_mode == "compare_only"
        entry_policy_enforce = strategy_inputs.entry_policy_enforce
        entry_order_type = strategy_inputs.entry_order_type
        entry_limit_ttl_seconds = strategy_inputs.entry_limit_ttl_seconds
        execution_inputs = manifest_account.execution_inputs
        hedge_mode = resolve_manifest_option(
            hedge_mode,
            execution_inputs.hedge_mode,
            "--hedge-mode",
        )
        entry_long_only = resolve_manifest_option(
            entry_long_only,
            execution_inputs.entry_long_only,
            "--entry-long-only",
        )
        entry_leverage = resolve_manifest_option(
            options.entry_leverage,
            execution_inputs.entry_leverage,
            "--entry-leverage",
        )
        configured_margin_type = (
            None if options.margin_type is None else options.margin_type.strip().upper()
        )
        margin_type = resolve_manifest_option(
            configured_margin_type,
            execution_inputs.margin_type,
            "--margin-type",
        )
        exit_mode = resolve_manifest_option(
            exit_mode,
            execution_inputs.exit_mode,
            "--exit-mode",
        )
        take_profit_pct = resolve_manifest_decimal_option(
            take_profit_pct,
            execution_inputs.take_profit_pct,
            "--take-profit-pct",
        )
        stop_loss_pct = resolve_manifest_decimal_option(
            stop_loss_pct,
            execution_inputs.stop_loss_pct,
            "--stop-loss-pct",
        )
        candle_grace_bars = resolve_manifest_option(
            candle_grace_bars,
            execution_inputs.candle_grace_bars,
            "--candle-grace-bars",
        )
        candle_grace_decision_profit_pct = resolve_manifest_decimal_option(
            candle_grace_decision_profit_pct,
            execution_inputs.candle_grace_decision_profit_pct,
            "--candle-grace-decision-profit-pct",
        )
        candle_grace_profit_pct = resolve_manifest_decimal_option(
            candle_grace_profit_pct,
            execution_inputs.candle_grace_profit_pct,
            "--candle-grace-profit-pct",
        )
        persist_exchange_operations = resolve_manifest_operations(
            persist_exchange_operations,
            execution_inputs.persist_exchange_operations,
            "--persist-exchange-operations",
        )

        configured_git_commit = options.git_commit_hash.strip() or values.get(
            "CML_CODE_COMMIT",
            "",
        ).strip()
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if (
            configured_git_commit
            and configured_git_commit.lower() != manifest_git_commit
        ):
            raise LiveRuntimeOptionsError(
                "git commit does not match the runtime manifest"
            )
        git_commit_hash = manifest_git_commit

        configured_migration_revision = (
            options.migration_revision.strip()
            or values.get("CML_LIVE_MIGRATION_REVISION", "").strip()
        )
        if (
            configured_migration_revision
            and configured_migration_revision != manifest_account.migration_revision
        ):
            raise LiveRuntimeOptionsError(
                "migration revision does not match the runtime manifest"
            )
        migration_revision = manifest_account.migration_revision

        manifest_strategy_hash = runtime_manifest_strategy_config_hash(manifest_account)
        configured_strategy_hash = options.strategy_config_hash.strip().lower()
        if configured_strategy_hash not in {"", "unset"}:
            configured_strategy_hash = _validate_hex_hash(
                configured_strategy_hash,
                "--strategy-config-hash",
                _CONFIG_HASH_LENGTH,
            )
            if configured_strategy_hash != manifest_strategy_hash:
                raise LiveRuntimeOptionsError(
                    "strategy config hash does not match the runtime manifest"
                )
        strategy_config_hash = manifest_strategy_hash

    if (
        session_id is None
        or lease_owner is None
        or profile is None
        or entry_order_type is None
        or entry_limit_ttl_seconds is None
        or entry_policy_compare_only is None
        or entry_policy_enforce is None
        or hedge_mode is None
        or exit_mode is None
        or take_profit_pct is None
        or stop_loss_pct is None
        or entry_long_only is None
        or entry_leverage is None
        or margin_type is None
        or candle_grace_bars is None
        or candle_grace_decision_profit_pct is None
        or candle_grace_profit_pct is None
        or persist_exchange_operations is None
    ):
        raise LiveRuntimeOptionsError("live runtime options are incomplete")
    try:
        take_profit = Decimal(take_profit_pct)
        stop_loss = Decimal(stop_loss_pct)
        candle_decision_profit = Decimal(candle_grace_decision_profit_pct)
        candle_profit = Decimal(candle_grace_profit_pct)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise LiveRuntimeOptionsError(
            f"live execution decimal option is invalid: {error}"
        ) from error

    return LiveRuntimeConfig(
        databases=LiveRuntimeDatabases(
            execution_database_url=_resolve_database_url(
                options.database_url,
                "CML_EXECUTION_DATABASE_URL",
                values,
            ),
            market_database_url=_resolve_database_url(
                options.database_url,
                "CML_MARKET_DATABASE_URL",
                values,
            ),
            observability_database_url=_resolve_database_url(
                options.database_url,
                "CML_OBSERVABILITY_DATABASE_URL",
                values,
            ),
        ),
        identity=LiveRuntimeIdentity(
            account_label=options.account_label,
            strategy_name=strategy,
            session_id=session_id,
            operator=options.operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
        ),
        market=LiveRuntimeMarket(
            market_environment=options.market_environment,
            market_state_source=options.market_state_source,
            market_state_hub_url=options.market_state_hub_url,
            market_quote_hub_url=options.market_quote_hub_url,
            market_quote_volume_hub_url=options.market_quote_volume_hub_url,
            market_websocket_url=options.market_websocket_url,
            account_event_hub_url=options.account_event_hub_url,
            risk_control_hub_url=options.risk_control_hub_url,
        ),
        strategy=LiveRuntimeStrategy(
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
            entry_policy_compare_only=entry_policy_compare_only,
            entry_policy_enforce=entry_policy_enforce,
        ),
        execution=LiveRuntimeExecution(
            hedge_mode=hedge_mode,
            exit_mode=exit_mode,
            take_profit_pct=take_profit,
            stop_loss_pct=stop_loss,
            entry_long_only=entry_long_only,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
            candle_grace_bars=candle_grace_bars,
            candle_grace_decision_profit_pct=candle_decision_profit,
            candle_grace_profit_pct=candle_profit,
        ),
        lifecycle=LiveRuntimeLifecycle(
            max_runtime_seconds=options.max_runtime_seconds,
            poll_interval_seconds=options.poll_interval_seconds,
            checkpoint_every_states=options.checkpoint_every_states,
            persist_exchange_operations=parse_exchange_operations(
                persist_exchange_operations
            ),
            acknowledge_missing_shadow_preflight=(
                options.acknowledge_missing_shadow_preflight
            ),
        ),
        credentials=LiveRuntimeCredentials(
            base_url=options.base_url,
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
        ),
    )


def _validate_hex_hash(
    raw_value: str,
    option_name: str,
    expected_length: int,
) -> str:
    value = raw_value.strip().lower()
    if len(value) != expected_length or _HEX_HASH_PATTERN.fullmatch(value) is None:
        raise LiveRuntimeOptionsError(
            f"{option_name} must be exactly {expected_length} lowercase hex characters"
        )
    return value


def _resolve_database_url(
    value: str | None,
    plane_env_var: str,
    environment: Mapping[str, str],
) -> str:
    resolved = (value or "").strip() or environment.get(plane_env_var, "").strip()
    if not resolved:
        resolved = environment.get("CML_DATABASE_URL", "").strip()
    if not resolved:
        raise LiveRuntimeOptionsError(
            f"--database-url or {plane_env_var} or CML_DATABASE_URL is required"
        )
    return resolved


__all__ = [
    "LiveRunOptions",
    "LiveRuntimeOptionsError",
    "parse_exchange_operations",
    "resolve_live_entry_positive_gainer_top_count",
    "resolve_live_profile_options",
    "resolve_live_runtime_config",
    "resolve_manifest_decimal_option",
    "resolve_manifest_operations",
    "resolve_manifest_option",
    "runtime_manifest_account_for_cli",
    "runtime_manifest_strategy_config_hash",
]
