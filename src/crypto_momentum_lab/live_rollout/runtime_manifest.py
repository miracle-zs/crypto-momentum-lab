"""Load and validate the desired live-runtime manifest."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode


class RuntimeManifestError(ValueError):
    """Raised when a desired runtime manifest is missing or malformed."""


@dataclass(frozen=True, slots=True)
class LiveRuntimeAccount:
    """Desired identity and deployment references for one live account."""

    label: str
    strategy: str
    session_id: str
    lease_owner: str
    image_commit: str
    migration_revision: str
    strategy_config_hash: str
    profile_ref: str
    limits_ref: str
    services: tuple[str, ...]
    strategy_inputs: LiveRuntimeStrategyInputs
    execution_inputs: LiveRuntimeExecutionInputs


@dataclass(frozen=True, slots=True)
class LiveRuntimeStrategyInputs:
    """Deterministic strategy-hash inputs owned by the runtime manifest."""

    profile: LiveOrderFlowImpulseProfile
    entry_positive_gainer_top_count: int
    require_price_above_ema5: bool
    require_price_above_ema10: bool
    entry_policy_mode: str
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int

    @property
    def entry_policy_enforce(self) -> bool:
        return self.entry_policy_mode == "enforce"


@dataclass(frozen=True, slots=True)
class LiveRuntimeExecutionInputs:
    """Account-scoped order and exit settings owned by the manifest."""

    hedge_mode: bool
    entry_long_only: bool
    entry_leverage: int
    margin_type: str
    exit_mode: PositionExitMode
    take_profit_pct: Decimal
    stop_loss_pct: Decimal
    candle_grace_bars: int
    candle_grace_decision_profit_pct: Decimal
    candle_grace_profit_pct: Decimal
    persist_exchange_operations: str


@dataclass(frozen=True, slots=True)
class LiveRuntimeManifest:
    """Validated desired runtime state with account lookup by label."""

    schema_version: int
    accounts: tuple[LiveRuntimeAccount, ...]

    def account(self, label: str) -> LiveRuntimeAccount:
        normalized = label.strip()
        for account in self.accounts:
            if account.label == normalized:
                return account
        raise RuntimeManifestError(
            f"runtime manifest has no account labeled {normalized!r}"
        )


_ENV_REFERENCE = re.compile(
    r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:(?P<operator>:-|:\?)(?P<argument>[^}]*))?\}"
)

_DEFAULT_EXECUTION_INPUTS = LiveRuntimeExecutionInputs(
    hedge_mode=True,
    entry_long_only=True,
    entry_leverage=1,
    margin_type="CROSSED",
    exit_mode=PositionExitMode.CANDLE_15M,
    take_profit_pct=Decimal("0.02"),
    stop_loss_pct=Decimal("0.01"),
    candle_grace_bars=1,
    candle_grace_decision_profit_pct=Decimal("0.001"),
    candle_grace_profit_pct=Decimal("0.0088"),
    persist_exchange_operations="submit,cancel",
)


def load_live_runtime_manifest(
    path: Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> LiveRuntimeManifest:
    """Load one manifest and expand only explicit shell-style references.

    Bare ``${NAME}`` and ``${NAME:?message}`` references are required.  The
    ``${NAME:-default}`` form is useful for local validation and keeps the
    checked-in manifest free of account credentials.
    """

    if not path.is_file():
        raise RuntimeManifestError(f"runtime manifest does not exist: {path}")
    values = os.environ if environment is None else environment
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeManifestError(
            f"cannot read runtime manifest {path}: {error}"
        ) from error
    expanded = _expand_environment_references(raw_text, values)
    try:
        document = yaml.safe_load(expanded)
    except yaml.YAMLError as error:
        raise RuntimeManifestError(
            f"runtime manifest is not valid YAML: {path}"
        ) from error
    return _parse_manifest(document, path=path)


def _expand_environment_references(
    value: str,
    environment: Mapping[str, str],
) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        raw_value = environment.get(name, "")
        operator = match.group("operator")
        argument = match.group("argument") or ""
        if operator == ":-" and raw_value == "":
            return argument
        if operator == ":?" and raw_value == "":
            detail = argument or f"{name} is required"
            raise RuntimeManifestError(detail)
        if operator is None and raw_value == "":
            raise RuntimeManifestError(f"{name} is required")
        return raw_value

    return _ENV_REFERENCE.sub(replace, value)


def _parse_manifest(document: Any, *, path: Path) -> LiveRuntimeManifest:
    root = _mapping(document, "root", path)
    schema_version = root.get("schema_version")
    if schema_version != 1:
        raise RuntimeManifestError(
            f"runtime manifest {path} must use schema_version: 1"
        )

    runtime = _mapping(root.get("runtime"), "runtime", path)
    default_image_commit = _text(runtime.get("image_commit"), "runtime.image_commit")
    default_migration_revision = _text(
        runtime.get("migration_revision"),
        "runtime.migration_revision",
    )
    raw_accounts = root.get("accounts")
    if not isinstance(raw_accounts, list) or not raw_accounts:
        raise RuntimeManifestError(
            f"runtime manifest {path} must define a non-empty accounts list"
        )

    accounts: list[LiveRuntimeAccount] = []
    labels: set[str] = set()
    for index, raw_account in enumerate(raw_accounts):
        prefix = f"accounts[{index}]"
        account = _mapping(raw_account, prefix, path)
        label = _text(account.get("label"), f"{prefix}.label")
        if label in labels:
            raise RuntimeManifestError(f"duplicate runtime account label: {label}")
        labels.add(label)
        services = _services(account.get("services"), prefix)
        strategy_inputs = _strategy_inputs(
            account.get("strategy_config"),
            prefix,
        )
        execution_inputs = _execution_inputs(
            account.get("execution_config"),
            prefix,
        )
        accounts.append(
            LiveRuntimeAccount(
                label=label,
                strategy=_text(account.get("strategy"), f"{prefix}.strategy"),
                session_id=_text(
                    account.get("session_id"),
                    f"{prefix}.session_id",
                ),
                lease_owner=_text(
                    account.get("lease_owner"),
                    f"{prefix}.lease_owner",
                ),
                image_commit=_text(
                    account.get("image_commit", default_image_commit),
                    f"{prefix}.image_commit",
                ),
                migration_revision=_text(
                    account.get("migration_revision", default_migration_revision),
                    f"{prefix}.migration_revision",
                ),
                strategy_config_hash=_text(
                    account.get("strategy_config_hash", "unset"),
                    f"{prefix}.strategy_config_hash",
                ).lower(),
                profile_ref=_text(
                    account.get("profile_ref"),
                    f"{prefix}.profile_ref",
                ),
                limits_ref=_text(
                    account.get("limits_ref"),
                    f"{prefix}.limits_ref",
                ),
                services=services,
                strategy_inputs=strategy_inputs,
                execution_inputs=execution_inputs,
            )
        )
    return LiveRuntimeManifest(
        schema_version=schema_version,
        accounts=tuple(accounts),
    )


def _mapping(value: Any, field: str, path: Path) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeManifestError(f"{path}: {field} must be a mapping")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeManifestError(f"{field} must be a non-empty string")
    return value.strip()


def _services(value: Any, prefix: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise RuntimeManifestError(f"{prefix}.services must be a non-empty list")
    services = tuple(
        _text(item, f"{prefix}.services[{index}]")
        for index, item in enumerate(value)
    )
    if len(set(services)) != len(services):
        raise RuntimeManifestError(f"{prefix}.services must not contain duplicates")
    return services


def _strategy_inputs(value: Any, prefix: str) -> LiveRuntimeStrategyInputs:
    field = f"{prefix}.strategy_config"
    config = _mapping(value, field, Path("runtime-manifest"))
    try:
        profile = LiveOrderFlowImpulseProfile(
            impulse_window_buckets=_integer(
                config.get("impulse_window_buckets"),
                f"{field}.impulse_window_buckets",
            ),
            confirmation_buckets=_integer(
                config.get("confirmation_buckets"),
                f"{field}.confirmation_buckets",
            ),
            min_return_pct=_decimal(
                config.get("min_return_pct"),
                f"{field}.min_return_pct",
            ),
            min_aggressive_imbalance=_decimal(
                config.get("min_imbalance"),
                f"{field}.min_imbalance",
            ),
            min_notional_intensity=_decimal(
                config.get("min_intensity"),
                f"{field}.min_intensity",
            ),
            min_notional_5m_vs_30m=_decimal(
                config.get("min_notional_5m_vs_30m"),
                f"{field}.min_notional_5m_vs_30m",
            ),
            cooldown_buckets=_integer(
                config.get("cooldown_buckets"),
                f"{field}.cooldown_buckets",
            ),
        )
        top_count = _integer(
            config.get("entry_positive_gainer_top_count"),
            f"{field}.entry_positive_gainer_top_count",
        )
        if top_count <= 0:
            raise RuntimeManifestError(
                f"{field}.entry_positive_gainer_top_count must be positive"
            )
        policy_mode = _text(
            config.get("entry_policy_mode"),
            f"{field}.entry_policy_mode",
        ).lower()
        if policy_mode not in {"legacy", "compare_only", "enforce"}:
            raise RuntimeManifestError(
                f"{field}.entry_policy_mode must be legacy, compare_only, or enforce"
            )
        order_type = EntryType(
            _text(config.get("entry_order_type"), f"{field}.entry_order_type").lower()
        )
        ttl_seconds = _integer(
            config.get("entry_limit_ttl_seconds"),
            f"{field}.entry_limit_ttl_seconds",
        )
        if ttl_seconds < 601:
            raise RuntimeManifestError(
                f"{field}.entry_limit_ttl_seconds must be at least 601"
            )
        return LiveRuntimeStrategyInputs(
            profile=profile,
            entry_positive_gainer_top_count=top_count,
            require_price_above_ema5=_boolean(
                config.get("require_price_above_ema5"),
                f"{field}.require_price_above_ema5",
            ),
            require_price_above_ema10=_boolean(
                config.get("require_price_above_ema10"),
                f"{field}.require_price_above_ema10",
            ),
            entry_policy_mode=policy_mode,
            entry_order_type=order_type,
            entry_limit_ttl_seconds=ttl_seconds,
        )
    except (InvalidOperation, ValueError) as error:
        if isinstance(error, RuntimeManifestError):
            raise
        raise RuntimeManifestError(f"{field} is invalid: {error}") from error


def _execution_inputs(
    value: Any,
    prefix: str,
) -> LiveRuntimeExecutionInputs:
    if value is None:
        return _DEFAULT_EXECUTION_INPUTS
    field = f"{prefix}.execution_config"
    config = _mapping(value, field, Path("runtime-manifest"))
    try:
        hedge_mode = _boolean(
            config.get("hedge_mode"),
            f"{field}.hedge_mode",
        )
        entry_long_only = _boolean(
            config.get("entry_long_only"),
            f"{field}.entry_long_only",
        )
        entry_leverage = _integer(
            config.get("entry_leverage"),
            f"{field}.entry_leverage",
        )
        if not 1 <= entry_leverage <= 125:
            raise RuntimeManifestError(
                f"{field}.entry_leverage must be between 1 and 125"
            )
        margin_type = _text(
            config.get("margin_type"),
            f"{field}.margin_type",
        ).upper()
        if margin_type not in {"CROSSED", "ISOLATED"}:
            raise RuntimeManifestError(
                f"{field}.margin_type must be CROSSED or ISOLATED"
            )
        exit_mode = PositionExitMode(
            _text(config.get("exit_mode"), f"{field}.exit_mode").lower()
        )
        take_profit_pct = _positive_decimal(
            config.get("take_profit_pct"),
            f"{field}.take_profit_pct",
        )
        stop_loss_pct = _positive_decimal(
            config.get("stop_loss_pct"),
            f"{field}.stop_loss_pct",
        )
        candle_grace_bars = _integer(
            config.get("candle_grace_bars"),
            f"{field}.candle_grace_bars",
        )
        if candle_grace_bars < 0:
            raise RuntimeManifestError(
                f"{field}.candle_grace_bars must not be negative"
            )
        candle_grace_decision_profit_pct = _decimal(
            config.get("candle_grace_decision_profit_pct"),
            f"{field}.candle_grace_decision_profit_pct",
        )
        candle_grace_profit_pct = _decimal(
            config.get("candle_grace_profit_pct"),
            f"{field}.candle_grace_profit_pct",
        )
        if candle_grace_bars > 0 and candle_grace_decision_profit_pct <= 0:
            raise RuntimeManifestError(
                f"{field}.candle_grace_decision_profit_pct must be positive "
                "when grace is enabled"
            )
        for candidate, name in (
            (candle_grace_decision_profit_pct, "candle_grace_decision_profit_pct"),
            (candle_grace_profit_pct, "candle_grace_profit_pct"),
        ):
            if not candidate.is_finite() or not 0 <= candidate < 1:
                raise RuntimeManifestError(
                    f"{field}.{name} must be finite and in [0, 1)"
                )
        persist_exchange_operations = _operations(
            config.get("persist_exchange_operations"),
            f"{field}.persist_exchange_operations",
        )
        return LiveRuntimeExecutionInputs(
            hedge_mode=hedge_mode,
            entry_long_only=entry_long_only,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
            exit_mode=exit_mode,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
            candle_grace_bars=candle_grace_bars,
            candle_grace_decision_profit_pct=candle_grace_decision_profit_pct,
            candle_grace_profit_pct=candle_grace_profit_pct,
            persist_exchange_operations=persist_exchange_operations,
        )
    except (InvalidOperation, ValueError) as error:
        if isinstance(error, RuntimeManifestError):
            raise
        raise RuntimeManifestError(f"{field} is invalid: {error}") from error


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise RuntimeManifestError(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise RuntimeManifestError(f"{field} must be an integer") from error


def _decimal(value: Any, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise RuntimeManifestError(f"{field} must be a decimal") from error


def _positive_decimal(value: Any, field: str) -> Decimal:
    parsed = _decimal(value, field)
    if not parsed.is_finite() or parsed <= 0:
        raise RuntimeManifestError(f"{field} must be finite and positive")
    return parsed


def _operations(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise RuntimeManifestError(f"{field} must be a comma-separated string")
    normalized = value.strip()
    if not normalized:
        raise RuntimeManifestError(f"{field} must not be empty")
    if normalized.lower() == "all":
        return "all"
    operations = tuple(item.strip() for item in normalized.split(","))
    if any(not item for item in operations):
        raise RuntimeManifestError(
            f"{field} must contain only non-empty operation names"
        )
    if any(item.lower() == "all" for item in operations):
        raise RuntimeManifestError(f"{field} accepts 'all' only by itself")
    if len(set(operations)) != len(operations):
        raise RuntimeManifestError(f"{field} must not contain duplicates")
    return ",".join(sorted(operations))


def _boolean(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise RuntimeManifestError(f"{field} must be a boolean")


__all__ = [
    "LiveRuntimeAccount",
    "LiveRuntimeExecutionInputs",
    "LiveRuntimeManifest",
    "LiveRuntimeStrategyInputs",
    "RuntimeManifestError",
    "load_live_runtime_manifest",
]
