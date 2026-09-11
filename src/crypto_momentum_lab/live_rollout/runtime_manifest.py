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
    "LiveRuntimeManifest",
    "LiveRuntimeStrategyInputs",
    "RuntimeManifestError",
    "load_live_runtime_manifest",
]
