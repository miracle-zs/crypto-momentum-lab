import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import NoReturn

from crypto_momentum_lab.config.models import (
    BinanceCredentialConfig,
    BinanceCredentialRef,
    BinanceCredentialRole,
)

LEGACY_BINANCE_CREDENTIAL_REF = BinanceCredentialRef(
    api_key_env="BINANCE_API_KEY",
    api_secret_env="BINANCE_API_SECRET",
)

_DEFAULT_ROLE_ENV_NAMES: dict[
    BinanceCredentialRole, tuple[str, str]
] = {
    BinanceCredentialRole.READ: (
        "BINANCE_READ_API_KEY",
        "BINANCE_READ_API_SECRET",
    ),
    BinanceCredentialRole.TRADE: (
        "BINANCE_TRADE_API_KEY",
        "BINANCE_TRADE_API_SECRET",
    ),
}


class CredentialResolutionError(ValueError):
    """Raised when a configured credential pair cannot be resolved safely."""


@dataclass(frozen=True, slots=True, repr=False)
class ResolvedBinanceCredentials:
    """A resolved credential pair with secret-free diagnostics.

    The raw values are available only for constructing the Binance adapter.  The
    generated representation deliberately contains environment names and a
    short one-way API-key fingerprint, never either secret value.
    """

    role: BinanceCredentialRole
    api_key_env: str
    api_secret_env: str
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    @property
    def api_key_fingerprint(self) -> str:
        """Return a stable, non-secret marker for startup metadata."""

        return hashlib.sha256(self.api_key.encode("utf-8")).hexdigest()[:16]

    def metadata(self) -> dict[str, str]:
        """Return diagnostics that are safe to log or persist."""

        return {
            "credential_role": self.role.value,
            "api_key_env": self.api_key_env,
            "api_secret_env": self.api_secret_env,
            "api_key_fingerprint": self.api_key_fingerprint,
        }

    def __repr__(self) -> str:
        return (
            "ResolvedBinanceCredentials("
            f"role={self.role.value!r}, "
            f"api_key_env={self.api_key_env!r}, "
            f"api_secret_env={self.api_secret_env!r}, "
            f"api_key_fingerprint={self.api_key_fingerprint!r})"
        )


def resolve_binance_credentials(
    config: BinanceCredentialConfig,
    role: BinanceCredentialRole,
    *,
    environ: Mapping[str, str] | None = None,
    legacy_ref: BinanceCredentialRef | None = None,
    allow_legacy_fallback: bool = False,
) -> ResolvedBinanceCredentials:
    """Resolve one role's credentials from an injected environment mapping.

    The configuration model validates role separation before this seam is
    crossed.  This function only resolves the selected pair and fails closed
    when either variable is absent or blank; it never includes values in its
    error text.
    """

    values = os.environ if environ is None else environ
    reference = _reference_for_role(config, role)
    missing = _missing_names(reference, values)
    if not missing:
        return _resolved_from_reference(role, reference, values)

    # A partially configured role must fail closed instead of silently mixing
    # one role-specific value with a legacy value from another source.
    if len(missing) != 2 or not allow_legacy_fallback or legacy_ref is None:
        _raise_missing(role, reference, missing)

    # The guard above proves the optional fallback reference is present; keep a
    # local non-optional binding so the invariant is clear for type checkers.
    fallback_ref = legacy_ref
    legacy_missing = _missing_names(fallback_ref, values)
    if legacy_missing:
        primary_names = ", ".join(
            (reference.api_key_env, reference.api_secret_env)
        )
        legacy_names = ", ".join(
            (fallback_ref.api_key_env, fallback_ref.api_secret_env)
        )
        raise CredentialResolutionError(
            f"{role.value} credential variables are missing or blank: "
            f"{primary_names}; legacy fallback variables are missing or blank: "
            f"{legacy_names}"
        )
    return _resolved_from_reference(role, fallback_ref, values)


def credential_config_for_role(
    role: BinanceCredentialRole,
    *,
    api_key_env: str | None = None,
    api_secret_env: str | None = None,
) -> BinanceCredentialConfig:
    """Build a role-separated config while allowing one role's env names.

    The opposite role always uses the conventional role-specific names.  This
    keeps the CLI override small while ensuring the resulting config still
    describes both roles and rejects accidental overlap.
    """

    selected_key_env, selected_secret_env = _DEFAULT_ROLE_ENV_NAMES[role]
    if api_key_env is not None:
        selected_key_env = api_key_env
    if api_secret_env is not None:
        selected_secret_env = api_secret_env
    selected = BinanceCredentialRef(
        api_key_env=selected_key_env,
        api_secret_env=selected_secret_env,
    )
    read_ref = (
        selected
        if role is BinanceCredentialRole.READ
        else _default_reference(BinanceCredentialRole.READ)
    )
    trade_ref = (
        selected
        if role is BinanceCredentialRole.TRADE
        else _default_reference(BinanceCredentialRole.TRADE)
    )
    return BinanceCredentialConfig(read=read_ref, trade=trade_ref)


def resolve_role_credentials(
    role: BinanceCredentialRole,
    *,
    api_key_env: str | None = None,
    api_secret_env: str | None = None,
    environ: Mapping[str, str] | None = None,
    allow_legacy_fallback: bool = False,
) -> ResolvedBinanceCredentials:
    """Resolve one conventional role, with an explicit legacy fallback.

    This is the composition-root seam used by long-running services.  Callers
    may override both names for a secret store, but a partial override is
    rejected so a key and secret cannot silently come from different roles.
    """

    if (api_key_env is None) != (api_secret_env is None):
        raise CredentialResolutionError(
            "api-key and api-secret environment overrides must be provided together"
        )
    config = credential_config_for_role(
        role,
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
    )
    return resolve_binance_credentials(
        config,
        role,
        environ=environ,
        legacy_ref=LEGACY_BINANCE_CREDENTIAL_REF,
        allow_legacy_fallback=allow_legacy_fallback,
    )


def _default_reference(role: BinanceCredentialRole) -> BinanceCredentialRef:
    api_key_env, api_secret_env = _DEFAULT_ROLE_ENV_NAMES[role]
    return BinanceCredentialRef(
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
    )


def _missing_names(
    reference: BinanceCredentialRef,
    values: Mapping[str, str],
) -> tuple[str, ...]:
    return tuple(
        variable_name
        for variable_name, value in (
            (reference.api_key_env, values.get(reference.api_key_env)),
            (reference.api_secret_env, values.get(reference.api_secret_env)),
        )
        if value is None or not value.strip()
    )


def _raise_missing(
    role: BinanceCredentialRole,
    reference: BinanceCredentialRef,
    missing: tuple[str, ...],
) -> NoReturn:
    joined_names = ", ".join(missing)
    raise CredentialResolutionError(
        f"{role.value} credential variables are missing or blank: {joined_names}"
    )


def _resolved_from_reference(
    role: BinanceCredentialRole,
    reference: BinanceCredentialRef,
    values: Mapping[str, str],
) -> ResolvedBinanceCredentials:
    # Callers reach this helper only after _missing_names has established the
    # invariant; indexing keeps the invariant explicit for type checkers.
    api_key = values[reference.api_key_env]
    api_secret = values[reference.api_secret_env]
    return ResolvedBinanceCredentials(
        role=role,
        api_key_env=reference.api_key_env,
        api_secret_env=reference.api_secret_env,
        api_key=api_key,
        api_secret=api_secret,
    )


def _reference_for_role(
    config: BinanceCredentialConfig,
    role: BinanceCredentialRole,
) -> BinanceCredentialRef:
    if role is BinanceCredentialRole.READ:
        return config.read
    if role is BinanceCredentialRole.TRADE:
        return config.trade
    raise CredentialResolutionError(f"unsupported Binance credential role: {role!r}")
