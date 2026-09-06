import pytest
from pydantic import ValidationError

from crypto_momentum_lab.config import (
    LEGACY_BINANCE_CREDENTIAL_REF,
    BinanceCredentialConfig,
    BinanceCredentialRef,
    BinanceCredentialRole,
    CredentialResolutionError,
    credential_config_for_role,
    resolve_binance_credentials,
    resolve_role_credentials,
)


def _credentials(*, allow_shared: bool = False) -> BinanceCredentialConfig:
    return BinanceCredentialConfig(
        read=BinanceCredentialRef(
            api_key_env="BINANCE_READ_API_KEY",
            api_secret_env="BINANCE_READ_API_SECRET",
        ),
        trade=BinanceCredentialRef(
            api_key_env="BINANCE_TRADE_API_KEY",
            api_secret_env="BINANCE_TRADE_API_SECRET",
        ),
        allow_shared=allow_shared,
    )


def test_credential_schema_keeps_roles_explicit_without_storing_secrets() -> None:
    credentials = _credentials()

    assert BinanceCredentialRole.READ.value == "read"
    assert credentials.read.api_key_env == "BINANCE_READ_API_KEY"
    assert credentials.trade.api_secret_env == "BINANCE_TRADE_API_SECRET"
    assert "BINANCE_READ_SECRET_VALUE" not in repr(credentials)


def test_runtime_resolver_selects_role_and_exposes_secret_free_metadata() -> None:
    credentials = _credentials()
    environment = {
        "BINANCE_READ_API_KEY": "read-key-value",
        "BINANCE_READ_API_SECRET": "read-secret-value",
        "BINANCE_TRADE_API_KEY": "trade-key-value",
        "BINANCE_TRADE_API_SECRET": "trade-secret-value",
    }

    resolved = resolve_binance_credentials(
        credentials,
        BinanceCredentialRole.TRADE,
        environ=environment,
    )

    assert resolved.api_key == "trade-key-value"
    assert resolved.api_secret == "trade-secret-value"
    assert resolved.metadata()["credential_role"] == "trade"
    assert resolved.metadata()["api_key_env"] == "BINANCE_TRADE_API_KEY"
    assert "trade-key-value" not in repr(resolved)
    assert "trade-secret-value" not in repr(resolved)
    assert "trade-secret-value" not in str(resolved.metadata())


def test_runtime_resolver_requires_explicit_legacy_fallback() -> None:
    credentials = _credentials()
    environment = {
        "BINANCE_API_KEY": "legacy-key-value",
        "BINANCE_API_SECRET": "legacy-secret-value",
    }

    with pytest.raises(CredentialResolutionError, match="BINANCE_READ_API_KEY"):
        resolve_binance_credentials(
            credentials,
            BinanceCredentialRole.READ,
            environ=environment,
            legacy_ref=LEGACY_BINANCE_CREDENTIAL_REF,
        )

    resolved = resolve_binance_credentials(
        credentials,
        BinanceCredentialRole.READ,
        environ=environment,
        legacy_ref=LEGACY_BINANCE_CREDENTIAL_REF,
        allow_legacy_fallback=True,
    )
    assert resolved.api_key == "legacy-key-value"
    assert resolved.api_key_env == "BINANCE_API_KEY"


def test_runtime_resolver_does_not_partially_fallback() -> None:
    environment = {
        "BINANCE_READ_API_KEY": "role-key-value",
        "BINANCE_API_SECRET": "legacy-secret-value",
    }

    with pytest.raises(CredentialResolutionError, match="BINANCE_READ_API_SECRET"):
        resolve_binance_credentials(
            _credentials(),
            BinanceCredentialRole.READ,
            environ=environment,
            legacy_ref=LEGACY_BINANCE_CREDENTIAL_REF,
            allow_legacy_fallback=True,
        )


def test_role_config_override_keeps_the_other_role_explicit() -> None:
    config = credential_config_for_role(
        BinanceCredentialRole.TRADE,
        api_key_env="CUSTOM_TRADE_KEY",
        api_secret_env="CUSTOM_TRADE_SECRET",
    )

    assert config.read.api_key_env == "BINANCE_READ_API_KEY"
    assert config.trade.api_secret_env == "CUSTOM_TRADE_SECRET"


def test_role_resolver_rejects_partial_environment_override() -> None:
    with pytest.raises(CredentialResolutionError, match="provided together"):
        resolve_role_credentials(
            BinanceCredentialRole.TRADE,
            api_key_env="CUSTOM_TRADE_KEY",
        )


@pytest.mark.parametrize(
    ("environment", "missing_name"),
    [
        (
            {"BINANCE_READ_API_SECRET": "read-secret-value"},
            "BINANCE_READ_API_KEY",
        ),
        (
            {
                "BINANCE_READ_API_KEY": "read-key-value",
                "BINANCE_READ_API_SECRET": "  ",
            },
            "BINANCE_READ_API_SECRET",
        ),
    ],
)
def test_runtime_resolver_fails_closed_without_secret_values(
    environment: dict[str, str],
    missing_name: str,
) -> None:
    with pytest.raises(CredentialResolutionError) as error:
        resolve_binance_credentials(
            _credentials(),
            BinanceCredentialRole.READ,
            environ=environment,
        )

    assert str(error.value).endswith(missing_name)
    assert "read-secret-value" not in str(error.value)


def test_shared_credential_pair_requires_an_explicit_migration_escape_hatch() -> None:
    shared = {
        "read": {
            "api_key_env": "BINANCE_API_KEY",
            "api_secret_env": "BINANCE_API_SECRET",
        },
        "trade": {
            "api_key_env": "BINANCE_API_KEY",
            "api_secret_env": "BINANCE_API_SECRET",
        },
    }

    with pytest.raises(ValidationError, match="allow_shared"):
        BinanceCredentialConfig.model_validate(shared)
    assert BinanceCredentialConfig.model_validate(
        {**shared, "allow_shared": True}
    ).allow_shared


def test_partial_credential_reference_overlap_is_rejected() -> None:
    with pytest.raises(ValidationError, match="must not overlap"):
        BinanceCredentialConfig(
            read=BinanceCredentialRef(
                api_key_env="BINANCE_READ_API_KEY",
                api_secret_env="BINANCE_READ_API_SECRET",
            ),
            trade=BinanceCredentialRef(
                api_key_env="BINANCE_READ_API_KEY",
                api_secret_env="BINANCE_TRADE_API_SECRET",
            ),
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"api_key_env": "", "api_secret_env": "BINANCE_SECRET"},
        {"api_key_env": "BINANCE-KEY", "api_secret_env": "BINANCE_SECRET"},
        {"api_key_env": "1_BINANCE_KEY", "api_secret_env": "BINANCE_SECRET"},
        {
            "api_key_env": "BINANCE_KEY",
            "api_secret_env": "BINANCE_KEY",
        },
    ],
)
def test_credential_reference_rejects_unsafe_environment_names(
    payload: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        BinanceCredentialRef.model_validate(payload)
