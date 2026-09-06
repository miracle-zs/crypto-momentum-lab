import pytest
from typer import BadParameter
from typer.testing import CliRunner

from crypto_momentum_lab.apps.execution_account import main
from crypto_momentum_lab.config import BinanceCredentialRole

runner = CliRunner()


def test_execution_account_sync_once_requires_credentials(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_SECRET", raising=False)

    result = runner.invoke(
        main.app,
        [
            "sync-once",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
        ],
    )

    assert result.exit_code != 0
    assert "BINANCE_READ_API_KEY" in result.output


def test_execution_account_can_use_legacy_credentials_only_with_explicit_flag(
    monkeypatch,
) -> None:
    monkeypatch.delenv("BINANCE_READ_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_SECRET", raising=False)
    monkeypatch.setenv("BINANCE_API_KEY", "legacy-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "legacy-secret")

    with pytest.raises(BadParameter, match="BINANCE_READ_API_KEY"):
        main._resolve_cli_credentials(
            role=BinanceCredentialRole.READ,
            api_key_env=None,
            api_secret_env=None,
            allow_legacy_fallback=False,
        )

    resolved = main._resolve_cli_credentials(
        role=BinanceCredentialRole.READ,
        api_key_env=None,
        api_secret_env=None,
        allow_legacy_fallback=True,
    )
    assert resolved.api_key == "legacy-key"
    assert resolved.api_key_env == "BINANCE_API_KEY"
