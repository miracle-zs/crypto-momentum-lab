from typer.testing import CliRunner

from crypto_momentum_lab.apps.execution_account import main

runner = CliRunner()


def test_execution_account_sync_once_requires_credentials(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)

    result = runner.invoke(
        main.app,
        [
            "sync-once",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
        ],
    )

    assert result.exit_code != 0
    assert "BINANCE_API_KEY and BINANCE_API_SECRET are required" in result.output
