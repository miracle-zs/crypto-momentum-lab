from typer.testing import CliRunner

from crypto_momentum_lab.apps.live_rollout.main import app

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
        "preflight",
        "run",
        "status",
        "disable-new-entries",
        "report",
    ):
        assert command in result.stdout
