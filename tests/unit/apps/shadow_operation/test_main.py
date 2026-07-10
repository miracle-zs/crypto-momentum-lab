from typer.testing import CliRunner

from crypto_momentum_lab.apps.shadow_operation.main import app

runner = CliRunner()


def test_shadow_cli_exposes_run_report_and_drill_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "run" in result.stdout
    assert "report" in result.stdout
    assert "drill" in result.stdout


def test_shadow_run_requires_database_url() -> None:
    result = runner.invoke(
        app,
        ["run", "--run-id", "shadow-test"],
        env={"CML_DATABASE_URL": ""},
    )

    assert result.exit_code != 0
    assert "database-url" in result.output
