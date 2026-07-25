from pathlib import Path


def test_static_assets_are_packaged() -> None:
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert 'packages = ["src/crypto_momentum_lab"]' in pyproject
    assert "force-include" not in pyproject
    assert Path(
        "src/crypto_momentum_lab/operator_dashboard/static/index.html"
    ).is_file()
