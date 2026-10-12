import tomllib
from pathlib import Path

from crypto_momentum_lab import __version__


def test_package_exposes_version() -> None:
    assert __version__ == "0.1.0"


def test_package_readme_resolves_to_the_single_system_document() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["readme"] == "SYSTEM.md"
    assert (root / project["readme"]).is_file()
