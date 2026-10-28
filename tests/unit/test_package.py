import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from crypto_momentum_lab import __version__


def test_package_exposes_version() -> None:
    assert __version__ == "0.1.0"


def test_package_readme_resolves_to_the_single_system_document() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["readme"] == "SYSTEM.md"
    assert (root / project["readme"]).is_file()


def test_runtime_lock_satisfies_declared_dependencies() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    entries = [
        Requirement(line)
        for line in (root / "requirements-runtime.lock").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    versions = {
        canonicalize_name(entry.name): next(iter(entry.specifier)).version
        for entry in entries
    }
    assert all(
        len(entry.specifier) == 1 and next(iter(entry.specifier)).operator == "=="
        for entry in entries
    )
    for dependency in project["dependencies"]:
        requirement = Requirement(dependency)
        version = versions[canonicalize_name(requirement.name)]
        assert version in requirement.specifier
