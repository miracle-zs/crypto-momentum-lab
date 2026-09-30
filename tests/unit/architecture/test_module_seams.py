"""Guard application seams against accidental eager database imports."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.decision_facts",
        "crypto_momentum_lab.live_rollout.position_classification",
        "crypto_momentum_lab.operator_dashboard.ports",
        "crypto_momentum_lab.live_rollout.position_self_healing",
        "crypto_momentum_lab.domain.execution.position_repair",
        "crypto_momentum_lab.domain.execution.position_recovery",
        "crypto_momentum_lab.domain.execution.command_lifecycle",
        "crypto_momentum_lab.domain.execution.command_codec",
        "crypto_momentum_lab.domain.execution.command_repository",
    ],
)
def test_application_modules_import_without_storage(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class StorageGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname == 'sqlalchemy'
                or fullname.startswith('crypto_momentum_lab.persistence')):
            raise RuntimeError('application imported storage: ' + fullname)
sys.meta_path.insert(0, StorageGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "package, forbidden",
    [
        (
            "crypto_momentum_lab.operator_dashboard",
            "crypto_momentum_lab.operator_dashboard.api",
        ),
        (
            "crypto_momentum_lab.strategy_runner",
            "crypto_momentum_lab.strategy_runner.live_source",
        ),
    ],
)
def test_package_import_does_not_load_application_adapters(
    package: str,
    forbidden: str,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib, sys; importlib.import_module(sys.argv[1]); "
            "assert sys.argv[2] not in sys.modules",
            package,
            forbidden,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
