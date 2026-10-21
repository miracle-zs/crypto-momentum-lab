"""Public imports remain usable without running the trading application.

Do not freeze the old internal module graph: SYSTEM.md permits merging account
synchronization and execution. Domain values still must not depend on storage,
transport or process assembly.
"""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.domain.account",
        "crypto_momentum_lab.domain.execution",
        "crypto_momentum_lab.domain.market",
        "crypto_momentum_lab.domain.strategy",
        "crypto_momentum_lab.domain.risk",
    ],
)
def test_domain_packages_import_without_external_adapters(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class AdapterGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        forbidden = (
            'sqlalchemy', 'httpx', 'websockets',
            'crypto_momentum_lab.persistence',
            'crypto_momentum_lab.next_gen',
            'crypto_momentum_lab.live_rollout',
            'crypto_momentum_lab.operator_dashboard',
        )
        if any(fullname == name or fullname.startswith(name + '.')
               for name in forbidden):
            raise RuntimeError('domain imported adapter: ' + fullname)
sys.meta_path.insert(0, AdapterGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
