from dataclasses import replace
from pathlib import Path

import pytest

from crypto_momentum_lab.live_rollout.runtime_manifest import load_live_runtime_manifest


@pytest.fixture
def account_config():
    manifest = load_live_runtime_manifest(
        Path("deploy/live-runtime.yaml"), environment={"CML_CODE_COMMIT": "test-code"}
    )
    return replace(manifest.account("primary"), label="a1")
