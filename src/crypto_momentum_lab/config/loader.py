import hashlib
import json
import os
from pathlib import Path
from typing import Any

import yaml

from crypto_momentum_lab.config.models import (
    EnvironmentFile,
    RuntimeConfig,
    UniverseConfig,
)


def _read_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return loaded


def load_runtime_config(environment_path: Path) -> RuntimeConfig:
    environment = EnvironmentFile.model_validate(_read_yaml(environment_path))
    database_url = os.environ["CML_DATABASE_URL"]
    universe = UniverseConfig.model_validate(_read_yaml(environment.universe_config))
    return RuntimeConfig(
        environment=environment.environment,
        database_url=database_url,
        binance_base_url=environment.binance_base_url,
        universe=universe,
    )


def behavior_hash(config: RuntimeConfig) -> str:
    payload = {
        "environment": config.environment,
        "binance_base_url": str(config.binance_base_url),
        "universe": config.universe.model_dump(mode="json"),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
