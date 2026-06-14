from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_momentum_lab.config.loader import behavior_hash, load_runtime_config


def test_load_runtime_config_is_frozen_and_hash_is_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        "\n".join(
            [
                "top_count: 20",
                "retention_rank: 30",
                "retention_hours: 2",
                "activation_minute: 1",
            ]
        ),
        encoding="utf-8",
    )
    environment_path = tmp_path / "research.yaml"
    environment_path.write_text(
        "\n".join(
            [
                "environment: research",
                "binance_base_url: https://fapi.binance.com",
                f"universe_config: {universe_path}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://user:secret@localhost/db",
    )

    first = load_runtime_config(environment_path)
    second = load_runtime_config(environment_path)

    assert first.universe.top_count == 20
    assert behavior_hash(first) == behavior_hash(second)
    assert "secret" not in behavior_hash(first)
    with pytest.raises(ValidationError):
        first.universe.top_count = 10  # type: ignore[misc]


def test_rejects_retention_rank_smaller_than_target_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        "top_count: 20\nretention_rank: 10\nretention_hours: 2\n"
        "activation_minute: 1\n",
        encoding="utf-8",
    )
    environment_path = tmp_path / "research.yaml"
    environment_path.write_text(
        "environment: research\n"
        "binance_base_url: https://fapi.binance.com\n"
        f"universe_config: {universe_path}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://user:secret@localhost/db",
    )

    with pytest.raises(ValidationError):
        load_runtime_config(environment_path)
