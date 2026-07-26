from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_momentum_lab.config.loader import behavior_hash, load_runtime_config
from crypto_momentum_lab.config.models import CaptureConfig


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
    capture_path = tmp_path / "capture.yaml"
    capture_path.write_text(
        "\n".join(
            [
                "market_websocket_url: wss://fstream.binance.com/market/ws",
                "public_websocket_url: wss://fstream.binance.com/public/ws",
                "enabled_streams:",
                "  - aggTrade",
                "max_subscriptions_per_connection: 100",
                "control_messages_per_second: 5",
                "connection_lifetime_seconds: 82800",
                "open_timeout_seconds: 10",
                "ping_interval_seconds: 180",
                "ping_timeout_seconds: 600",
                "silence_timeout_seconds: 30",
                "queue_max_events: 1000",
                "queue_max_bytes: 1000000",
                "shutdown_timeout_seconds: 30",
                "archive:",
                "  root: data/raw",
                "  zstd_level: 3",
                "  rotation_uncompressed_bytes: 1000000",
                "  max_open_writers: 64",
                "  group_commit_max_events: 250",
                "  group_commit_max_milliseconds: 250",
                "  warning_free_bytes: 300",
                "  halt_free_bytes: 200",
                "  recovery_free_bytes: 250",
                "  disk_check_interval_seconds: 10",
                "  pending_manifest_max_age_seconds: 300",
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
                f"capture_config: {capture_path}",
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
    capture_path = tmp_path / "capture.yaml"
    capture_path.write_text(
        "\n".join(
            [
                "market_websocket_url: wss://fstream.binance.com/market/ws",
                "public_websocket_url: wss://fstream.binance.com/public/ws",
                "enabled_streams:",
                "  - aggTrade",
                "max_subscriptions_per_connection: 100",
                "control_messages_per_second: 5",
                "connection_lifetime_seconds: 82800",
                "open_timeout_seconds: 10",
                "ping_interval_seconds: 180",
                "ping_timeout_seconds: 600",
                "silence_timeout_seconds: 30",
                "queue_max_events: 1000",
                "queue_max_bytes: 1000000",
                "shutdown_timeout_seconds: 30",
                "archive:",
                "  root: data/raw",
                "  zstd_level: 3",
                "  rotation_uncompressed_bytes: 1000000",
                "  max_open_writers: 64",
                "  group_commit_max_events: 250",
                "  group_commit_max_milliseconds: 250",
                "  warning_free_bytes: 300",
                "  halt_free_bytes: 200",
                "  recovery_free_bytes: 250",
                "  disk_check_interval_seconds: 10",
                "  pending_manifest_max_age_seconds: 300",
            ]
        ),
        encoding="utf-8",
    )
    environment_path = tmp_path / "research.yaml"
    environment_path.write_text(
        "environment: research\n"
        "binance_base_url: https://fapi.binance.com\n"
        f"universe_config: {universe_path}\n"
        f"capture_config: {capture_path}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://user:secret@localhost/db",
    )

    with pytest.raises(ValidationError):
        load_runtime_config(environment_path)


def test_loads_websocket_capture_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CML_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml",
    )

    config = load_runtime_config(
        Path("configs/environments/research.yaml")
    )

    assert str(config.capture.market_websocket_url) == (
        "wss://fstream.binance.com/market/ws"
    )
    assert str(config.capture.public_websocket_url) == (
        "wss://fstream.binance.com/public/ws"
    )
    assert config.capture.enabled_streams == (
        "aggTrade",
        "bookTicker",
        "forceOrder",
        "markPrice@1s",
        "kline_1m",
    )
    assert config.capture.max_subscriptions_per_connection == 100
    assert config.capture.closure_delay_seconds == 3.0
    assert config.capture.archive.max_open_writers == 512
    assert config.capture.archive.group_commit_max_events == 250
    assert config.capture.archive.group_commit_max_milliseconds == 250


def test_capture_config_rejects_invalid_disk_hysteresis() -> None:
    with pytest.raises(
        ValueError,
        match="recovery_free_bytes must be greater",
    ):
        CaptureConfig.model_validate(
            {
                "market_websocket_url": "wss://example.test/market/ws",
                "public_websocket_url": "wss://example.test/public/ws",
                "enabled_streams": ["aggTrade"],
                "max_subscriptions_per_connection": 100,
                "control_messages_per_second": 5,
                "connection_lifetime_seconds": 82800,
                "open_timeout_seconds": 10,
                "ping_interval_seconds": 180,
                "ping_timeout_seconds": 600,
                "silence_timeout_seconds": 30,
                "queue_max_events": 1000,
                "queue_max_bytes": 1000000,
                "shutdown_timeout_seconds": 30,
                "archive": {
                    "root": "data/raw",
                    "zstd_level": 3,
                    "rotation_uncompressed_bytes": 1000000,
                    "max_open_writers": 64,
                    "group_commit_max_events": 250,
                    "group_commit_max_milliseconds": 250,
                    "warning_free_bytes": 300,
                    "halt_free_bytes": 200,
                    "recovery_free_bytes": 100,
                    "disk_check_interval_seconds": 10,
                    "pending_manifest_max_age_seconds": 300,
                },
            }
        )


def test_capture_config_rejects_non_websocket_urls() -> None:
    with pytest.raises(
        ValueError,
        match="websocket URLs must use ws or wss",
    ):
        CaptureConfig.model_validate(
            {
                "market_websocket_url": "https://example.test/market/ws",
                "public_websocket_url": "wss://example.test/public/ws",
                "enabled_streams": ["aggTrade"],
                "max_subscriptions_per_connection": 100,
                "control_messages_per_second": 5,
                "connection_lifetime_seconds": 82800,
                "open_timeout_seconds": 10,
                "ping_interval_seconds": 180,
                "ping_timeout_seconds": 600,
                "silence_timeout_seconds": 30,
                "queue_max_events": 1000,
                "queue_max_bytes": 1000000,
                "shutdown_timeout_seconds": 30,
                "archive": {
                    "root": "data/raw",
                    "zstd_level": 3,
                    "rotation_uncompressed_bytes": 1000000,
                    "max_open_writers": 64,
                    "group_commit_max_events": 250,
                    "group_commit_max_milliseconds": 250,
                    "warning_free_bytes": 300,
                    "halt_free_bytes": 200,
                    "recovery_free_bytes": 250,
                    "disk_check_interval_seconds": 10,
                    "pending_manifest_max_age_seconds": 300,
                },
            }
        )
