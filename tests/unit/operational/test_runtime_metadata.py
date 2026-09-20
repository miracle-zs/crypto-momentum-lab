from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from crypto_momentum_lab.domain.operational.runtime_metadata import (
    RuntimeMetadataSnapshot,
    compute_content_hash,
)


def test_compute_content_hash() -> None:
    dict_a = {"b": 2, "a": 1}
    dict_b = {"a": 1, "b": 2}
    # Determinism across key insertion ordering
    assert compute_content_hash(dict_a) == compute_content_hash(dict_b)

    hash_str = compute_content_hash("hello world")
    assert len(hash_str) == 64
    assert hash_str == compute_content_hash(b"hello world")

    with pytest.raises(TypeError, match="Unsupported content type"):
        compute_content_hash(12345)  # type: ignore[arg-type]


def test_runtime_metadata_snapshot_validations() -> None:
    now = datetime.now(timezone.utc)
    valid_snapshot = RuntimeMetadataSnapshot(
        environment="live",
        account_label="binance-sub01",
        git_commit="abcdef1234567890abcdef1234567890abcdef12",
        code_generation="abcdef1234567890abcdef1234567890abcdef12",
        python_version="3.13.3",
        strategy_config_hash="1111222233334444555566667777888899990000aaaa11112222333344445555",
        risk_config_hash="222233334444555566667777888899990000aaaa111122223333444455556666",
        trading_rules_hash="33334444555566667777888899990000aaaa1111222233334444555566667777",
        started_at=now,
    )
    assert valid_snapshot.environment == "live"

    # Immutability
    with pytest.raises(FrozenInstanceError):
        valid_snapshot.environment = "paper"  # type: ignore[misc]

    # Empty validation
    with pytest.raises(ValueError, match="environment must not be empty"):
        RuntimeMetadataSnapshot(
            environment="",
            account_label="binance-sub01",
            git_commit="abc",
            code_generation="abc",
            python_version="3.13.3",
            strategy_config_hash="hash",
            risk_config_hash="hash",
            trading_rules_hash="hash",
            started_at=now,
        )

    # Naive started_at
    with pytest.raises(ValueError, match="started_at must be timezone-aware"):
        RuntimeMetadataSnapshot(
            environment="live",
            account_label="binance-sub01",
            git_commit="abc",
            code_generation="abc",
            python_version="3.13.3",
            strategy_config_hash="hash",
            risk_config_hash="hash",
            trading_rules_hash="hash",
            started_at=datetime(2026, 9, 20, 12, 0, 0),
        )


def test_runtime_metadata_snapshot_serialization_roundtrip() -> None:
    now = datetime(2026, 9, 20, 12, 34, 56, tzinfo=timezone.utc)
    snapshot = RuntimeMetadataSnapshot(
        environment="paper",
        account_label="paper-01",
        git_commit="commit123",
        code_generation="commit123",
        python_version="3.13.3",
        strategy_config_hash="hash1",
        risk_config_hash="hash2",
        trading_rules_hash="hash3",
        started_at=now,
    )

    serialized = snapshot.to_dict()
    assert serialized["environment"] == "paper"
    assert serialized["started_at"] == "2026-09-20T12:34:56+00:00"

    deserialized = RuntimeMetadataSnapshot.from_dict(serialized)
    assert deserialized == snapshot


def test_runtime_metadata_snapshot_create_factory() -> None:
    strat_cfg = {"symbol": "BTCUSDT", "threshold": 0.02}
    risk_cfg = {"max_drawdown": 0.05}
    trading_rules = {"min_qty": 0.001}

    snapshot = RuntimeMetadataSnapshot.create(
        environment="live",
        account_label="main",
        git_commit="commit456",
        strategy_config=strat_cfg,
        risk_config=risk_cfg,
        trading_rules=trading_rules,
    )

    assert snapshot.environment == "live"
    assert snapshot.code_generation == "commit456"
    assert snapshot.strategy_config_hash == compute_content_hash(strat_cfg)
    assert snapshot.risk_config_hash == compute_content_hash(risk_cfg)
    assert snapshot.trading_rules_hash == compute_content_hash(trading_rules)
    assert snapshot.started_at.tzinfo is not None
