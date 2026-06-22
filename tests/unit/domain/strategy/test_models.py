from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_config_hash,
    deterministic_signal_id,
)


def test_strategy_run_identity_requires_aware_created_at() -> None:
    with pytest.raises(ValueError, match="created_at must be timezone-aware"):
        StrategyRunIdentity(
            run_id="run-1",
            strategy_name="compression_breakout",
            strategy_version="v0",
            config_hash="abc",
            run_mode=RunMode.REPLAY,
            code_commit="unknown",
            created_at=datetime(2026, 6, 22, 0, 0),
            source_paths=("states",),
        )


def test_deterministic_config_hash_is_order_stable() -> None:
    left = deterministic_config_hash({"b": "2", "a": {"x": "1"}})
    right = deterministic_config_hash({"a": {"x": "1"}, "b": "2"})

    assert left == right
    assert len(left) == 64


def test_deterministic_signal_and_candidate_ids_are_stable() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    signal_id = deterministic_signal_id(
        identity=identity,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        sequence=1,
    )
    candidate_id = deterministic_candidate_id(signal_id=signal_id, sequence=1)

    assert signal_id == deterministic_signal_id(
        identity=identity,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        sequence=1,
    )
    assert signal_id.startswith("sig_")
    assert candidate_id == deterministic_candidate_id(
        signal_id=signal_id,
        sequence=1,
    )
    assert candidate_id.startswith("cand_")


def test_strategy_records_validate_timestamps_and_relationships() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    signal = StrategySignal(
        signal_id="sig_1",
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        source_state_at=detected_at,
        reason="compression_breakout",
        features={"range_high": "100"},
        reference_prices={"breakout_price": "101"},
    )
    candidate = OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id=signal.signal_id,
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=detected_at + timedelta(seconds=30),
        created_at=detected_at,
        reason="compression_breakout",
        features=signal.features,
    )
    rejection = StrategyRejection(
        reason=RejectionReason.NO_SIGNAL,
        symbol="ETHUSDT",
        bucket_start=detected_at,
        details={"state": "evaluated"},
    )
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": detected_at},
        warmup_buckets_by_symbol={"BTCUSDT": 4},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"buffer_sizes": {"BTCUSDT": 4}},
    )

    decision = StrategyDecision(
        signals=(signal,),
        candidates=(candidate,),
        rejections=(rejection,),
        checkpoint=checkpoint,
    )

    assert decision.signals == (signal,)
    assert decision.candidates == (candidate,)
    assert decision.rejections == (rejection,)
    assert decision.checkpoint == checkpoint


def test_data_requirement_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="warmup_buckets must be positive"):
        StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=0,
            required_fields=("close_price",),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )


def test_metadata_rejects_empty_name_and_version() -> None:
    with pytest.raises(ValueError, match="strategy name must not be empty"):
        StrategyMetadata(name="", version="v0")
    with pytest.raises(ValueError, match="strategy version must not be empty"):
        StrategyMetadata(name="compression_breakout", version="")


def _identity() -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="abc",
        run_mode=RunMode.REPLAY,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("states",),
    )
