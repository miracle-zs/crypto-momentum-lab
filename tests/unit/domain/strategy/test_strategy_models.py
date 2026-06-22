from dataclasses import dataclass
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


def test_strategy_decision_rejects_unknown_candidate_signal_id() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    signal = _signal(identity=identity, detected_at=detected_at)
    candidate = _candidate(
        identity=identity,
        signal_id="sig_unknown",
        detected_at=detected_at,
    )

    with pytest.raises(
        ValueError,
        match="candidate signal_id must reference a decision signal",
    ):
        StrategyDecision(
            signals=(signal,),
            candidates=(candidate,),
            rejections=(),
            checkpoint=_checkpoint(detected_at),
        )


def test_strategy_decision_rejects_candidate_source_signal_mismatch() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    signal = _signal(identity=identity, detected_at=detected_at)
    candidate = _candidate(
        identity=identity,
        signal_id=signal.signal_id,
        detected_at=detected_at,
        symbol="ETHUSDT",
    )

    with pytest.raises(
        ValueError,
        match="candidate must match source signal identity",
    ):
        StrategyDecision(
            signals=(signal,),
            candidates=(candidate,),
            rejections=(),
            checkpoint=_checkpoint(detected_at),
        )


def test_order_intent_candidate_rejects_expires_at_not_after_created_at() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="expires_at must be after created_at"):
        OrderIntentCandidate(
            candidate_id="cand_1",
            signal_id="sig_1",
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
            expires_at=detected_at,
            created_at=detected_at,
            reason="compression_breakout",
            features={"range_high": "100"},
        )


def test_order_intent_candidate_rejects_non_positive_desired_notional() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="desired_notional must be positive"):
        OrderIntentCandidate(
            candidate_id="cand_1",
            signal_id="sig_1",
            run_id=identity.run_id,
            strategy_name=identity.strategy_name,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("0"),
            reduce_only=False,
            expires_at=detected_at + timedelta(seconds=30),
            created_at=detected_at,
            reason="compression_breakout",
            features={"range_high": "100"},
        )


def test_order_intent_candidate_rejects_reduce_only() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    with pytest.raises(
        ValueError,
        match="reduce_only candidates are out of scope for V0",
    ):
        OrderIntentCandidate(
            candidate_id="cand_1",
            signal_id="sig_1",
            run_id=identity.run_id,
            strategy_name=identity.strategy_name,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("100"),
            reduce_only=True,
            expires_at=detected_at + timedelta(seconds=30),
            created_at=detected_at,
            reason="compression_breakout",
            features={"range_high": "100"},
        )


def test_strategy_payload_fields_store_normalized_json_values() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    payload = _normalizable_payload(detected_at)
    expected_payload = _normalized_payload(detected_at)

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
        features=payload,
        reference_prices=payload,
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
        features=payload,
    )
    rejection = StrategyRejection(
        reason=RejectionReason.NO_SIGNAL,
        symbol="ETHUSDT",
        bucket_start=detected_at,
        details=payload,
    )
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": detected_at},
        warmup_buckets_by_symbol={"BTCUSDT": 4},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload=payload,
    )

    assert signal.features == expected_payload
    assert signal.reference_prices == expected_payload
    assert candidate.features == expected_payload
    assert rejection.details == expected_payload
    assert checkpoint.payload == expected_payload


def test_strategy_payload_fields_reject_unsupported_values() -> None:
    identity = _identity()
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    with pytest.raises(TypeError, match="features must be JSON-normalizable"):
        StrategySignal(
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
            features={"unsupported": object()},
            reference_prices={"breakout_price": "101"},
        )


def test_strategy_payload_fields_reject_non_string_mapping_keys() -> None:
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    with pytest.raises(TypeError, match="details must be JSON-normalizable"):
        StrategyRejection(
            reason=RejectionReason.NO_SIGNAL,
            symbol="ETHUSDT",
            bucket_start=detected_at,
            details={"nested": {1: "invalid"}},
        )


def test_non_finite_float_is_rejected_from_payload_and_config_hash() -> None:
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)

    for non_finite in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(TypeError, match="payload must be JSON-normalizable"):
            StrategyCheckpoint(
                last_processed_at_by_symbol={"BTCUSDT": detected_at},
                warmup_buckets_by_symbol={"BTCUSDT": 4},
                cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
                payload={"value": non_finite},
            )

        with pytest.raises(TypeError, match="JSON numbers must be finite"):
            deterministic_config_hash({"value": non_finite})


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


def _signal(
    *,
    identity: StrategyRunIdentity,
    detected_at: datetime,
) -> StrategySignal:
    return StrategySignal(
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


def _candidate(
    *,
    identity: StrategyRunIdentity,
    signal_id: str,
    detected_at: datetime,
    symbol: str = "BTCUSDT",
) -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id=signal_id,
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol=symbol,
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=detected_at + timedelta(seconds=30),
        created_at=detected_at,
        reason="compression_breakout",
        features={"range_high": "100"},
    )


def _checkpoint(detected_at: datetime) -> StrategyCheckpoint:
    return StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": detected_at},
        warmup_buckets_by_symbol={"BTCUSDT": 4},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"buffer_sizes": {"BTCUSDT": 4}},
    )


@dataclass(frozen=True)
class _PayloadRecord:
    amount: Decimal
    observed_at: datetime
    side: StrategySide


def _normalizable_payload(observed_at: datetime) -> dict[str, object]:
    return {
        "decimal": Decimal("1.25"),
        "datetime": observed_at,
        "enum": StrategySide.SHORT,
        "dataclass": _PayloadRecord(
            amount=Decimal("2.50"),
            observed_at=observed_at,
            side=StrategySide.LONG,
        ),
        "mapping": {"inner_decimal": Decimal("3.75")},
        "list": [Decimal("4.00"), observed_at, StrategySide.SHORT],
        "tuple": (Decimal("5.25"), observed_at, StrategySide.LONG),
    }


def _normalized_payload(observed_at: datetime) -> dict[str, object]:
    observed_at_value = observed_at.isoformat()
    return {
        "decimal": "1.25",
        "datetime": observed_at_value,
        "enum": "short",
        "dataclass": {
            "amount": "2.50",
            "observed_at": observed_at_value,
            "side": "long",
        },
        "mapping": {"inner_decimal": "3.75"},
        "list": ["4.00", observed_at_value, "short"],
        "tuple": ["5.25", observed_at_value, "long"],
    }
