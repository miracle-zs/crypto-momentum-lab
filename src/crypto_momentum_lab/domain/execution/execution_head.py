"""Direct current-position head encoding."""

from __future__ import annotations

from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec

EXECUTION_HEAD_SCHEMA_VERSION = 4


def is_supported_execution_head_schema(value: object) -> bool:
    return type(value) is int and value == EXECUTION_HEAD_SCHEMA_VERSION


def decode_execution_head_state(
    payload: object,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
) -> PositionBook:
    if not isinstance(payload, dict) or not is_supported_execution_head_schema(
        payload.get("schema_version")
    ):
        raise ValueError("unsupported execution head schema")
    if payload.get("position_key") != encode_position_key(key):
        raise ValueError("execution head position identity mismatch")
    if payload.get("stream_scope") != {
        "stream_id": scope.stream_id,
        "stream_epoch": scope.stream_epoch,
    }:
        raise ValueError("execution head stream identity mismatch")
    state = payload.get("position_state")
    if not isinstance(state, dict) or set(state) != {
        "projection",
        "episode_sequence",
        "batch_sequence",
        "verification",
    }:
        raise ValueError("execution head position state is malformed")
    projection = PositionRecoveryCodec.decode_projection(state.get("projection"))
    verification = PositionRecoveryCodec.decode_facts(state.get("verification"))
    if projection.archived_episodes:
        raise ValueError("current execution head contains archived episodes")
    return PositionBook.from_current_state(
        position_key=key,
        stream_scope=scope,
        projection=projection,
        verification=verification,
        episode_sequence=_sequence(state, "episode_sequence"),
        batch_sequence=_sequence(state, "batch_sequence"),
    )


def _sequence(state: dict[str, object], field: str) -> int:
    value = state.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"execution head {field} is invalid")
    return value
