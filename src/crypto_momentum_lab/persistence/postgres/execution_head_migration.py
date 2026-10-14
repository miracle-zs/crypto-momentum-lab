"""One-time conversion from the original full execution head to current state."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from crypto_momentum_lab.domain.execution.execution_head import (
    EXECUTION_HEAD_SCHEMA_VERSION,
    decode_execution_head_state,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.projection_codec import (
    encode_position_key,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts

_EPISODE_SUFFIX = re.compile(r"_(\d+)$")
_BATCH_SUFFIX = re.compile(r"_b(\d+)$")


def convert_legacy_execution_head(payload: object) -> dict[str, object]:
    """Convert a schema-1 head to the single schema-4 current-state shape."""
    if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int:
        raise ValueError("legacy execution head is malformed")
    if payload["schema_version"] != 1:
        raise ValueError("expected a schema-1 execution head")

    key = PositionRecoveryCodec.decode_position_key(payload.get("position_key"))
    old_scope = payload.get("stream_scope")
    if not isinstance(old_scope, dict):
        raise ValueError("legacy execution head stream scope is malformed")
    stream_id = old_scope.get("stream_id")
    stream_epoch = old_scope.get("stream_epoch")
    if not isinstance(stream_id, str) or not isinstance(stream_epoch, str):
        raise ValueError("legacy execution head stream scope is incomplete")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id=stream_id, stream_epoch=stream_epoch
    )

    projection = PositionRecoveryCodec.decode_projection(payload.get("projection"))
    if projection.position_key != key or projection.stream_scope != scope:
        raise ValueError("legacy execution head projection identity mismatch")
    active_fills = tuple(
        PositionRecoveryCodec.decode_fill(item)
        for item in _array(payload, "active_fills")
    )
    snapshots = tuple(
        PositionRecoveryCodec.decode_snapshot(item)
        for item in _array(payload, "active_snapshots")
    )
    exit_boundaries = tuple(
        PositionRecoveryCodec.decode_boundary(item)
        for item in _array(payload, "active_exit_boundaries")
    )
    raw_coverage = payload.get("coverage")
    facts = AccountFacts(
        position_key=key,
        snapshots=snapshots,
        exit_boundaries=exit_boundaries,
        coverage=(
            PositionRecoveryCodec.decode_coverage(raw_coverage)
            if raw_coverage is not None
            else None
        ),
        has_synthetic_fills=any(
            bool(fill.raw_payload.get("synthetic_from_order", False))
            for fill in active_fills
        ),
        stream_scope=scope,
        prefix_facts_complete=False,
    )

    active_episode = projection.active_episode
    episodes = (
        (*projection.archived_episodes, active_episode)
        if active_episode is not None
        else projection.archived_episodes
    )
    episode_sequence = max(
        (_suffix(episode.episode_id, _EPISODE_SUFFIX) for episode in episodes),
        default=0,
    )
    batch_sequence = (
        max(
            (
                _suffix(batch.batch_id, _BATCH_SUFFIX)
                for batch in active_episode.batches
            ),
            default=0,
        )
        if active_episode is not None
        else 0
    )

    position_book = PositionBook.from_current_state(
        position_key=key,
        stream_scope=scope,
        projection=replace(projection, archived_episodes=()),
        verification=facts,
        episode_sequence=episode_sequence,
        batch_sequence=batch_sequence,
    )
    last_sequence = payload.get("last_sequence")
    if last_sequence is not None and (
        type(last_sequence) is not int or last_sequence < 0
    ):
        raise ValueError("legacy execution head last sequence is invalid")

    converted = {
        "schema_version": EXECUTION_HEAD_SCHEMA_VERSION,
        "position_key": encode_position_key(key),
        "stream_scope": {"stream_id": stream_id, "stream_epoch": stream_epoch},
        "position_state": position_book.encode_current_state(),
        "last_sequence": last_sequence,
        "active_reservation_ids": sorted(
            _string_array(payload, "active_reservation_ids")
        ),
        "external_recovery_ids": sorted(
            _string_array(payload, "external_recovery_ids")
        ),
        "recovery_command_ids": sorted(_string_array(payload, "recovery_command_ids")),
    }
    decode_execution_head_state(converted, key=key, scope=scope)
    return converted


def _array(payload: dict[str, Any], field: str) -> list[object]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise ValueError(f"legacy execution head {field} is malformed")
    return value


def _string_array(payload: dict[str, Any], field: str) -> list[str]:
    value = _array(payload, field)
    if any(not isinstance(item, str) for item in value):
        raise ValueError(f"legacy execution head {field} is malformed")
    return [item for item in value if isinstance(item, str)]


def _suffix(identity: str, pattern: re.Pattern[str]) -> int:
    match = pattern.search(identity)
    if match is None:
        raise ValueError(f"legacy execution head identity has no sequence: {identity}")
    return int(match.group(1))
