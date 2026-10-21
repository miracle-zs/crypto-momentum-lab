"""One-time conversion from the original full execution head to current state."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.projection_codec import (
    encode_position_key,
    encode_projection,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)

_EPISODE_SUFFIX = re.compile(r"_(\d+)$")
_BATCH_SUFFIX = re.compile(r"_b(\d+)$")
_EXECUTION_HEAD_SCHEMA_VERSION = 4


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

    position_state = _encode_current_position_state(
        position_key=key,
        stream_scope=scope,
        projection=projection,
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
        "schema_version": _EXECUTION_HEAD_SCHEMA_VERSION,
        "position_key": encode_position_key(key),
        "stream_scope": {"stream_id": stream_id, "stream_epoch": stream_epoch},
        "position_state": position_state,
        "last_sequence": last_sequence,
        "active_reservation_ids": sorted(
            _string_array(payload, "active_reservation_ids")
        ),
        "external_recovery_ids": sorted(
            _string_array(payload, "external_recovery_ids")
        ),
        "recovery_command_ids": sorted(_string_array(payload, "recovery_command_ids")),
    }
    _validate_current_execution_head(converted, key=key, scope=scope)
    return converted


def _encode_current_position_state(
    *,
    position_key: PositionKey,
    stream_scope: AccountFactStreamScope,
    projection: PositionLedgerProjection,
    verification: AccountFacts,
    episode_sequence: int,
    batch_sequence: int,
) -> dict[str, object]:
    if (
        projection.position_key != position_key
        or projection.stream_scope != stream_scope
    ):
        raise ValueError("projection identity does not match execution head")
    if (
        verification.position_key != position_key
        or verification.stream_scope != stream_scope
    ):
        raise ValueError("verification identity does not match execution head")
    if verification.fills or verification.late_fills:
        raise ValueError("current position state must not contain fill history")
    if type(episode_sequence) is not int or episode_sequence < 0:
        raise ValueError("episode sequence must be a non-negative integer")
    if type(batch_sequence) is not int or batch_sequence < 0:
        raise ValueError("batch sequence must be a non-negative integer")

    active_episode = projection.active_episode
    if active_episode is not None:
        active_episode = replace(
            active_episode,
            batches=tuple(batch for batch in active_episode.batches if batch.quantity > 0),
            reductions=(),
        )
    current_projection = replace(
        projection,
        active_episode=active_episode,
        archived_episodes=(),
        episode_sequence=episode_sequence,
        batch_sequence=batch_sequence,
    )
    current_verification = _current_verification(verification)
    return {
        "projection": encode_projection(current_projection, include_derived=False),
        "episode_sequence": episode_sequence,
        "batch_sequence": batch_sequence,
        "verification": PositionRecoveryCodec.encode_facts(current_verification),
    }


def _current_verification(facts: AccountFacts) -> AccountFacts:
    latest_snapshot = max(
        facts.snapshots,
        key=lambda item: item.observed_at,
        default=None,
    )
    source_anchor_snapshot = None
    provenance = facts.fill_load_provenance
    if provenance is not None:
        source_anchor_snapshot = next(
            (
                snapshot
                for snapshot in facts.snapshots
                if snapshot.observed_at == provenance.source_anchor_event_cut
                and stable_snapshot_anchor_id(snapshot) == provenance.source_anchor_id
            ),
            None,
        )
    if source_anchor_snapshot is None:
        snapshots = (latest_snapshot,) if latest_snapshot is not None else ()
    elif (
        latest_snapshot is None
        or latest_snapshot.observed_at == source_anchor_snapshot.observed_at
    ):
        snapshots = (source_anchor_snapshot,)
    else:
        snapshots = (source_anchor_snapshot, latest_snapshot)

    exit_boundaries = {item.order_id: item for item in facts.exit_boundaries}
    return AccountFacts(
        position_key=facts.position_key,
        snapshots=snapshots,
        exit_boundaries=tuple(exit_boundaries.values()),
        coverage=facts.coverage,
        has_synthetic_fills=facts.has_synthetic_fills,
        conflicting_fills=facts.conflicting_fills[-1:],
        has_late_events=facts.has_late_events,
        stream_scope=facts.stream_scope,
        fact_conflicts=facts.fact_conflicts[-1:],
        integrity_issues=facts.integrity_issues[-1:],
        fill_cursor_provenance=facts.fill_cursor_provenance,
        fill_load_provenance=provenance,
        prefix_facts_complete=False,
    )


def _validate_current_execution_head(
    payload: object,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
) -> None:
    if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int:
        raise ValueError("converted execution head is malformed")
    if payload["schema_version"] != _EXECUTION_HEAD_SCHEMA_VERSION:
        raise ValueError("converted execution head schema is unsupported")
    if payload.get("position_key") != encode_position_key(key):
        raise ValueError("converted execution head position identity mismatch")
    if payload.get("stream_scope") != {
        "stream_id": scope.stream_id,
        "stream_epoch": scope.stream_epoch,
    }:
        raise ValueError("converted execution head stream identity mismatch")
    state = payload.get("position_state")
    if not isinstance(state, dict) or set(state) != {
        "projection",
        "episode_sequence",
        "batch_sequence",
        "verification",
    }:
        raise ValueError("converted execution head position state is malformed")
    projection = PositionRecoveryCodec.decode_projection(state.get("projection"))
    verification = PositionRecoveryCodec.decode_facts(state.get("verification"))
    if (
        projection.position_key != key
        or projection.stream_scope != scope
        or projection.archived_episodes
    ):
        raise ValueError("converted execution head projection is invalid")
    if (
        verification.position_key != key
        or verification.stream_scope != scope
        or verification.fills
        or verification.late_fills
    ):
        raise ValueError("converted execution head verification is invalid")
    _sequence(state, "episode_sequence")
    _sequence(state, "batch_sequence")


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


def _sequence(state: dict[str, object], field: str) -> int:
    value = state.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"execution head {field} is invalid")
    return value
