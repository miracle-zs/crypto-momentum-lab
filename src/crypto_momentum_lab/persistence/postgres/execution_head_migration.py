"""One-time conversion from the original full execution head to current state."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    PositionKey,
)
from .execution_head_migration_codec import (
    PositionRecoveryCodec,
)
from .execution_head_migration_projection import (
    LegacyPositionProjection,
    encode_position_key,
    encode_projection,
)
from .execution_head_migration_snapshot import stable_snapshot_anchor_id

_EPISODE_SUFFIX = re.compile(r"_(\d+)$")
_BATCH_SUFFIX = re.compile(r"_b(\d+)$")
_EXECUTION_HEAD_SCHEMA_VERSION = 4
_ACCOUNT_FACTS_SCHEMA_VERSION = 4


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
    active_fills: tuple[AccountFillEvent, ...] = tuple(
        PositionRecoveryCodec.decode_fill(item)
        for item in _array(payload, "active_fills")
    )
    snapshots: tuple[AccountPositionSnapshot, ...] = tuple(
        PositionRecoveryCodec.decode_snapshot(item)
        for item in _array(payload, "active_snapshots")
    )
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...] = tuple(
        PositionRecoveryCodec.decode_boundary(item)
        for item in _array(payload, "active_exit_boundaries")
    )
    raw_coverage = payload.get("coverage")
    coverage = (
        PositionRecoveryCodec.decode_coverage(raw_coverage)
        if raw_coverage is not None
        else None
    )
    if coverage is not None and coverage.stream_scope != scope:
        raise ValueError("coverage scope does not match execution head")
    has_synthetic_fills = any(
        bool(fill.raw_payload.get("synthetic_from_order", False))
        for fill in active_fills
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
        snapshots=snapshots,
        exit_boundaries=exit_boundaries,
        coverage=coverage,
        has_synthetic_fills=has_synthetic_fills,
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
    projection: LegacyPositionProjection,
    snapshots: tuple[AccountPositionSnapshot, ...],
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...],
    coverage: FactCoverageInterval | None,
    has_synthetic_fills: bool,
    episode_sequence: int,
    batch_sequence: int,
) -> dict[str, object]:
    if (
        projection.position_key != position_key
        or projection.stream_scope != stream_scope
    ):
        raise ValueError("projection identity does not match execution head")
    if type(episode_sequence) is not int or episode_sequence < 0:
        raise ValueError("episode sequence must be a non-negative integer")
    if type(batch_sequence) is not int or batch_sequence < 0:
        raise ValueError("batch sequence must be a non-negative integer")

    active_episode = projection.active_episode
    if active_episode is not None:
        active_episode = replace(
            active_episode,
            batches=tuple(
                batch for batch in active_episode.batches if batch.quantity > 0
            ),
        )
    current_projection = replace(
        projection,
        active_episode=active_episode,
        archived_episodes=(),
    )
    current_verification = _current_verification(
        position_key=position_key,
        stream_scope=stream_scope,
        snapshots=snapshots,
        exit_boundaries=exit_boundaries,
        coverage=coverage,
        has_synthetic_fills=has_synthetic_fills,
    )
    return {
        "projection": encode_projection(current_projection, include_derived=False),
        "episode_sequence": episode_sequence,
        "batch_sequence": batch_sequence,
        "verification": current_verification,
    }


def _current_verification(
    *,
    position_key: PositionKey,
    stream_scope: AccountFactStreamScope,
    snapshots: tuple[AccountPositionSnapshot, ...],
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...],
    coverage: FactCoverageInterval | None,
    has_synthetic_fills: bool,
) -> dict[str, object]:
    latest_snapshot = max(
        snapshots,
        key=lambda item: item.observed_at,
        default=None,
    )
    source_anchor_snapshot = None
    provenance = coverage.load_provenance if coverage is not None else None
    if provenance is not None:
        source_anchor_snapshot = next(
            (
                snapshot
                for snapshot in snapshots
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

    deduplicated_boundaries = {item.order_id: item for item in exit_boundaries}
    verification: dict[str, object] = {
        "schema_version": _ACCOUNT_FACTS_SCHEMA_VERSION,
        "position_key": encode_position_key(position_key),
        "fills": [],
        "snapshots": [
            PositionRecoveryCodec.encode_snapshot(item) for item in snapshots
        ],
        "exit_boundaries": [
            PositionRecoveryCodec.encode_boundary(item)
            for item in deduplicated_boundaries.values()
        ],
        "coverage": (
            PositionRecoveryCodec.encode_coverage(coverage)
            if coverage is not None
            else None
        ),
        "has_synthetic_fills": has_synthetic_fills,
        "conflicting_fills": [],
        "has_late_events": False,
        "stream_scope": PositionRecoveryCodec.encode_scope(stream_scope),
        "recovery_checkpoint": None,
        "fact_conflicts": [],
        "integrity_issues": [],
        "late_fills": [],
        "fill_cursor_provenance": None,
        "prefix_facts_complete": False,
    }
    if provenance is None:
        verification["fill_load_provenance"] = None
    return verification


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
    _validate_current_verification(state.get("verification"), key=key, scope=scope)
    if (
        projection.position_key != key
        or projection.stream_scope != scope
        or projection.archived_episodes
    ):
        raise ValueError("converted execution head projection is invalid")
    _sequence(state, "episode_sequence")
    _sequence(state, "batch_sequence")


def _validate_current_verification(
    value: object,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
) -> None:
    if not isinstance(value, dict):
        raise ValueError("converted execution head verification is malformed")
    expected_keys = {
        "schema_version",
        "position_key",
        "fills",
        "snapshots",
        "exit_boundaries",
        "coverage",
        "has_synthetic_fills",
        "conflicting_fills",
        "has_late_events",
        "stream_scope",
        "recovery_checkpoint",
        "fact_conflicts",
        "integrity_issues",
        "late_fills",
        "fill_cursor_provenance",
        "prefix_facts_complete",
    }
    if set(value) not in (expected_keys, expected_keys | {"fill_load_provenance"}):
        raise ValueError("converted execution head verification fields are malformed")
    if (
        type(value.get("schema_version")) is not int
        or value["schema_version"] != _ACCOUNT_FACTS_SCHEMA_VERSION
        or value.get("position_key") != encode_position_key(key)
        or value.get("stream_scope") != PositionRecoveryCodec.encode_scope(scope)
    ):
        raise ValueError("converted execution head verification identity mismatch")
    empty_fields = (
        "fills",
        "conflicting_fills",
        "fact_conflicts",
        "integrity_issues",
        "late_fills",
    )
    if any(value.get(field) != [] for field in empty_fields):
        raise ValueError("converted execution head verification contains history")
    if (
        type(value.get("has_synthetic_fills")) is not bool
        or type(value.get("has_late_events")) is not bool
        or value.get("has_late_events")
        or value.get("recovery_checkpoint") is not None
        or value.get("fill_cursor_provenance") is not None
        or value.get("prefix_facts_complete") is not False
    ):
        raise ValueError("converted execution head verification state is invalid")
    if not isinstance(value.get("snapshots"), list) or not isinstance(
        value.get("exit_boundaries"), list
    ):
        raise ValueError("converted execution head verification facts are invalid")
    coverage_payload = value.get("coverage")
    coverage = (
        PositionRecoveryCodec.decode_coverage(coverage_payload)
        if coverage_payload is not None
        else None
    )
    if coverage is not None and coverage.stream_scope != scope:
        raise ValueError("converted execution head verification coverage is invalid")
    if "fill_load_provenance" not in value and (
        coverage is None or coverage.load_provenance is None
    ):
        raise ValueError("converted execution head verification provenance is missing")
    if value.get("fill_load_provenance") is not None:
        raise ValueError("converted execution head verification provenance is invalid")


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
