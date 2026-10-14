"""Direct current-position head encoding."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


EXECUTION_HEAD_SCHEMA_VERSION = 3


def build_execution_head_payload(
    book: ExecutionBook,
    key: PositionKey,
    *,
    active_reservations: Callable[[PositionKey], tuple[PositionReservation, ...]],
) -> dict[str, object]:
    scope = book._stream_scopes.get(key.canonical_id)
    if scope is None:
        raise RuntimeError("durable execution head requires a stream scope")
    position_book = book._ensure_book(key)
    return {
        "schema_version": EXECUTION_HEAD_SCHEMA_VERSION,
        "position_key": encode_position_key(key),
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "position_state": position_book.encode_current_state(),
        "last_sequence": book._last_sequences.get(key.canonical_id),
        "active_reservation_ids": sorted(
            reservation.reservation_id for reservation in active_reservations(key)
        ),
        "external_recovery_ids": sorted(
            identity
            for identity, position in book._external_recovery_positions.items()
            if position == key
        ),
        "recovery_command_ids": sorted(
            identity
            for identity in book._recovery_required_commands
            if identity in book._outbox_by_command_id
            and book._outbox_by_command_id[identity].scope.to_position_key() == key
        ),
    }


def decode_execution_head_state(
    payload: object,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
) -> PositionBook:
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        EXECUTION_HEAD_SCHEMA_VERSION
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


def decode_current_facts(
    payload: object,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
):
    return decode_execution_head_state(payload, key=key, scope=scope).current_facts


def _sequence(state: dict[str, object], field: str) -> int:
    value = state.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"execution head {field} is invalid")
    return value
