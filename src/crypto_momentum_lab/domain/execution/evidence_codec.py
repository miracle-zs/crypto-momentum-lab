"""Canonical execution evidence and head digests shared by writers/recovery."""

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionView
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
)


def _digest_json_payload(payload: object) -> str:
    def encode(value: object) -> object:
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, StrEnum):
            return value.value
        raise TypeError(f"unsupported execution evidence value {type(value).__name__}")

    canonical = json.dumps(
        payload,
        default=encode,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _recovery_checkpoint_head_binding(
    checkpoint: PositionRecoveryCheckpoint | None,
) -> dict[str, object] | None:
    if checkpoint is None:
        return None
    parent_scope = getattr(checkpoint, "parent_stream_scope", None)
    return {
        "checkpoint_id": checkpoint.checkpoint_id,
        "stream_scope": PositionRecoveryCodec.encode_scope(checkpoint.stream_scope),
        "event_cut": checkpoint.event_cut.astimezone(UTC).isoformat(),
        "facts_hash": checkpoint.facts_hash,
        "projection_digest": checkpoint.projection_digest,
        "parent_stream_scope": (
            PositionRecoveryCodec.encode_scope(parent_scope)
            if parent_scope is not None
            else None
        ),
        "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
        "parent_facts_hash": checkpoint.parent_facts_hash,
        "parent_projection_digest": checkpoint.parent_projection_digest,
        "parent_event_cut": (
            checkpoint.parent_event_cut.astimezone(UTC).isoformat()
            if checkpoint.parent_event_cut is not None
            else None
        ),
        "suffix_facts_hash": checkpoint.suffix_facts_hash,
    }


def _view_projection_digest(view: PositionView) -> str:
    payload = asdict(view)
    payload.pop("projection_version", None)
    return _digest_json_payload(payload)


def _trade_payload_digest(fill: AccountFillEvent) -> str:
    """Hash global trade identity independently of the transport stream epoch."""
    return _digest_json_payload(asdict(fill))
