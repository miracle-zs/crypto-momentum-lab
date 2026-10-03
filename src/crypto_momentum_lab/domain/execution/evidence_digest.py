"""Canonical execution evidence digests independent of recovery serialization."""

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionView


def digest_json_payload(payload: object) -> str:
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


def trade_payload_digest(fill: AccountFillEvent) -> str:
    """Hash immutable business facts, independent of REST/WS payload formatting."""
    payload = asdict(fill)
    payload.pop("raw_payload")
    payload["position_side"] = fill.raw_position_side
    payload["identity_schema_version"] = 2
    for field in ("price", "quantity", "realized_pnl", "fee"):
        value = payload[field]
        rendered = format(value, "f")
        payload[field] = (
            "0"
            if value == 0
            else rendered.rstrip("0").rstrip(".")
            if "." in rendered
            else rendered
        )
    return digest_json_payload(payload)


def view_projection_digest(view: PositionView) -> str:
    payload = asdict(view)
    payload.pop("projection_version", None)
    payload.pop("pending_command_ids", None)
    payload.pop("active_entry_command_ids", None)
    return digest_json_payload(payload)
