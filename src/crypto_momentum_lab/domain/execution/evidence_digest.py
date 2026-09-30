"""Canonical execution evidence digests independent of recovery serialization."""

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.account.models import AccountFillEvent


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
    """Hash global trade identity independently of the transport stream epoch."""
    return digest_json_payload(asdict(fill))
