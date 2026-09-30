"""Canonical position snapshot encoding and anchor identity without recovery codec."""

import hashlib
import json
from collections.abc import Mapping

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot


def encode_position_snapshot(snapshot: AccountPositionSnapshot) -> dict[str, object]:
    if snapshot.observed_at.tzinfo is None or snapshot.observed_at.utcoffset() is None:
        raise ValueError("recovery datetimes must be timezone-aware")
    return {
        "environment": snapshot.environment,
        "account_label": snapshot.account_label,
        "symbol": snapshot.symbol,
        "position_side": snapshot.position_side,
        "position_amt": format(snapshot.position_amt, "f"),
        "entry_price": format(snapshot.entry_price, "f"),
        "mark_price": format(snapshot.mark_price, "f"),
        "unrealized_pnl": format(snapshot.unrealized_pnl, "f"),
        "notional": format(snapshot.notional, "f"),
        "leverage": snapshot.leverage,
        "margin_type": snapshot.margin_type,
        "observed_at": snapshot.observed_at.isoformat(),
        "raw_payload": snapshot.raw_payload,
    }


def snapshot_anchor_id(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"psnap_{hashlib.sha256(encoded).hexdigest()}"


def stable_snapshot_anchor_id(snapshot: AccountPositionSnapshot) -> str:
    return snapshot_anchor_id(encode_position_snapshot(snapshot))
