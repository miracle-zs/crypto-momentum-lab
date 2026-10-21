"""Decode and maintain evidence stored with historical decision traces."""

from __future__ import annotations

import base64
import hashlib
import json
import zlib
from datetime import datetime
from typing import Any

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import (
    MarketRevisionRef,
    MarketVisibilityMode,
)


def share_policy_states(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Replace large policy states with verified identities.

    Other decision evidence remains inline.
    """
    decoded = expand_trace_payload(payload)
    prior = decoded.get("prior_policy_state")
    following = decoded.get("next_policy_state")
    if not isinstance(prior, dict) or not isinstance(following, dict):
        return dict(payload), {}
    if len(json.dumps(prior)) + len(json.dumps(following)) < 8192:
        return dict(payload), {}
    prior_digest = _payload_digest(prior)
    next_digest = prior_digest if following == prior else _payload_digest(following)
    states = {prior_digest: prior, next_digest: following}
    decoded.pop("prior_policy_state")
    decoded.pop("next_policy_state")
    decoded["policy_state_refs"] = {
        "prior": prior_digest,
        "next": next_digest,
    }
    return decoded, states

def policy_state_reference_ids(payload: dict[str, Any]) -> set[str]:
    refs = payload.get("policy_state_refs")
    if refs is None:
        return set()
    if not isinstance(refs, dict) or set(refs) != {"prior", "next"}:
        raise UnreproducibleError("Invalid shared policy state references")
    if any(not isinstance(value, str) or len(value) != 64 for value in refs.values()):
        raise UnreproducibleError("Invalid shared policy state digest")
    return set(refs.values())

def resolve_policy_states(
    payload: dict[str, Any], states: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Resolve exact state evidence and reject missing, corrupt or ambiguous inputs."""
    result = expand_trace_payload(payload)
    ids = policy_state_reference_ids(result)
    if not ids:
        return result
    if "prior_policy_state" in result or "next_policy_state" in result:
        raise UnreproducibleError("Ambiguous shared policy evidence")
    for digest in ids:
        if digest not in states or _payload_digest(states[digest]) != digest:
            raise UnreproducibleError("Shared policy evidence is missing or corrupt")
    refs = result.pop("policy_state_refs")
    result["prior_policy_state"] = states[refs["prior"]]
    result["next_policy_state"] = states[refs["next"]]
    return result

def expand_trace_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Decode exact policy evidence; malformed or altered evidence fails closed."""
    result = dict(payload)
    packed = result.pop("compressed_policy_states", None)
    if packed is None:
        return result
    try:
        if not isinstance(packed, dict) or packed.get("codec") != "zlib-base64-v1":
            raise ValueError("unsupported policy state codec")
        if "prior_policy_state" in result or "next_policy_state" in result:
            raise ValueError("ambiguous policy evidence")
        decoder = zlib.decompressobj()
        raw = decoder.decompress(
            base64.b64decode(packed["data"], validate=True), 16 * 1024**2
        )
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError("incomplete or oversized policy evidence")
        if hashlib.sha256(raw).hexdigest() != packed["sha256"]:
            raise ValueError("policy evidence digest mismatch")
        states = json.loads(raw)
        if not isinstance(states, dict):
            raise ValueError("policy evidence is not an object")
        prior = states["prior"]
        following = states.get("next", prior)
        if not isinstance(prior, dict) or not isinstance(following, dict):
            raise ValueError("policy state is not an object")
        result["prior_policy_state"] = prior
        result["next_policy_state"] = following
    except (KeyError, TypeError, ValueError, zlib.error) as exc:
        raise UnreproducibleError(
            "Invalid compressed decision policy evidence"
        ) from exc
    return result

def load_summary_market_refs(
    payload: dict[str, Any], *, decision_id: str
) -> list[MarketRevisionRef]:
    """Decode the reference identities attached to a compacted trace."""
    raw_refs = payload.get("market_refs")
    if not isinstance(raw_refs, list) or not raw_refs:
        raise UnreproducibleError(
            f"Summary decision trace {decision_id} has no market references"
        )
    refs: list[MarketRevisionRef] = []
    try:
        for raw in raw_refs:
            if not isinstance(raw, dict):
                raise TypeError("market reference is not an object")
            observed_at = raw.get("observed_at")
            refs.append(
                MarketRevisionRef(
                    scope=str(raw["scope"]),
                    symbol=str(raw["symbol"]),
                    interval=str(raw["interval"]),
                    bucket_start=datetime.fromisoformat(str(raw["bucket_start"])),
                    bucket_end=datetime.fromisoformat(str(raw["bucket_end"])),
                    revision_id=str(raw["revision_id"]),
                    content_hash=str(raw["content_hash"]),
                    published_at=datetime.fromisoformat(str(raw["published_at"])),
                    source_epoch=str(raw["source_epoch"]),
                    visibility_mode=MarketVisibilityMode(str(raw["visibility_mode"])),
                    observed_at=(
                        datetime.fromisoformat(str(observed_at))
                        if observed_at
                        else None
                    ),
                )
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise UnreproducibleError(
            f"Summary decision trace {decision_id} has invalid market references"
        ) from exc
    return refs

def _payload_digest(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

__all__ = [
    "expand_trace_payload",
    "load_summary_market_refs",
    "share_policy_states",
]
