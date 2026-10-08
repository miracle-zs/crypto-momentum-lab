"""Hot-storage policy for durable decision evidence.

The live runtime evaluates a decision every market bucket.  A normal
``holding_position_no_exit`` result is useful operational telemetry, but it is
not an execution, a risk rejection, or an incident.  Keeping its complete
replay input in PostgreSQL made the observability database grow with every
bucket.  Until a cold evidence archive is configured, retain a tamper-evident
summary for that one high-volume outcome and keep complete evidence for every
other result.

Large full-evidence policy states use a lossless, verified storage codec. Equal
prior/next states share one encoded copy; adapters expand them before returning
domain traces. This does not turn no-candidate decisions into summary-only rows.

This module owns that policy so Postgres adapters do not each need to know
which decision outcomes are safe to compact.
"""

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

_NORMAL_HOLD_OUTCOME = "holding_position_no_exit"


def retains_complete_replay_evidence(
    *,
    intent_produced: bool,
    rejection_reason: str | None,
    trace_payload: dict[str, Any],
) -> bool:
    """Whether this decision must keep its complete replay evidence hot.

    The default is deliberately conservative.  Only the normal, no-effect
    hold outcome is compacted.  A produced intent, an exit command, an unknown
    outcome, and every risk/policy rejection retain their full evidence.
    """
    return (
        intent_produced
        or rejection_reason != _NORMAL_HOLD_OUTCOME
        or trace_payload.get("output_exit_command") is not None
    )


def compact_trace_for_hot_storage(
    *,
    intent_produced: bool,
    rejection_reason: str | None,
    trace_payload: dict[str, Any],
) -> dict[str, Any]:
    """Return the immutable payload appropriate for PostgreSQL hot storage.

    Summary rows intentionally cannot be replayed: they retain outcome,
    decision/frame digests, and a digest of the original payload for incident
    correlation.  Complete evidence remains mandatory for all other outcomes.
    """
    if retains_complete_replay_evidence(
        intent_produced=intent_produced,
        rejection_reason=rejection_reason,
        trace_payload=trace_payload,
    ):
        return _compress_policy_states(trace_payload)

    return {
        "evidence_level": "summary",
        "summary_schema_version": 1,
        "frame_digest": str(trace_payload.get("frame_digest", "")),
        "input_hash": str(trace_payload.get("input_hash", "")),
        "original_payload_sha256": _payload_digest(trace_payload),
        "outcome": _NORMAL_HOLD_OUTCOME,
    }


def _compress_policy_states(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep exact replay inputs while storing repeated states only once."""
    result = dict(payload)
    prior = payload.get("prior_policy_state")
    following = payload.get("next_policy_state")
    if not isinstance(prior, dict) or not isinstance(following, dict):
        return result
    states = {"prior": prior}
    if following != prior:
        states["next"] = following
    raw = json.dumps(
        states, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    # Small inputs already fit efficiently into PostgreSQL's normal storage.
    if len(raw) < 8192 or len(raw) >= 16 * 1024**2:
        return result
    compressed = base64.b64encode(zlib.compress(raw, level=6)).decode("ascii")
    if len(compressed) + 256 >= len(raw):
        return result
    result.pop("prior_policy_state")
    result.pop("next_policy_state")
    result["compressed_policy_states"] = {
        "codec": "zlib-base64-v1",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "data": compressed,
    }
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


def summary_market_refs(
    refs: tuple[MarketRevisionRef, ...],
) -> list[dict[str, str]]:
    """Encode revision identities needed to explain a summary decision."""
    return [
        {
            "scope": ref.scope,
            "symbol": ref.symbol,
            "interval": ref.interval,
            "bucket_start": ref.bucket_start.isoformat(),
            "bucket_end": ref.bucket_end.isoformat(),
            "revision_id": ref.revision_id,
            "content_hash": ref.content_hash,
            "published_at": ref.published_at.isoformat(),
            "source_epoch": ref.source_epoch,
            "visibility_mode": ref.visibility_mode.value,
            "observed_at": ref.observed_at.isoformat() if ref.observed_at else "",
        }
        for ref in refs
    ]


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
    "compact_trace_for_hot_storage",
    "expand_trace_payload",
    "load_summary_market_refs",
    "retains_complete_replay_evidence",
    "summary_market_refs",
]
