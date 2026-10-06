"""Hot-storage policy for durable decision evidence.

The live runtime evaluates a decision every market bucket.  A normal
``holding_position_no_exit`` result is useful operational telemetry, but it is
not an execution, a risk rejection, or an incident.  Keeping its complete
replay input in PostgreSQL made the observability database grow with every
bucket.  Until a cold evidence archive is configured, retain a tamper-evident
summary for that one high-volume outcome and keep complete evidence for every
other result.

This module owns that policy so Postgres adapters do not each need to know
which decision outcomes are safe to compact.
"""

from __future__ import annotations

import hashlib
import json
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
        return dict(trace_payload)

    return {
        "evidence_level": "summary",
        "summary_schema_version": 1,
        "frame_digest": str(trace_payload.get("frame_digest", "")),
        "input_hash": str(trace_payload.get("input_hash", "")),
        "original_payload_sha256": _payload_digest(trace_payload),
        "outcome": _NORMAL_HOLD_OUTCOME,
    }


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
    "load_summary_market_refs",
    "retains_complete_replay_evidence",
    "summary_market_refs",
]
